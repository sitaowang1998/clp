"""
Spider-based reducer task that receives results via channels.

This module provides the reducer task function for use with Spider task graphs.
The reducer receives search results through a Spider channel and aggregates
them before writing the final results to MongoDB.
"""

import datetime
import json
from collections import defaultdict
from typing import Any

import msgpack
from clp_py_utils.clp_logging import get_logger
from pymongo import MongoClient
from spider_py import Int8, TaskContext
from spider_py.client import Receiver

from job_orchestration.utils.spider_utils import int8_list_to_utf8_str, utf8_str_to_int8_list

logger = get_logger("spider_reducer")


class Aggregator:
    """Base class for reducer-compatible aggregation."""

    def process_record_group(self, record_group: dict[str, Any]) -> None:
        """Process a record group (deserialized from msgpack)."""
        raise NotImplementedError

    def finalize_documents(self) -> list[dict[str, Any]]:
        """Return MongoDB documents matching the reducer output schema."""
        raise NotImplementedError

    def total_count(self) -> int:
        """Return total count across all groups."""
        raise NotImplementedError


class CountAggregator(Aggregator):
    """Aggregates counts per group_tags."""

    def __init__(self) -> None:
        """Initialize the group counter."""
        self._group_counts: dict[tuple[str, ...], int] = defaultdict(int)

    def process_record_group(self, record_group: dict[str, Any]) -> None:
        """Accumulate counts from a record group."""
        tags = tuple(record_group.get("group_tags", []))
        records = record_group.get("records", [])
        for record in records:
            if "count" in record:
                self._group_counts[tags] += int(record["count"])

    def finalize_documents(self) -> list[dict[str, Any]]:
        """Return documents formatted for MongoDB insert."""
        return [
            {"group_tags": list(tags), "records": [{"count": count}]}
            for tags, count in self._group_counts.items()
        ]

    def total_count(self) -> int:
        """Return the total count across all groups."""
        return sum(self._group_counts.values())


class CountByTimeAggregator(Aggregator):
    """Aggregates counts per timestamp bucket."""

    def __init__(self) -> None:
        """Initialize the bucket counter."""
        self._bucket_counts: dict[int, int] = defaultdict(int)

    def process_record_group(self, record_group: dict[str, Any]) -> None:
        """Accumulate counts from a record group."""
        tags = record_group.get("group_tags", [])
        if not tags:
            return
        try:
            timestamp = int(tags[0])
        except (TypeError, ValueError):
            return
        records = record_group.get("records", [])
        for record in records:
            if "count" in record:
                self._bucket_counts[timestamp] += int(record["count"])

    def finalize_documents(self) -> list[dict[str, Any]]:
        """Return documents formatted for MongoDB insert."""
        return [
            {"timestamp": ts, "count": count} for ts, count in sorted(self._bucket_counts.items())
        ]

    def total_count(self) -> int:
        """Return the total count across all buckets."""
        return sum(self._bucket_counts.values())


def reducer_task(
    _: TaskContext,
    receiver: Receiver[bytes],
    job_id: list[Int8],
    aggregation_config_json: list[Int8],
    results_cache_uri: list[Int8],
) -> list[Int8]:
    """
    Spider reducer task that aggregates results from channel.

    This task receives search results from multiple search tasks through
    a channel and aggregates them. The final aggregated result is written
    to MongoDB.

    :param _: Spider task context (unused)
    :param receiver: Channel receiver for getting results from search tasks
    :param job_id: Job identifier as UTF-8 encoded Int8 list
    :param aggregation_config_json: Aggregation config as JSON string (Int8 list)
    :param results_cache_uri: MongoDB URI as UTF-8 encoded Int8 list
    :return: Reducer result as JSON string (Int8 list)
    """
    task_name = "reducer"
    start_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)

    # Decode inputs
    job_id_str = int8_list_to_utf8_str(job_id)
    config_dict = json.loads(int8_list_to_utf8_str(aggregation_config_json))
    results_uri = int8_list_to_utf8_str(results_cache_uri)

    logger.info(
        "Started %s task for job %s at %s",
        task_name,
        job_id_str,
        start_time.isoformat(),
    )

    try:
        # Create aggregator based on configuration
        count_by_time_bucket_size = config_dict.get("count_by_time_bucket_size")
        if count_by_time_bucket_size:
            aggregator: Aggregator = CountByTimeAggregator()
        else:
            aggregator = CountAggregator()

        # Process results from channel
        batch_count = 0
        while True:
            # Receive from channel with timeout (60 seconds)
            item, drained = receiver.recv(timeout_ms=60000)

            if item is not None:
                try:
                    record_group = msgpack.unpackb(item, raw=False)
                except Exception:
                    logger.exception("Failed to decode record group")
                    continue
                aggregator.process_record_group(record_group)
                batch_count += 1

                if batch_count % 100 == 0:
                    logger.debug("Reducer processed %d batches for job %s", batch_count, job_id_str)

            if drained:
                logger.info(
                    "Reducer received all results for job %s (%d batches)",
                    job_id_str,
                    batch_count,
                )
                break

            if item is None and not drained:
                logger.debug("Reducer timeout waiting for results for job %s", job_id_str)
                continue

        # Finalize aggregation and write results to MongoDB
        documents = aggregator.finalize_documents()
        total_count = aggregator.total_count()
        _write_results_to_cache(
            results_uri,
            job_id_str,
            documents,
            is_timeline=bool(count_by_time_bucket_size),
        )

        end_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
        duration = (end_time - start_time).total_seconds()
        logger.info(
            "Reducer completed for job %s at %s in %.2fs",
            job_id_str,
            end_time.isoformat(),
            duration,
        )

        result = {
            "status": "success",
            "duration": duration,
            "total_count": total_count,
        }

    except Exception as exc:
        end_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
        duration = (end_time - start_time).total_seconds()
        logger.exception(
            "Reducer failed for job %s at %s",
            job_id_str,
            end_time.isoformat(),
        )

        result = {
            "status": "failed",
            "duration": duration,
            "error_message": str(exc),
        }

    logger.info("Reducer result for job %s: %s", job_id_str, result)
    return utf8_str_to_int8_list(json.dumps(result))


def _write_results_to_cache(
    results_uri: str,
    job_id: str,
    documents: list[dict[str, Any]],
    is_timeline: bool,
) -> None:
    """Write reducer-compatible aggregation results to MongoDB."""
    client = MongoClient(results_uri)
    try:
        db = client.get_default_database()
        if db is None:
            db = client["clp_results"]

        collection = db[job_id]
        if not documents:
            return

        if is_timeline:
            for doc in documents:
                collection.replace_one({"timestamp": doc["timestamp"]}, doc, upsert=True)
        else:
            collection.insert_many(documents)
        logger.info("Wrote aggregation results to MongoDB for job %s", job_id)

    finally:
        client.close()
