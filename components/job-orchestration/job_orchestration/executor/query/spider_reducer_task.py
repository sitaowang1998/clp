"""
Spider-based reducer task that receives results via channels.

This module provides the reducer task function for use with Spider task graphs.
The reducer receives search results through a Spider channel and aggregates
them before writing the final results to MongoDB.
"""

import datetime
import json
import logging
import os
import time
from collections import defaultdict
from typing import Any

import msgpack
from clp_py_utils.clp_logging import get_logger, get_logging_formatter, set_logging_level
from pymongo import MongoClient
from spider_py import Int8, TaskContext
from spider_py.client import Receiver
from spider_py.storage import StorageError

from job_orchestration.utils.spider_utils import int8_list_to_utf8_str, utf8_str_to_int8_list

logger = get_logger("spider_reducer")


def _ensure_task_log_handler() -> None:
    log_path = os.getenv("CLP_WORKER_LOG_PATH")
    if not log_path:
        return
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler) and handler.baseFilename == log_path:
            return
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(get_logging_formatter())
    logger.addHandler(file_handler)


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
    ctx: TaskContext,
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

    :param ctx: Spider task context containing Spider's internal task UUID
    :param receiver: Channel receiver for getting results from search tasks
    :param job_id: Job identifier as UTF-8 encoded Int8 list
    :param aggregation_config_json: Aggregation config as JSON string (Int8 list)
    :param results_cache_uri: MongoDB URI as UTF-8 encoded Int8 list
    :return: Reducer result as JSON string (Int8 list)
    """
    task_name = "reducer"
    func_entry_ms = int(time.monotonic() * 1000)
    start_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)

    # Decode inputs
    job_id_str = int8_list_to_utf8_str(job_id)
    config_dict = json.loads(int8_list_to_utf8_str(aggregation_config_json))
    results_uri = int8_list_to_utf8_str(results_cache_uri)
    spider_task_uuid = str(ctx.task_id)
    decode_end_ms = int(time.monotonic() * 1000)

    # Setup logging
    set_logging_level(logger, os.getenv("CLP_LOGGING_LEVEL"))
    _ensure_task_log_handler()

    logger.info(
        "Started %s task for job %s at %s",
        task_name,
        job_id_str,
        start_time.isoformat(),
    )
    logger.info(
        "[TIMING] spider_task_id=%s func_entry=%d decode_end=%d decode_duration_ms=%d",
        spider_task_uuid,
        func_entry_ms,
        decode_end_ms,
        decode_end_ms - func_entry_ms,
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
        channel_recv_start_ms = int(time.monotonic() * 1000)
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

        channel_recv_end_ms = int(time.monotonic() * 1000)
        logger.info(
            "[TIMING] spider_task_id=%s channel_recv_start=%d channel_recv_end=%d "
            "channel_recv_duration_ms=%d batch_count=%d",
            spider_task_uuid,
            channel_recv_start_ms,
            channel_recv_end_ms,
            channel_recv_end_ms - channel_recv_start_ms,
            batch_count,
        )

        # Finalize aggregation
        finalize_start_ms = int(time.monotonic() * 1000)
        documents = aggregator.finalize_documents()
        total_count = aggregator.total_count()
        finalize_end_ms = int(time.monotonic() * 1000)
        logger.info(
            "[TIMING] spider_task_id=%s finalize_start=%d finalize_end=%d "
            "finalize_duration_ms=%d total_count=%d num_documents=%d",
            spider_task_uuid,
            finalize_start_ms,
            finalize_end_ms,
            finalize_end_ms - finalize_start_ms,
            total_count,
            len(documents),
        )

        # Write results to MongoDB
        mongo_write_start_ms = int(time.monotonic() * 1000)
        _write_results_to_cache(
            results_uri,
            job_id_str,
            documents,
            is_timeline=bool(count_by_time_bucket_size),
        )
        mongo_write_end_ms = int(time.monotonic() * 1000)
        logger.info(
            "[TIMING] spider_task_id=%s mongo_write_start=%d mongo_write_end=%d "
            "mongo_write_duration_ms=%d",
            spider_task_uuid,
            mongo_write_start_ms,
            mongo_write_end_ms,
            mongo_write_end_ms - mongo_write_start_ms,
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

    except StorageError:
        logger.exception("Reducer storage error for job %s", job_id_str)
        raise  # Let Spider mark task as 'fail'
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

    func_exit_ms = int(time.monotonic() * 1000)
    logger.info(
        "[TIMING] spider_task_id=%s func_entry=%d func_exit=%d total_func_duration_ms=%d",
        spider_task_uuid,
        func_entry_ms,
        func_exit_ms,
        func_exit_ms - func_entry_ms,
    )
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
