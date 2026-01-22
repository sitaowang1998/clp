"""
Spider-based reducer task that receives results via channels.

This module provides the reducer task function for use with Spider task graphs.
The reducer receives search results through a Spider channel and aggregates
them before writing the final results to MongoDB.
"""

import datetime
import json
from collections import defaultdict
from typing import Dict, List, Optional

from clp_py_utils.clp_logging import get_logger
from pymongo import MongoClient
from spider_py import Int8, Int64, TaskContext
from spider_py.client import Receiver

from job_orchestration.utils.spider_utils import int8_list_to_utf8_str, utf8_str_to_int8_list

logger = get_logger("spider_reducer")


class Aggregator:
    """Base class for result aggregation."""

    def process(self, batch_data: bytes) -> None:
        """Process a batch of results from a search task."""
        raise NotImplementedError

    def finalize(self) -> Dict:
        """Return the final aggregated result."""
        raise NotImplementedError


class CountAggregator(Aggregator):
    """Aggregator for simple count aggregation."""

    def __init__(self):
        self.total_count = 0

    def process(self, batch_data: bytes) -> None:
        """Count the number of result lines in the batch."""
        lines = batch_data.split(b"\n")
        # First line is header "TASK:{task_id}:ARCHIVE:{archive_id}"
        # Rest are result lines (last may be empty)
        result_lines = [line for line in lines[1:] if line]
        self.total_count += len(result_lines)

    def finalize(self) -> Dict:
        """Return the total count."""
        return {"count": self.total_count}


class CountByTimeAggregator(Aggregator):
    """Aggregator for count-by-time bucket aggregation."""

    def __init__(self, bucket_size_ms: int):
        self.bucket_size_ms = bucket_size_ms
        self.time_buckets: Dict[int, int] = defaultdict(int)
        self.total_count = 0

    def process(self, batch_data: bytes) -> None:
        """Count results by time bucket."""
        lines = batch_data.split(b"\n")
        result_lines = [line for line in lines[1:] if line]

        for line in result_lines:
            self.total_count += 1
            timestamp_ms = self._extract_timestamp(line)
            if timestamp_ms is not None:
                bucket = (timestamp_ms // self.bucket_size_ms) * self.bucket_size_ms
                self.time_buckets[bucket] += 1
            else:
                self.time_buckets[0] += 1

    def _extract_timestamp(self, line: bytes) -> Optional[int]:
        """Extract timestamp from a result line."""
        # TODO: Implement proper timestamp extraction based on CLP output format
        return None

    def finalize(self) -> Dict:
        """Return the time bucket counts."""
        return {
            "count": self.total_count,
            "count_by_time": [
                {"timestamp": ts, "count": count}
                for ts, count in sorted(self.time_buckets.items())
            ],
        }


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
    start_time = datetime.datetime.now()

    # Decode inputs
    job_id_str = int8_list_to_utf8_str(job_id)
    config_dict = json.loads(int8_list_to_utf8_str(aggregation_config_json))
    results_uri = int8_list_to_utf8_str(results_cache_uri)

    logger.info(f"Started {task_name} task for job {job_id_str}")

    try:
        # Create aggregator based on configuration
        count_by_time_bucket_size = config_dict.get("count_by_time_bucket_size")
        if count_by_time_bucket_size:
            aggregator = CountByTimeAggregator(count_by_time_bucket_size)
        else:
            aggregator = CountAggregator()

        # Process results from channel
        batch_count = 0
        while True:
            # Receive from channel with timeout (60 seconds)
            item, drained = receiver.recv(timeout_ms=60000)

            if item is not None:
                aggregator.process(item)
                batch_count += 1

                if batch_count % 100 == 0:
                    logger.debug(f"Reducer processed {batch_count} batches for job {job_id_str}")

            if drained:
                logger.info(
                    f"Reducer received all results for job {job_id_str} ({batch_count} batches)"
                )
                break

            if item is None and not drained:
                logger.debug(f"Reducer timeout waiting for results for job {job_id_str}")
                continue

        # Finalize aggregation
        aggregated_result = aggregator.finalize()
        total_count = aggregated_result.get("count", 0)

        # Write aggregated results to MongoDB
        _write_results_to_cache(results_uri, job_id_str, aggregated_result)

        duration = (datetime.datetime.now() - start_time).total_seconds()
        logger.info(f"Reducer completed for job {job_id_str} in {duration:.2f}s")

        result = {
            "status": "success",
            "duration": duration,
            "total_count": total_count,
        }

    except Exception as e:
        duration = (datetime.datetime.now() - start_time).total_seconds()
        logger.error(f"Reducer failed for job {job_id_str}: {e}")

        result = {
            "status": "failed",
            "duration": duration,
            "error_message": str(e),
        }

    return utf8_str_to_int8_list(json.dumps(result))


def _write_results_to_cache(
    results_uri: str,
    job_id: str,
    aggregated_result: Dict,
) -> None:
    """Write aggregated results to MongoDB results cache."""
    client = MongoClient(results_uri)
    try:
        db = client.get_default_database()
        if db is None:
            db = client["clp_results"]

        collection = db[job_id]

        doc = {
            "type": "aggregation_result",
            "job_id": job_id,
            "result": aggregated_result,
            "timestamp": datetime.datetime.utcnow(),
        }
        collection.insert_one(doc)
        logger.info(f"Wrote aggregation result to MongoDB for job {job_id}")

    finally:
        client.close()
