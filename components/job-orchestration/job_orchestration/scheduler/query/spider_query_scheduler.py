"""
Spider-based query scheduler for CLP.

This module provides functions to build and dispatch search jobs using Spider
task graphs with channels instead of Celery with TCP reducer connections.

Key design:
- Uses Spider's `channel_task()` to create tasks bound to channels
- Uses TDL types (Int64, list[Int8]) for task inputs/outputs
- All tasks (search + reducer) are siblings in the task graph (no dependencies)
- Reducer starts immediately and polls channel for results
"""

from __future__ import annotations

import datetime
import json
from logging import getLogger
from typing import Any, Dict, List

from spider_py import Int64, Int8
from spider_py.client import Channel, channel_task, Driver, group

from job_orchestration.executor.query.spider_search_task import search_with_channel
from job_orchestration.executor.query.spider_reducer_task import reducer_task
from job_orchestration.scheduler.job_config import SearchJobConfig
from job_orchestration.utils.spider_utils import utf8_str_to_int8_list

logger = getLogger(__name__)


def build_search_task_graph(num_archives: int):
    """
    Build Spider task graph for search with aggregation (reducer).

    Creates a channel connecting all search tasks (producers) to a single
    reducer task (consumer). All tasks are siblings - no dependencies.

    :param num_archives: Number of archives to search (determines number of search tasks)
    :return: TaskGraph with channel bindings
    """
    # Create channel for search -> reducer communication
    channel = Channel[bytes]()

    # Create search task graphs (producers)
    search_task_graphs = [
        channel_task(search_with_channel, senders={"sender": channel})
        for _ in range(num_archives)
    ]

    # Create reducer task graph (consumer)
    reducer_graph = channel_task(reducer_task, receivers={"receiver": channel})

    # Combine all tasks (no dependencies - channel coordinates data flow)
    all_graphs = search_task_graphs + [reducer_graph]
    return group(all_graphs)


def prepare_job_inputs(
    job_id: str,
    archives: List[Dict[str, Any]],
    task_ids: List[int],
    search_config: SearchJobConfig,
    db_conn_params: Dict[str, Any],
    results_cache_uri: str,
) -> List[tuple]:
    """
    Prepare job inputs for all tasks.

    Returns a list of input tuples - one for each search task followed by
    the reducer task input.

    :param job_id: Job identifier
    :param archives: List of archives to search
    :param task_ids: List of task IDs
    :param search_config: Search job configuration
    :param db_conn_params: Database connection parameters
    :param results_cache_uri: MongoDB URI for results cache
    :return: List of input tuples for each task
    """
    inputs = []

    # Convert common data to TDL format
    job_id_int8 = utf8_str_to_int8_list(job_id)
    job_config_json_int8 = utf8_str_to_int8_list(json.dumps(search_config.model_dump()))
    db_conn_params_json_int8 = utf8_str_to_int8_list(json.dumps(db_conn_params))

    # Prepare search task inputs
    for archive, task_id in zip(archives, task_ids):
        archive_id_int8 = utf8_str_to_int8_list(archive["archive_id"])

        # Input tuple: (job_id, task_id, archive_id, job_config_json, db_conn_params_json)
        # Note: sender is injected by Spider at position after TaskContext
        inputs.append((
            job_id_int8,
            Int64(task_id),
            archive_id_int8,
            job_config_json_int8,
            db_conn_params_json_int8,
        ))

    # Prepare reducer task input
    aggregation_config = search_config.aggregation_config
    aggregation_config_dict = aggregation_config.model_dump() if aggregation_config else {}
    aggregation_config_json_int8 = utf8_str_to_int8_list(json.dumps(aggregation_config_dict))
    results_cache_uri_int8 = utf8_str_to_int8_list(results_cache_uri)

    # Input tuple: (job_id, aggregation_config_json, results_cache_uri)
    # Note: receiver is injected by Spider at position after TaskContext
    inputs.append((
        job_id_int8,
        aggregation_config_json_int8,
        results_cache_uri_int8,
    ))

    return inputs


class SpiderQueryJobHandle:
    """
    Handle for a Spider-based query job.

    Provides methods to check job status and get results.
    """

    def __init__(
        self,
        spider_job,
        job_id: str,
        num_search_tasks: int,
        start_time: datetime.datetime,
    ):
        self._spider_job = spider_job
        self._job_id = job_id
        self._num_search_tasks = num_search_tasks
        self._start_time = start_time

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def spider_job(self):
        return self._spider_job

    @property
    def start_time(self) -> datetime.datetime:
        return self._start_time

    def is_complete(self) -> bool:
        """Check if the Spider job is complete."""
        from spider_py.core import JobStatus

        status = self._spider_job.get_status()
        return status in (JobStatus.Succeeded, JobStatus.Failed, JobStatus.Cancelled)

    def get_search_task_results(self) -> List[Dict[str, Any]]:
        """Get results from search tasks."""
        from job_orchestration.utils.spider_utils import int8_list_to_utf8_str

        outputs = self._spider_job.get_outputs()
        results = []
        for i in range(self._num_search_tasks):
            if i < len(outputs) and outputs[i] is not None:
                result_json = int8_list_to_utf8_str(outputs[i])
                results.append(json.loads(result_json))
        return results

    def get_reducer_result(self) -> Dict[str, Any]:
        """Get result from reducer task."""
        from job_orchestration.utils.spider_utils import int8_list_to_utf8_str

        outputs = self._spider_job.get_outputs()
        if len(outputs) > self._num_search_tasks:
            result_json = int8_list_to_utf8_str(outputs[self._num_search_tasks])
            return json.loads(result_json)
        return None


def dispatch_search_job(
    driver: Driver,
    job_id: str,
    archives: List[Dict[str, Any]],
    task_ids: List[int],
    search_config: SearchJobConfig,
    db_conn_params: Dict[str, Any],
    results_cache_uri: str,
) -> SpiderQueryJobHandle:
    """
    Dispatch a search job using Spider.

    :param driver: Spider Driver instance
    :param job_id: Job identifier
    :param archives: List of archives to search
    :param task_ids: List of task IDs
    :param search_config: Search job configuration
    :param db_conn_params: Database connection parameters
    :param results_cache_uri: MongoDB URI for results cache
    :return: SpiderQueryJobHandle for monitoring the job
    """
    start_time = datetime.datetime.now()

    # Build task graph
    task_graph = build_search_task_graph(num_archives=len(archives))

    # Prepare inputs for all tasks
    job_inputs = prepare_job_inputs(
        job_id=job_id,
        archives=archives,
        task_ids=task_ids,
        search_config=search_config,
        db_conn_params=db_conn_params,
        results_cache_uri=results_cache_uri,
    )

    logger.info(
        f"Submitting Spider job {job_id} with {len(archives)} search tasks and reducer"
    )

    # Submit job to Spider
    jobs = driver.submit_jobs([task_graph], [tuple(job_inputs)])

    if not jobs or len(jobs) == 0:
        raise RuntimeError(f"Failed to submit Spider job {job_id}")

    return SpiderQueryJobHandle(
        spider_job=jobs[0],
        job_id=job_id,
        num_search_tasks=len(archives),
        start_time=start_time,
    )


def create_spider_driver(storage_url: str) -> Driver:
    """Create a Spider Driver instance."""
    return Driver(storage_url)


def cancel_spider_job(job_handle: SpiderQueryJobHandle) -> bool:
    """Cancel a Spider job."""
    try:
        job_handle.spider_job.cancel()
        logger.info(f"Cancelled Spider job {job_handle.job_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to cancel Spider job {job_handle.job_id}: {e}")
        return False
