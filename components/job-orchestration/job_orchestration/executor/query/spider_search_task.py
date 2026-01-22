"""
Spider-based search task that sends results via channels.

This module provides the search task function for use with Spider task graphs.
Results are streamed to a channel for consumption by a reducer task.
"""

import datetime
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from clp_py_utils.clp_config import Database, StorageEngine
from clp_py_utils.clp_logging import get_logger, set_logging_level
from clp_py_utils.sql_adapter import SQL_Adapter
from spider_py import Int64, Int8, TaskContext
from spider_py.client import Sender

from job_orchestration.executor.query.fs_search_task import (
    _make_core_clp_command_and_env_vars,
    _make_core_clp_s_command_and_env_vars,
)
from job_orchestration.executor.query.utils import (
    get_task_log_file_path,
    update_query_task_metadata,
)
from job_orchestration.executor.utils import load_worker_config
from job_orchestration.scheduler.constants import QueryTaskStatus
from job_orchestration.scheduler.job_config import SearchJobConfig
from job_orchestration.scheduler.scheduler_data import QueryTaskResult
from job_orchestration.utils.spider_utils import int8_list_to_utf8_str, utf8_str_to_int8_list

logger = get_logger("spider_search")


def _make_command_for_channel_output(
    clp_home: Path,
    worker_config,
    archive_id: str,
    search_config: SearchJobConfig,
) -> Optional[List[str]]:
    """
    Build search command for channel output mode.

    Instead of sending to reducer via TCP or results-cache, outputs to stdout
    for streaming through the channel.
    """
    storage_engine = worker_config.package.storage_engine

    if StorageEngine.CLP == storage_engine:
        command, _ = _make_core_clp_command_and_env_vars(
            clp_home, worker_config, archive_id, search_config
        )
    elif StorageEngine.CLP_S == storage_engine:
        command, _ = _make_core_clp_s_command_and_env_vars(
            clp_home, worker_config, archive_id, search_config
        )
    else:
        logger.error(f"Unsupported storage engine {storage_engine}")
        return None

    if command is None:
        return None

    # Add query parameters
    command.append(search_config.query_string)
    if search_config.begin_timestamp is not None:
        command.append("--tge")
        command.append(str(search_config.begin_timestamp))
    if search_config.end_timestamp is not None:
        command.append("--tle")
        command.append(str(search_config.end_timestamp))
    if search_config.ignore_case:
        command.append("--ignore-case")

    # Add aggregation flags if present
    if search_config.aggregation_config is not None:
        aggregation_config = search_config.aggregation_config
        if aggregation_config.do_count_aggregation is not None:
            command.append("--count")
        if aggregation_config.count_by_time_bucket_size is not None:
            command.append("--count-by-time")
            command.append(str(aggregation_config.count_by_time_bucket_size))

    # Use stdout output mode for channel streaming
    command.append("stdout")

    return command


def search_with_channel(
    _: TaskContext,
    sender: Sender[bytes],
    job_id: list[Int8],
    task_id: Int64,
    archive_id: list[Int8],
    job_config_json: list[Int8],
    clp_metadata_db_conn_params_json: list[Int8],
) -> list[Int8]:
    """
    Spider search task that sends results via channel.

    This task searches an archive and streams results to a channel
    for consumption by a reducer task.

    :param _: Spider task context (unused)
    :param sender: Channel sender for streaming results to reducer
    :param job_id: Job identifier as UTF-8 encoded Int8 list
    :param task_id: Task identifier
    :param archive_id: Archive to search as UTF-8 encoded Int8 list
    :param job_config_json: Search job config as JSON string (Int8 list)
    :param clp_metadata_db_conn_params_json: DB connection params as JSON string (Int8 list)
    :return: QueryTaskResult as JSON string (Int8 list)
    """
    task_name = "search_with_channel"

    # Decode inputs
    job_id_str = int8_list_to_utf8_str(job_id)
    task_id_int = int(task_id)
    archive_id_str = int8_list_to_utf8_str(archive_id)
    job_config_dict = json.loads(int8_list_to_utf8_str(job_config_json))
    db_conn_params = json.loads(int8_list_to_utf8_str(clp_metadata_db_conn_params_json))

    # Setup logging
    clp_logs_dir = Path(os.getenv("CLP_LOGS_DIR"))
    clp_logging_level = os.getenv("CLP_LOGGING_LEVEL")
    set_logging_level(logger, clp_logging_level)

    logger.info(f"Started {task_name} task {task_id_int} for job {job_id_str}")

    start_time = datetime.datetime.now()
    sql_adapter = SQL_Adapter(Database.model_validate(db_conn_params))

    # Load worker configuration
    clp_config_path = Path(os.getenv("CLP_CONFIG_PATH"))
    worker_config = load_worker_config(clp_config_path, logger)
    if worker_config is None:
        return _make_failure_result(sql_adapter, task_id_int, start_time)

    # Build search command for channel output
    clp_home = Path(os.getenv("CLP_HOME"))
    search_config = SearchJobConfig.model_validate(job_config_dict)

    task_command = _make_command_for_channel_output(
        clp_home=clp_home,
        worker_config=worker_config,
        archive_id=archive_id_str,
        search_config=search_config,
    )
    if not task_command:
        logger.error(f"Error creating {task_name} command")
        return _make_failure_result(sql_adapter, task_id_int, start_time)

    # Run search and stream results to channel
    result = _run_search_with_channel(
        sql_adapter=sql_adapter,
        sender=sender,
        task_command=task_command,
        job_id=job_id_str,
        task_id=task_id_int,
        archive_id=archive_id_str,
        start_time=start_time,
    )

    return utf8_str_to_int8_list(json.dumps(result.model_dump()))


def _run_search_with_channel(
    sql_adapter: SQL_Adapter,
    sender: Sender[bytes],
    task_command: List[str],
    job_id: str,
    task_id: int,
    archive_id: str,
    start_time: datetime.datetime,
) -> QueryTaskResult:
    """Run search subprocess and stream results to channel."""
    task_name = "search_with_channel"
    clp_logs_dir = Path(os.getenv("CLP_LOGS_DIR"))
    log_path = get_task_log_file_path(clp_logs_dir, job_id, task_id)
    log_file = open(log_path, "w")

    task_status = QueryTaskStatus.RUNNING
    update_query_task_metadata(
        sql_adapter, task_id, dict(status=task_status, start_time=start_time)
    )

    logger.info(f'Running: {" ".join(task_command)}')

    task_proc = subprocess.Popen(
        task_command,
        preexec_fn=os.setpgrp,
        close_fds=True,
        stdout=subprocess.PIPE,
        stderr=log_file,
    )

    def sigterm_handler(_signo, _stack_frame):
        logger.debug("Entered sigterm handler")
        if task_proc.poll() is None:
            logger.debug(f"Trying to kill {task_name} process")
            os.killpg(os.getpgid(task_proc.pid), signal.SIGTERM)
            os.waitpid(task_proc.pid, 0)
            logger.info(f"Cancelling {task_name} task.")
        sys.exit(_signo + 128)

    signal.signal(signal.SIGTERM, sigterm_handler)

    # Stream stdout to channel
    logger.info(f"Streaming {task_name} results to channel")
    batch_buffer: List[bytes] = []
    batch_size = 100  # Send in batches for efficiency

    for line in task_proc.stdout:
        batch_buffer.append(line)
        if len(batch_buffer) >= batch_size:
            # Send batch to channel
            batch_data = _pack_result_batch(task_id, archive_id, batch_buffer)
            sender.send(batch_data)
            batch_buffer = []

    # Send any remaining results
    if batch_buffer:
        batch_data = _pack_result_batch(task_id, archive_id, batch_buffer)
        sender.send(batch_data)

    # Wait for process to complete
    task_proc.wait()
    return_code = task_proc.returncode

    if 0 != return_code:
        task_status = QueryTaskStatus.FAILED
        logger.error(
            f"{task_name} task {task_id} failed for job {job_id} - return_code={return_code}"
        )
    else:
        task_status = QueryTaskStatus.SUCCEEDED
        logger.info(f"{task_name} task {task_id} completed for job {job_id}")

    log_file.close()
    duration = (datetime.datetime.now() - start_time).total_seconds()

    update_query_task_metadata(
        sql_adapter,
        task_id,
        dict(status=task_status, start_time=start_time, duration=duration),
    )

    result = QueryTaskResult(
        status=task_status,
        task_id=task_id,
        duration=duration,
    )

    if task_status == QueryTaskStatus.FAILED:
        result.error_log_path = str(log_path)

    return result


def _pack_result_batch(task_id: int, archive_id: str, lines: List[bytes]) -> bytes:
    """
    Pack a batch of result lines into a single bytes message.

    Format: header line + result lines.
    """
    header = f"TASK:{task_id}:ARCHIVE:{archive_id}\n".encode()
    return header + b"".join(lines)


def _make_failure_result(
    sql_adapter: SQL_Adapter,
    task_id: int,
    start_time: datetime.datetime,
) -> list[Int8]:
    """Create a failure result and update the database."""
    task_status = QueryTaskStatus.FAILED
    update_query_task_metadata(
        sql_adapter,
        task_id,
        dict(status=task_status, duration=0, start_time=start_time),
    )

    result = QueryTaskResult(
        task_id=task_id,
        status=task_status,
        duration=0,
    )

    return utf8_str_to_int8_list(json.dumps(result.model_dump()))
