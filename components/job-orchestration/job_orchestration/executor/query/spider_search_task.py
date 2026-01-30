"""
Spider-based search task that sends results via channels.

This module provides the search task function for use with Spider task graphs.
Results are streamed to a channel for consumption by a reducer task.
"""

import datetime
import logging
import json
import os
import signal
import socket
import struct
import subprocess
import sys
from pathlib import Path

import msgpack
from botocore.exceptions import BotoCoreError, ClientError
from clp_py_utils.clp_config import Database, StorageEngine, StorageType, WorkerConfig
from clp_py_utils.clp_logging import get_logger, get_logging_formatter, set_logging_level
from clp_py_utils.s3_utils import s3_put
from clp_py_utils.sql_adapter import SqlAdapter
from spider_py import Int8, Int64, TaskContext
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
_MAX_RECORD_GROUP_BYTES = 16 * 1024 * 1024


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


def _make_command_and_env_vars(  # noqa: C901, PLR0912, PLR0913
    clp_home: Path,
    worker_config: WorkerConfig,
    archive_id: str,
    search_config: SearchJobConfig,
    results_cache_uri: str,
    results_collection: str,
) -> tuple[list[str] | None, dict[str, str] | None, str | None]:
    """
    Build search command and env vars, selecting the output mode.

    When aggregation is enabled, outputs to stdout for channel streaming.
    Otherwise, matches Celery search output modes (network, file, results-cache).
    """
    storage_engine = worker_config.package.storage_engine

    if StorageEngine.CLP == storage_engine:
        command, _ = _make_core_clp_command_and_env_vars(
            clp_home, worker_config, archive_id, search_config
        )
        env_vars = None
    elif StorageEngine.CLP_S == storage_engine:
        command, env_vars = _make_core_clp_s_command_and_env_vars(
            clp_home, worker_config, archive_id, search_config
        )
    else:
        logger.error("Unsupported storage engine %s", storage_engine)
        return None, None, None

    if command is None:
        return None, None, None

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

    if search_config.aggregation_config is not None:
        aggregation_config = search_config.aggregation_config
        if (
            aggregation_config.reducer_host is None
            or aggregation_config.reducer_port is None
            or aggregation_config.job_id is None
        ):
            logger.error("Reducer output requested but reducer host/port/job_id not set.")
            return None, None, None
        # fmt: off
        command.extend((
            "reducer",
            "--host", aggregation_config.reducer_host,
            "--port", str(aggregation_config.reducer_port),
            "--job-id", str(aggregation_config.job_id),
        ))
        # fmt: on
        output_mode = "reducer"
    elif search_config.network_address is not None:
        # fmt: off
        command.extend((
            "network",
            "--host", search_config.network_address[0],
            "--port", str(search_config.network_address[1]),
        ))
        # fmt: on
        output_mode = "network"
    elif search_config.write_to_file:
        output_directory = worker_config.stream_output.get_directory() / results_collection
        output_directory.mkdir(exist_ok=True)
        output_path = output_directory / archive_id
        # fmt: off
        command.extend((
            "file",
            "--path", str(output_path),
        ))
        # fmt: on
        output_mode = "file"
    else:
        # fmt: off
        command.extend((
            "results-cache",
            "--uri", results_cache_uri,
            "--collection", results_collection,
            "--max-num-results", str(search_config.max_num_results),
        ))
        # fmt: on
        output_mode = "results-cache"

    return command, env_vars, output_mode


def search_with_channel(  # noqa: PLR0913, PLR0915
    ctx: TaskContext,
    sender: Sender[bytes],
    job_id: list[Int8],
    task_id: Int64,
    archive_id: list[Int8],
    job_config_json: list[Int8],
    clp_metadata_db_conn_params_json: list[Int8],
    results_cache_uri: list[Int8],
) -> list[Int8]:
    """
    Spider search task that sends results via channel.

    This task searches an archive and streams results to a channel
    for consumption by a reducer task.

    :param ctx: Spider task context containing Spider's internal task UUID
    :param sender: Channel sender for streaming results to reducer
    :param job_id: Job identifier as UTF-8 encoded Int8 list
    :param task_id: Task identifier
    :param archive_id: Archive to search as UTF-8 encoded Int8 list
    :param job_config_json: Search job config as JSON string (Int8 list)
    :param clp_metadata_db_conn_params_json: DB connection params as JSON string (Int8 list)
    :param results_cache_uri: Results cache URI as UTF-8 encoded Int8 list
    :return: QueryTaskResult as JSON string (Int8 list)
    """
    task_name = "search_with_channel"

    # Decode inputs
    job_id_str = int8_list_to_utf8_str(job_id)
    task_id_int = int(task_id)
    archive_id_str = int8_list_to_utf8_str(archive_id)
    job_config_dict = json.loads(int8_list_to_utf8_str(job_config_json))
    db_conn_params = json.loads(int8_list_to_utf8_str(clp_metadata_db_conn_params_json))
    results_cache_uri_str = int8_list_to_utf8_str(results_cache_uri)
    spider_task_uuid = str(ctx.task_id)

    # Setup logging
    clp_logging_level = os.getenv("CLP_LOGGING_LEVEL")
    set_logging_level(logger, clp_logging_level)
    _ensure_task_log_handler()

    start_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
    logger.info(
        "[TASK_ID_MAP] spider_task_id=%s search_task_id=%d job_id=%s",
        spider_task_uuid,
        task_id_int,
        job_id_str,
    )
    logger.info(
        "Started %s task %d for job %s at %s",
        task_name,
        task_id_int,
        job_id_str,
        start_time.isoformat(),
    )
    sql_adapter = SqlAdapter(Database.model_validate(db_conn_params))

    # Load worker configuration
    clp_config_path = Path(os.getenv("CLP_CONFIG_PATH"))
    worker_config = load_worker_config(clp_config_path, logger)
    if worker_config is None:
        return _make_failure_result(sql_adapter, task_id_int, start_time)

    search_config = SearchJobConfig.model_validate(job_config_dict)

    reducer_socket: socket.socket | None = None
    job_id_int: int | None = None
    if search_config.aggregation_config is not None:
        try:
            job_id_int = int(job_id_str)
        except ValueError:
            logger.exception("Invalid job ID for reducer output: %s", job_id_str)
            return _make_failure_result(sql_adapter, task_id_int, start_time)

        reducer_socket, reducer_host, reducer_port = _start_reducer_proxy()
        logger.info(
            "%s task %d reducer proxy listening on %s:%d",
            task_name,
            task_id_int,
            reducer_host,
            reducer_port,
        )
        search_config.aggregation_config.reducer_host = reducer_host
        search_config.aggregation_config.reducer_port = reducer_port
        search_config.aggregation_config.job_id = job_id_int

    # Build search command
    clp_home = Path(os.getenv("CLP_HOME"))

    task_command, core_clp_env_vars, output_mode = _make_command_and_env_vars(
        clp_home=clp_home,
        worker_config=worker_config,
        archive_id=archive_id_str,
        search_config=search_config,
        results_cache_uri=results_cache_uri_str,
        results_collection=job_id_str,
    )
    if not task_command or output_mode is None:
        logger.error("Error creating %s command", task_name)
        if reducer_socket is not None:
            reducer_socket.close()
        return _make_failure_result(sql_adapter, task_id_int, start_time)

    # Run search and stream results to channel
    result = _run_search_with_channel(
        sql_adapter=sql_adapter,
        sender=sender,
        task_command=task_command,
        env_vars=core_clp_env_vars,
        output_mode=output_mode,
        reducer_socket=reducer_socket,
        reducer_job_id=job_id_int,
        job_id=job_id_str,
        task_id=task_id_int,
        archive_id=archive_id_str,
        start_time=start_time,
    )

    storage_config = worker_config.stream_output.storage
    if (
        StorageType.S3 == storage_config.type
        and search_config.write_to_file
        and QueryTaskStatus.SUCCEEDED == result.status
    ):
        s3_config = storage_config.s3_config
        dest_path = f"{job_id_str}/{archive_id_str}"
        src_file = Path(worker_config.stream_output.get_directory()) / job_id_str / archive_id_str

        logger.info("Uploading query results %s to S3...", dest_path)
        try:
            s3_put(s3_config, src_file, dest_path)
            logger.info("Finished uploading query results %s to S3.", dest_path)
        except (BotoCoreError, ClientError, ValueError):
            logger.exception("Failed to upload query results %s to S3.", dest_path)
            result.status = QueryTaskStatus.FAILED
            result.error_log_path = str(os.getenv("CLP_WORKER_LOG_PATH"))

        src_file.unlink()

    end_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
    logger.info(
        "Finished %s task %d for job %s at %s status=%s duration=%.2fs",
        task_name,
        task_id_int,
        job_id_str,
        end_time.isoformat(),
        result.status,
        result.duration,
    )
    return utf8_str_to_int8_list(json.dumps(result.model_dump()))


def _run_search_with_channel(  # noqa: C901, PLR0913, PLR0915
    sql_adapter: SqlAdapter,
    sender: Sender[bytes],
    task_command: list[str],
    env_vars: dict[str, str] | None,
    output_mode: str,
    reducer_socket: socket.socket | None,
    reducer_job_id: int | None,
    job_id: str,
    task_id: int,
    archive_id: str,
    start_time: datetime.datetime,
) -> QueryTaskResult:
    """Run search subprocess and stream results to channel."""
    task_name = "search_with_channel"
    clp_logs_dir = Path(os.getenv("CLP_LOGS_DIR"))
    log_path = get_task_log_file_path(clp_logs_dir, job_id, task_id)
    with log_path.open("w") as log_file:
        task_status = QueryTaskStatus.RUNNING
        update_query_task_metadata(
            sql_adapter, task_id, {"status": task_status, "start_time": start_time}
        )

        logger.info("Running: %s", " ".join(task_command))

        stdout_target = subprocess.PIPE if output_mode == "stdout" else log_file
        task_proc = subprocess.Popen(
            task_command,
            preexec_fn=os.setpgrp,  # noqa: PLW1509
            close_fds=True,
            stdout=stdout_target,
            stderr=log_file,
            env=env_vars,
        )

        def sigterm_handler(_signo: int, _stack_frame: object) -> None:
            logger.debug("Entered sigterm handler")
            if task_proc.poll() is None:
                logger.debug("Trying to kill %s process", task_name)
                os.killpg(os.getpgid(task_proc.pid), signal.SIGTERM)
                os.waitpid(task_proc.pid, 0)
                logger.info("Cancelling %s task.", task_name)
            sys.exit(_signo + 128)

        signal.signal(signal.SIGTERM, sigterm_handler)

        if output_mode == "stdout":
            # Stream stdout to channel.
            logger.info("Streaming %s results to channel", task_name)
            batch_buffer: list[bytes] = []
            batch_size = 100  # Send in batches for efficiency.

            if task_proc.stdout is not None:
                for line in task_proc.stdout:
                    batch_buffer.append(line)
                    if len(batch_buffer) >= batch_size:
                        # Send batch to channel.
                        batch_data = _pack_result_batch(task_id, archive_id, batch_buffer)
                        sender.send(batch_data)
                        batch_buffer = []

            # Send any remaining results.
            if batch_buffer:
                batch_data = _pack_result_batch(task_id, archive_id, batch_buffer)
                sender.send(batch_data)

            task_proc.wait()
        elif output_mode == "reducer":
            if reducer_socket is None:
                logger.error("Reducer proxy socket was not initialized.")
            else:
                record_groups_sent = _stream_reducer_results(
                    reducer_socket=reducer_socket,
                    sender=sender,
                    task_proc=task_proc,
                    expected_job_id=reducer_job_id,
                )
                if record_groups_sent == 0:
                    # Ensure channel closes even when search yields no record groups.
                    empty_group = msgpack.packb(
                        {"group_tags": [], "records": []},
                        use_bin_type=True,
                    )
                    sender.send(empty_group)
            task_proc.wait()
        else:
            # No channel streaming for non-stdout outputs.
            logger.info("Waiting for %s to finish", task_name)
            task_proc.wait()
        return_code = task_proc.returncode

        if 0 != return_code:
            task_status = QueryTaskStatus.FAILED
            logger.error(
                "%s task %d failed for job %s - return_code=%d",
                task_name,
                task_id,
                job_id,
                return_code,
            )
        else:
            task_status = QueryTaskStatus.SUCCEEDED
            logger.info("%s task %d completed for job %s", task_name, task_id, job_id)

        end_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
        duration = (end_time - start_time).total_seconds()

        update_query_task_metadata(
            sql_adapter,
            task_id,
            {"status": task_status, "start_time": start_time, "duration": duration},
        )

        result = QueryTaskResult(
            status=task_status,
            task_id=task_id,
            duration=duration,
        )

        if task_status == QueryTaskStatus.FAILED:
            result.error_log_path = str(log_path)

        return result


def _pack_result_batch(task_id: int, archive_id: str, lines: list[bytes]) -> bytes:
    """
    Pack a batch of result lines into a single bytes message.

    Format: header line + result lines.
    """
    header = f"TASK:{task_id}:ARCHIVE:{archive_id}\n".encode()
    return header + b"".join(lines)


def _start_reducer_proxy() -> tuple[socket.socket, str, int]:
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("127.0.0.1", 0))
    server_socket.listen(1)
    host, port = server_socket.getsockname()
    return server_socket, host, port


def _recv_exact(
    conn: socket.socket,
    num_bytes: int,
    task_proc: subprocess.Popen | None = None,
) -> bytes | None:
    buf = b""
    while len(buf) < num_bytes:
        try:
            chunk = conn.recv(num_bytes - len(buf))
        except TimeoutError:
            if task_proc is not None and task_proc.poll() is not None:
                return None
            continue
        if not chunk:
            return None
        buf += chunk
    return buf


def _stream_reducer_results(  # noqa: C901
    reducer_socket: socket.socket,
    sender: Sender[bytes],
    task_proc: subprocess.Popen,
    expected_job_id: int | None,
) -> int:
    reducer_socket.settimeout(0.5)
    conn = None
    record_groups_sent = 0
    try:
        while conn is None:
            try:
                conn, _ = reducer_socket.accept()
            except TimeoutError:  # noqa: PERF203
                if task_proc.poll() is not None:
                    logger.exception("Search process exited before reducer proxy connected.")
                    return

        conn.settimeout(0.5)
        job_id_bytes = _recv_exact(conn, 8, task_proc)
        if job_id_bytes is None:
            logger.error("Failed to read reducer handshake.")
            return
        reducer_job_id = struct.unpack("<q", job_id_bytes)[0]
        if expected_job_id is not None and reducer_job_id != expected_job_id:
            logger.warning(
                "Reducer handshake job_id mismatch: expected %s, got %s",
                expected_job_id,
                reducer_job_id,
            )
        conn.sendall(b"y")

        while True:
            size_bytes = _recv_exact(conn, 8, task_proc)
            if size_bytes is None:
                break
            record_size = struct.unpack("<Q", size_bytes)[0]
            if record_size > _MAX_RECORD_GROUP_BYTES:
                logger.error("Record group too large: %d bytes", record_size)
                break
            payload = _recv_exact(conn, record_size, task_proc)
            if payload is None:
                break
            sender.send(payload)
            record_groups_sent += 1
    finally:
        if conn is not None:
            conn.close()
        reducer_socket.close()
    return record_groups_sent


def _make_failure_result(
    sql_adapter: SqlAdapter,
    task_id: int,
    start_time: datetime.datetime,
) -> list[Int8]:
    """Create a failure result and update the database."""
    task_status = QueryTaskStatus.FAILED
    update_query_task_metadata(
        sql_adapter,
        task_id,
        {"status": task_status, "duration": 0, "start_time": start_time},
    )

    result = QueryTaskResult(
        task_id=task_id,
        status=task_status,
        duration=0,
    )

    return utf8_str_to_int8_list(json.dumps(result.model_dump()))


def search_without_channel(  # noqa: PLR0913
    ctx: TaskContext,
    job_id: list[Int8],
    task_id: Int64,
    archive_id: list[Int8],
    job_config_json: list[Int8],
    clp_metadata_db_conn_params_json: list[Int8],
    results_cache_uri: list[Int8],
) -> list[Int8]:
    """
    Spider search task WITHOUT channel (for non-aggregation search).

    This task searches an archive and writes results directly via clp binary
    (results-cache, network, or file output modes).

    :param ctx: Spider task context containing Spider's internal task UUID
    :param job_id: Job identifier as UTF-8 encoded Int8 list
    :param task_id: Task identifier
    :param archive_id: Archive to search as UTF-8 encoded Int8 list
    :param job_config_json: Search job config as JSON string (Int8 list)
    :param clp_metadata_db_conn_params_json: DB connection params as JSON string (Int8 list)
    :param results_cache_uri: Results cache URI as UTF-8 encoded Int8 list
    :return: QueryTaskResult as JSON string (Int8 list)
    """
    task_name = "search_without_channel"

    # Decode inputs
    job_id_str = int8_list_to_utf8_str(job_id)
    task_id_int = int(task_id)
    archive_id_str = int8_list_to_utf8_str(archive_id)
    job_config_dict = json.loads(int8_list_to_utf8_str(job_config_json))
    db_conn_params = json.loads(int8_list_to_utf8_str(clp_metadata_db_conn_params_json))
    results_cache_uri_str = int8_list_to_utf8_str(results_cache_uri)
    spider_task_uuid = str(ctx.task_id)

    # Setup logging
    clp_logging_level = os.getenv("CLP_LOGGING_LEVEL")
    set_logging_level(logger, clp_logging_level)
    _ensure_task_log_handler()

    start_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
    logger.info(
        "[TASK_ID_MAP] spider_task_id=%s search_task_id=%d job_id=%s",
        spider_task_uuid,
        task_id_int,
        job_id_str,
    )
    logger.info(
        "Started %s task %d for job %s at %s",
        task_name,
        task_id_int,
        job_id_str,
        start_time.isoformat(),
    )
    sql_adapter = SqlAdapter(Database.model_validate(db_conn_params))

    # Load worker configuration
    clp_config_path = Path(os.getenv("CLP_CONFIG_PATH"))
    worker_config = load_worker_config(clp_config_path, logger)
    if worker_config is None:
        return _make_failure_result(sql_adapter, task_id_int, start_time)

    search_config = SearchJobConfig.model_validate(job_config_dict)

    # Build search command
    clp_home = Path(os.getenv("CLP_HOME"))

    task_command, core_clp_env_vars, output_mode = _make_command_and_env_vars(
        clp_home=clp_home,
        worker_config=worker_config,
        archive_id=archive_id_str,
        search_config=search_config,
        results_cache_uri=results_cache_uri_str,
        results_collection=job_id_str,
    )
    if not task_command or output_mode is None:
        logger.error("Error creating %s command", task_name)
        return _make_failure_result(sql_adapter, task_id_int, start_time)

    # Run search (no channel streaming)
    result = _run_search_without_channel(
        sql_adapter=sql_adapter,
        task_command=task_command,
        env_vars=core_clp_env_vars,
        job_id=job_id_str,
        task_id=task_id_int,
        start_time=start_time,
    )

    storage_config = worker_config.stream_output.storage
    if (
        StorageType.S3 == storage_config.type
        and search_config.write_to_file
        and QueryTaskStatus.SUCCEEDED == result.status
    ):
        s3_config = storage_config.s3_config
        dest_path = f"{job_id_str}/{archive_id_str}"
        src_file = Path(worker_config.stream_output.get_directory()) / job_id_str / archive_id_str

        logger.info("Uploading query results %s to S3...", dest_path)
        try:
            s3_put(s3_config, src_file, dest_path)
            logger.info("Finished uploading query results %s to S3.", dest_path)
        except (BotoCoreError, ClientError, ValueError):
            logger.exception("Failed to upload query results %s to S3.", dest_path)
            result.status = QueryTaskStatus.FAILED
            result.error_log_path = str(os.getenv("CLP_WORKER_LOG_PATH"))

        src_file.unlink()

    end_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
    logger.info(
        "Finished %s task %d for job %s at %s status=%s duration=%.2fs",
        task_name,
        task_id_int,
        job_id_str,
        end_time.isoformat(),
        result.status,
        result.duration,
    )
    return utf8_str_to_int8_list(json.dumps(result.model_dump()))


def _run_search_without_channel(  # noqa: PLR0913
    sql_adapter: SqlAdapter,
    task_command: list[str],
    env_vars: dict[str, str] | None,
    job_id: str,
    task_id: int,
    start_time: datetime.datetime,
) -> QueryTaskResult:
    """Run search subprocess without channel streaming."""
    task_name = "search_without_channel"
    clp_logs_dir = Path(os.getenv("CLP_LOGS_DIR"))
    log_path = get_task_log_file_path(clp_logs_dir, job_id, task_id)
    with log_path.open("w") as log_file:
        task_status = QueryTaskStatus.RUNNING
        update_query_task_metadata(
            sql_adapter, task_id, {"status": task_status, "start_time": start_time}
        )

        logger.info("Running: %s", " ".join(task_command))

        task_proc = subprocess.Popen(
            task_command,
            preexec_fn=os.setpgrp,  # noqa: PLW1509
            close_fds=True,
            stdout=log_file,
            stderr=log_file,
            env=env_vars,
        )

        def sigterm_handler(_signo: int, _stack_frame: object) -> None:
            logger.debug("Entered sigterm handler")
            if task_proc.poll() is None:
                logger.debug("Trying to kill %s process", task_name)
                os.killpg(os.getpgid(task_proc.pid), signal.SIGTERM)
                os.waitpid(task_proc.pid, 0)
                logger.info("Cancelling %s task.", task_name)
            sys.exit(_signo + 128)

        signal.signal(signal.SIGTERM, sigterm_handler)

        logger.info("Waiting for %s to finish", task_name)
        task_proc.wait()
        return_code = task_proc.returncode

        if 0 != return_code:
            task_status = QueryTaskStatus.FAILED
            logger.error(
                "%s task %d failed for job %s - return_code=%d",
                task_name,
                task_id,
                job_id,
                return_code,
            )
        else:
            task_status = QueryTaskStatus.SUCCEEDED
            logger.info("%s task %d completed for job %s", task_name, task_id, job_id)

        end_time = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
        duration = (end_time - start_time).total_seconds()

        update_query_task_metadata(
            sql_adapter,
            task_id,
            {"status": task_status, "start_time": start_time, "duration": duration},
        )

        result = QueryTaskResult(
            status=task_status,
            task_id=task_id,
            duration=duration,
        )

        if task_status == QueryTaskStatus.FAILED:
            result.error_log_path = str(log_path)

        return result
