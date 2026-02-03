#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#   "matplotlib",
# ]
# ///

"""
Spider Search Task Timeline - Projected C++ Executor Performance v2

Visualizes the projected timeline if using a C++ task executor instead of Python,
based on microbenchmark speedup factors from Spider performance testing.

This version supports the comprehensive timeline format from spider_timeline_v4.py,
including:
- Worker scheduler request phases (connect, send, receive)
- Post-receive phases (deserialize, instance_conn, instance_create)
- Storage connection phases (pre-execution, handle_result)
- C++ worker phases (fetch, spawn, input_send, execution, submit)
- Python task executor phases (decode, config_load, cmd_build, search)

Default speedup factors (from Spider microbenchmark):
- Startup: 61.6x speedup (spawn_end -> py_func_entry)
- Shutdown: 30.0x speedup (py_func_exit -> execution_end)
"""

import argparse
import copy
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches


@dataclass
class TaskTiming:
    """Detailed timing data for a single search task."""

    task_id: str

    # Worker scheduler fetch phase (before request)
    sched_fetch_start: Optional[datetime] = None
    sched_fetch_end: Optional[datetime] = None

    # Worker scheduler request phases (client-side)
    worker_request_start: Optional[datetime] = None
    worker_connect_end: Optional[datetime] = None
    worker_send_end: Optional[datetime] = None
    worker_receive_end: Optional[datetime] = None

    # Post-receive phases (between receive_end and fetch_start)
    deserialize_end: Optional[datetime] = None
    instance_conn_end: Optional[datetime] = None
    instance_create_end: Optional[datetime] = None

    # Storage connection phases
    storage_connect_pre_start: Optional[datetime] = None
    storage_connect_pre_end: Optional[datetime] = None
    storage_connect_result_start: Optional[datetime] = None
    storage_connect_result_end: Optional[datetime] = None

    # C++ worker phases
    fetch_start: Optional[datetime] = None
    fetch_end: Optional[datetime] = None
    spawn_start: Optional[datetime] = None
    spawn_end: Optional[datetime] = None
    input_send_start: Optional[datetime] = None
    input_send_end: Optional[datetime] = None
    execution_start: Optional[datetime] = None
    execution_end: Optional[datetime] = None
    submit_start: Optional[datetime] = None
    submit_end: Optional[datetime] = None

    # Python task executor phases
    py_func_entry: Optional[datetime] = None
    py_decode_end: Optional[datetime] = None
    py_config_load_start: Optional[datetime] = None
    py_config_load_end: Optional[datetime] = None
    py_cmd_build_start: Optional[datetime] = None
    py_cmd_build_end: Optional[datetime] = None
    py_search_start: Optional[datetime] = None
    py_search_end: Optional[datetime] = None
    py_func_exit: Optional[datetime] = None


@dataclass
class WorkerTimeline:
    """Timeline data for a single worker."""

    worker_id: str  # Short UUID (first 8 chars)
    tasks: list[TaskTiming] = field(default_factory=list)


@dataclass
class JobInfo:
    """Job timing information from query scheduler log."""

    job_id: str
    submit_time: Optional[datetime] = None
    submit_end_time: Optional[datetime] = None
    complete_time: Optional[datetime] = None


# Log timestamp patterns
LOG_TIMESTAMP_PATTERN = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]")
PY_LOG_TIMESTAMP_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")

# C++ Worker TIMING patterns
CPP_TIMING_PATTERNS = {
    "fetch_input": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"fetch_input_start=(\d+)\s+fetch_input_end=(\d+)"
    ),
    "spawn": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"spawn_start=(\d+)\s+spawn_end=(\d+)"
    ),
    "input_send": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"input_send_start=(\d+)\s+input_send_end=(\d+)"
    ),
    "execution": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"execution_start=(\d+)\s+execution_end=(\d+)"
    ),
    "submit_output": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"submit_output_start=(\d+)\s+submit_output_end=(\d+)"
    ),
}

# Worker scheduler request timing pattern (client-side)
# New format with post-receive timing (deserialize, instance_conn, instance_create)
WORKER_SCHED_REQUEST_PATTERN_V3 = re.compile(
    r"\[TIMING\]\s+worker_id=(\S+)\s+task_id=(\S+)\s+get_next_task\s+"
    r"sched_fetch_start=(\d+)\s+sched_fetch_end=(\d+)\s+"
    r"request_start=(\d+)\s+connect_end=(\d+)\s+send_end=(\d+)\s+receive_end=(\d+)\s+"
    r"deserialize_end=(\d+)\s+instance_conn_end=(\d+)\s+instance_create_end=(\d+)"
)
# Format with sched_fetch timing but without post-receive timing
WORKER_SCHED_REQUEST_PATTERN_NEW = re.compile(
    r"\[TIMING\]\s+worker_id=(\S+)\s+task_id=(\S+)\s+get_next_task\s+"
    r"sched_fetch_start=(\d+)\s+sched_fetch_end=(\d+)\s+"
    r"request_start=(\d+)\s+connect_end=(\d+)\s+send_end=(\d+)\s+receive_end=(\d+)"
)
# Old format without sched_fetch timing
WORKER_SCHED_REQUEST_PATTERN_OLD = re.compile(
    r"\[TIMING\]\s+worker_id=(\S+)\s+task_id=(\S+)\s+get_next_task\s+"
    r"request_start=(\d+)\s+connect_end=(\d+)\s+send_end=(\d+)\s+receive_end=(\d+)"
)

# Storage connection timing patterns
STORAGE_CONNECT_PRE_PATTERN = re.compile(
    r"\[TIMING\]\s+task_id=(\S+)\s+"
    r"storage_connect_start=(\d+)\s+storage_connect_end=(\d+)\s+"
    r"storage_connect_duration_ms=\d+\s+phase=pre_execution"
)
STORAGE_CONNECT_RESULT_PATTERN = re.compile(
    r"\[TIMING\]\s+task_id=(\S+)\s+func=\S+\s+"
    r"storage_connect_start=(\d+)\s+storage_connect_end=(\d+)\s+"
    r"storage_connect_duration_ms=\d+\s+phase=handle_result"
)

# Python Task Executor TIMING patterns
PY_TIMING_DECODE = re.compile(
    r"\[TIMING\]\s+spider_task_id=(\S+)\s+"
    r"func_entry=(\d+)\s+decode_end=(\d+)"
)
PY_TIMING_CONFIG_LOAD = re.compile(
    r"\[TIMING\]\s+spider_task_id=(\S+)\s+"
    r"config_load_start=(\d+)\s+config_load_end=(\d+)"
)
PY_TIMING_CMD_BUILD = re.compile(
    r"\[TIMING\]\s+spider_task_id=(\S+)\s+"
    r"cmd_build_start=(\d+)\s+cmd_build_end=(\d+)"
)
PY_TIMING_SEARCH = re.compile(
    r"\[TIMING\]\s+spider_task_id=(\S+)\s+"
    r"search_start=(\d+)\s+search_end=(\d+)"
)
PY_TIMING_TOTAL = re.compile(
    r"\[TIMING\]\s+spider_task_id=(\S+)\s+"
    r"func_entry=(\d+)\s+func_exit=(\d+)"
)

# Query scheduler log patterns
SUBMIT_START_PATTERN = re.compile(
    r"Submitting Spider job (\S+) at (\S+) with (\d+) search tasks"
)
SUBMIT_END_PATTERN = re.compile(r"Submitted Spider job (\S+) at (\S+), submission took")
COMPLETE_PATTERN = re.compile(r"Completed job (\S+) at (\S+)")


def parse_log_timestamp(line: str) -> Optional[datetime]:
    """Extract timestamp from log line start."""
    match = LOG_TIMESTAMP_PATTERN.match(line)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S.%f")
    return None


def parse_py_log_timestamp(line: str) -> Optional[datetime]:
    """Extract timestamp from Python log line."""
    match = PY_LOG_TIMESTAMP_PATTERN.match(line)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S,%f")
    return None


def parse_iso_timestamp(ts_str: str) -> datetime:
    """Parse ISO format timestamp."""
    ts_str = ts_str.rstrip(",.")
    ts_str = ts_str.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts_str)
    except ValueError:
        if "+" in ts_str:
            ts_str = ts_str.split("+")[0]
        return datetime.fromisoformat(ts_str)


def epoch_to_datetime(epoch_ms: int, ref_epoch_ms: int, ref_datetime: datetime) -> datetime:
    """Convert epoch timestamp (ms) to datetime using a reference point."""
    delta_ms = epoch_ms - ref_epoch_ms
    return ref_datetime + timedelta(milliseconds=delta_ms)


def discover_worker_logs(log_dir: Path) -> list[Path]:
    """Find all spider_worker_*.log files."""
    search_dirs = [log_dir]
    if (log_dir / "logs").is_dir():
        search_dirs.insert(0, log_dir / "logs")

    worker_logs = []
    for search_dir in search_dirs:
        worker_logs.extend(search_dir.glob("spider_worker_*.log"))

    return sorted(worker_logs)


def parse_query_scheduler_log(path: Path) -> dict[str, JobInfo]:
    """Parse query_scheduler.log for job timing."""
    jobs: dict[str, JobInfo] = {}

    with open(path, "r") as f:
        for line in f:
            match = SUBMIT_START_PATTERN.search(line)
            if match:
                job_id, ts_str, _ = match.groups()
                if job_id not in jobs:
                    jobs[job_id] = JobInfo(job_id=job_id)
                jobs[job_id].submit_time = parse_iso_timestamp(ts_str)
                continue

            match = SUBMIT_END_PATTERN.search(line)
            if match:
                job_id, ts_str = match.groups()
                if job_id not in jobs:
                    jobs[job_id] = JobInfo(job_id=job_id)
                jobs[job_id].submit_end_time = parse_iso_timestamp(ts_str)
                continue

            match = COMPLETE_PATTERN.search(line)
            if match:
                job_id, ts_str = match.groups()
                if job_id not in jobs:
                    jobs[job_id] = JobInfo(job_id=job_id)
                jobs[job_id].complete_time = parse_iso_timestamp(ts_str)
                continue

    return jobs


def parse_worker_log(path: Path) -> tuple[str, dict[str, TaskTiming]]:
    """Parse a C++ worker log file for task timing."""
    filename = path.name
    worker_uuid = filename.replace("spider_worker_", "").replace(".log", "")
    worker_id = worker_uuid[:8]

    tasks: dict[str, TaskTiming] = {}
    ref_epoch_ms: Optional[int] = None
    ref_datetime: Optional[datetime] = None

    with open(path, "r") as f:
        for line in f:
            if "[TIMING]" not in line:
                continue

            log_timestamp = parse_log_timestamp(line)
            if not log_timestamp:
                continue

            # Parse worker scheduler request timing (get_next_task)
            if "get_next_task" in line:
                # Try v3 format first (with post-receive timing)
                match = WORKER_SCHED_REQUEST_PATTERN_V3.search(line)
                if match:
                    (_, task_id, sched_fetch_start, sched_fetch_end,
                     req_start, conn_end, send_end, recv_end,
                     deser_end, inst_conn_end, inst_create_end) = match.groups()
                    if task_id == "none":
                        continue

                    inst_create_end_epoch = int(inst_create_end)

                    if ref_epoch_ms is None:
                        ref_epoch_ms = inst_create_end_epoch
                        ref_datetime = log_timestamp

                    if task_id not in tasks:
                        tasks[task_id] = TaskTiming(task_id=task_id)

                    task = tasks[task_id]
                    task.sched_fetch_start = epoch_to_datetime(
                        int(sched_fetch_start), ref_epoch_ms, ref_datetime
                    )
                    task.sched_fetch_end = epoch_to_datetime(
                        int(sched_fetch_end), ref_epoch_ms, ref_datetime
                    )
                    task.worker_request_start = epoch_to_datetime(
                        int(req_start), ref_epoch_ms, ref_datetime
                    )
                    task.worker_connect_end = epoch_to_datetime(
                        int(conn_end), ref_epoch_ms, ref_datetime
                    )
                    task.worker_send_end = epoch_to_datetime(
                        int(send_end), ref_epoch_ms, ref_datetime
                    )
                    task.worker_receive_end = epoch_to_datetime(
                        int(recv_end), ref_epoch_ms, ref_datetime
                    )
                    task.deserialize_end = epoch_to_datetime(
                        int(deser_end), ref_epoch_ms, ref_datetime
                    )
                    task.instance_conn_end = epoch_to_datetime(
                        int(inst_conn_end), ref_epoch_ms, ref_datetime
                    )
                    task.instance_create_end = epoch_to_datetime(
                        inst_create_end_epoch, ref_epoch_ms, ref_datetime
                    )
                    continue

                # Try v2 format (with sched_fetch but without post-receive timing)
                match = WORKER_SCHED_REQUEST_PATTERN_NEW.search(line)
                if match:
                    (_, task_id, sched_fetch_start, sched_fetch_end,
                     req_start, conn_end, send_end, recv_end) = match.groups()
                    if task_id == "none":
                        continue

                    recv_end_epoch = int(recv_end)

                    if ref_epoch_ms is None:
                        ref_epoch_ms = recv_end_epoch
                        ref_datetime = log_timestamp

                    if task_id not in tasks:
                        tasks[task_id] = TaskTiming(task_id=task_id)

                    task = tasks[task_id]
                    task.sched_fetch_start = epoch_to_datetime(
                        int(sched_fetch_start), ref_epoch_ms, ref_datetime
                    )
                    task.sched_fetch_end = epoch_to_datetime(
                        int(sched_fetch_end), ref_epoch_ms, ref_datetime
                    )
                    task.worker_request_start = epoch_to_datetime(
                        int(req_start), ref_epoch_ms, ref_datetime
                    )
                    task.worker_connect_end = epoch_to_datetime(
                        int(conn_end), ref_epoch_ms, ref_datetime
                    )
                    task.worker_send_end = epoch_to_datetime(
                        int(send_end), ref_epoch_ms, ref_datetime
                    )
                    task.worker_receive_end = epoch_to_datetime(
                        recv_end_epoch, ref_epoch_ms, ref_datetime
                    )
                    continue

                # Fall back to old format (without sched_fetch)
                match = WORKER_SCHED_REQUEST_PATTERN_OLD.search(line)
                if match:
                    _, task_id, req_start, conn_end, send_end, recv_end = match.groups()
                    if task_id == "none":
                        continue

                    req_start_epoch = int(req_start)
                    recv_end_epoch = int(recv_end)

                    if ref_epoch_ms is None:
                        ref_epoch_ms = recv_end_epoch
                        ref_datetime = log_timestamp

                    if task_id not in tasks:
                        tasks[task_id] = TaskTiming(task_id=task_id)

                    task = tasks[task_id]
                    task.worker_request_start = epoch_to_datetime(
                        req_start_epoch, ref_epoch_ms, ref_datetime
                    )
                    task.worker_connect_end = epoch_to_datetime(
                        int(conn_end), ref_epoch_ms, ref_datetime
                    )
                    task.worker_send_end = epoch_to_datetime(
                        int(send_end), ref_epoch_ms, ref_datetime
                    )
                    task.worker_receive_end = epoch_to_datetime(
                        recv_end_epoch, ref_epoch_ms, ref_datetime
                    )
                continue

            # Parse storage connection timing
            if "storage_connect" in line:
                # Pre-execution phase
                match = STORAGE_CONNECT_PRE_PATTERN.search(line)
                if match:
                    task_id, start_str, end_str = match.groups()
                    start_epoch = int(start_str)
                    end_epoch = int(end_str)

                    if ref_epoch_ms is None:
                        ref_epoch_ms = end_epoch
                        ref_datetime = log_timestamp

                    if task_id not in tasks:
                        tasks[task_id] = TaskTiming(task_id=task_id)

                    task = tasks[task_id]
                    task.storage_connect_pre_start = epoch_to_datetime(
                        start_epoch, ref_epoch_ms, ref_datetime
                    )
                    task.storage_connect_pre_end = epoch_to_datetime(
                        end_epoch, ref_epoch_ms, ref_datetime
                    )
                    continue

                # Handle result phase
                match = STORAGE_CONNECT_RESULT_PATTERN.search(line)
                if match:
                    task_id, start_str, end_str = match.groups()
                    start_epoch = int(start_str)
                    end_epoch = int(end_str)

                    if ref_epoch_ms is None:
                        ref_epoch_ms = end_epoch
                        ref_datetime = log_timestamp

                    if task_id not in tasks:
                        tasks[task_id] = TaskTiming(task_id=task_id)

                    task = tasks[task_id]
                    task.storage_connect_result_start = epoch_to_datetime(
                        start_epoch, ref_epoch_ms, ref_datetime
                    )
                    task.storage_connect_result_end = epoch_to_datetime(
                        end_epoch, ref_epoch_ms, ref_datetime
                    )
                continue

            # Parse task execution timing (search tasks only)
            if "search" not in line:
                continue

            for phase_name, pattern in CPP_TIMING_PATTERNS.items():
                match = pattern.search(line)
                if not match:
                    continue

                task_id, func_name, start_str, end_str = match.groups()
                if "search" not in func_name:
                    continue

                start_epoch = int(start_str)
                end_epoch = int(end_str)

                if ref_epoch_ms is None:
                    ref_epoch_ms = end_epoch
                    ref_datetime = log_timestamp

                start_dt = epoch_to_datetime(start_epoch, ref_epoch_ms, ref_datetime)
                end_dt = epoch_to_datetime(end_epoch, ref_epoch_ms, ref_datetime)

                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id)

                task = tasks[task_id]
                if phase_name == "fetch_input":
                    task.fetch_start = start_dt
                    task.fetch_end = end_dt
                elif phase_name == "spawn":
                    task.spawn_start = start_dt
                    task.spawn_end = end_dt
                elif phase_name == "input_send":
                    task.input_send_start = start_dt
                    task.input_send_end = end_dt
                elif phase_name == "execution":
                    task.execution_start = start_dt
                    task.execution_end = end_dt
                elif phase_name == "submit_output":
                    task.submit_start = start_dt
                    task.submit_end = end_dt

                break

    return worker_id, tasks


def parse_python_worker_log(path: Path) -> dict[str, TaskTiming]:
    """Parse worker.log for Python task executor timing."""
    tasks: dict[str, TaskTiming] = {}
    ref_epoch_ms: Optional[int] = None
    ref_datetime: Optional[datetime] = None

    with open(path, "r") as f:
        for line in f:
            if "[TIMING]" not in line or "spider_search" not in line:
                continue

            log_timestamp = parse_py_log_timestamp(line)
            if not log_timestamp:
                continue

            # Decode
            match = PY_TIMING_DECODE.search(line)
            if match:
                task_id, entry_str, decode_str = match.groups()
                if ref_epoch_ms is None:
                    ref_epoch_ms = int(decode_str)
                    ref_datetime = log_timestamp
                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id)
                tasks[task_id].py_func_entry = epoch_to_datetime(
                    int(entry_str), ref_epoch_ms, ref_datetime
                )
                tasks[task_id].py_decode_end = epoch_to_datetime(
                    int(decode_str), ref_epoch_ms, ref_datetime
                )
                continue

            # Config load
            match = PY_TIMING_CONFIG_LOAD.search(line)
            if match:
                task_id, start_str, end_str = match.groups()
                if ref_epoch_ms is None:
                    ref_epoch_ms = int(end_str)
                    ref_datetime = log_timestamp
                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id)
                tasks[task_id].py_config_load_start = epoch_to_datetime(
                    int(start_str), ref_epoch_ms, ref_datetime
                )
                tasks[task_id].py_config_load_end = epoch_to_datetime(
                    int(end_str), ref_epoch_ms, ref_datetime
                )
                continue

            # Command build
            match = PY_TIMING_CMD_BUILD.search(line)
            if match:
                task_id, start_str, end_str = match.groups()
                if ref_epoch_ms is None:
                    ref_epoch_ms = int(end_str)
                    ref_datetime = log_timestamp
                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id)
                tasks[task_id].py_cmd_build_start = epoch_to_datetime(
                    int(start_str), ref_epoch_ms, ref_datetime
                )
                tasks[task_id].py_cmd_build_end = epoch_to_datetime(
                    int(end_str), ref_epoch_ms, ref_datetime
                )
                continue

            # Search
            match = PY_TIMING_SEARCH.search(line)
            if match:
                task_id, start_str, end_str = match.groups()
                if ref_epoch_ms is None:
                    ref_epoch_ms = int(end_str)
                    ref_datetime = log_timestamp
                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id)
                tasks[task_id].py_search_start = epoch_to_datetime(
                    int(start_str), ref_epoch_ms, ref_datetime
                )
                tasks[task_id].py_search_end = epoch_to_datetime(
                    int(end_str), ref_epoch_ms, ref_datetime
                )
                continue

            # Total
            match = PY_TIMING_TOTAL.search(line)
            if match:
                task_id, entry_str, exit_str = match.groups()
                if ref_epoch_ms is None:
                    ref_epoch_ms = int(exit_str)
                    ref_datetime = log_timestamp
                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id)
                tasks[task_id].py_func_exit = epoch_to_datetime(
                    int(exit_str), ref_epoch_ms, ref_datetime
                )
                continue

    return tasks


def merge_task_timing(base: TaskTiming, other: TaskTiming) -> None:
    """Merge timing from other into base."""
    for attr in [
        "sched_fetch_start", "sched_fetch_end",
        "worker_request_start", "worker_connect_end",
        "worker_send_end", "worker_receive_end",
        "deserialize_end", "instance_conn_end", "instance_create_end",
        "storage_connect_pre_start", "storage_connect_pre_end",
        "storage_connect_result_start", "storage_connect_result_end",
        "fetch_start", "fetch_end", "spawn_start", "spawn_end",
        "input_send_start", "input_send_end", "execution_start", "execution_end",
        "submit_start", "submit_end",
        "py_func_entry", "py_decode_end",
        "py_config_load_start", "py_config_load_end",
        "py_cmd_build_start", "py_cmd_build_end",
        "py_search_start", "py_search_end", "py_func_exit",
    ]:
        other_val = getattr(other, attr)
        if other_val is not None and getattr(base, attr) is None:
            setattr(base, attr, other_val)


def get_task_start_time(task: TaskTiming) -> Optional[datetime]:
    """Get earliest timestamp for a task."""
    times = [
        t for t in [
            task.sched_fetch_start, task.worker_request_start, task.fetch_start,
            task.spawn_start, task.execution_start
        ] if t
    ]
    return min(times) if times else None


def get_task_end_time(task: TaskTiming) -> Optional[datetime]:
    """Get latest timestamp for a task."""
    times = [
        t for t in [
            task.submit_end, task.execution_end
        ] if t
    ]
    return max(times) if times else None


def filter_tasks_by_time_range(
    tasks: dict[str, TaskTiming],
    start_time: datetime,
    end_time: datetime,
) -> dict[str, TaskTiming]:
    """Filter tasks to those within the given time range."""
    filtered = {}
    for task_id, task in tasks.items():
        task_start = get_task_start_time(task)
        if task_start and start_time <= task_start <= end_time:
            filtered[task_id] = task
    return filtered


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def compute_statistics(
    all_tasks: list[TaskTiming],
    worker_task_sequences: Optional[dict[str, list[TaskTiming]]] = None,
) -> dict[str, dict]:
    """Compute timing statistics for all tasks."""
    stats: dict[str, list[float]] = {
        "sched_fetch": [],
        "worker_connect": [],
        "worker_send": [],
        "worker_receive": [],
        "worker_total": [],
        "worker_total_with_fetch": [],
        "deserialize": [],
        "instance_conn": [],
        "instance_create": [],
        "post_receive_total": [],
        "receive_to_fetch_gap": [],
        "inter_task_gap": [],
        "exec_to_exec_gap": [],
        "storage_connect_pre": [],
        "storage_connect_result": [],
        "fetch_duration": [],
        "spawn_duration": [],
        "input_send_duration": [],
        "cpp_exec_duration": [],
        "submit_duration": [],
        "py_decode_duration": [],
        "py_config_load_duration": [],
        "py_cmd_build_duration": [],
        "py_search_duration": [],
        "py_total_duration": [],
        "py_startup_gap": [],
        "py_shutdown_gap": [],
    }

    for task in all_tasks:
        # Scheduler fetch phase
        if task.sched_fetch_start and task.sched_fetch_end:
            stats["sched_fetch"].append(
                (task.sched_fetch_end - task.sched_fetch_start).total_seconds() * 1000
            )
        # Total including sched_fetch
        if task.sched_fetch_start and task.worker_receive_end:
            stats["worker_total_with_fetch"].append(
                (task.worker_receive_end - task.sched_fetch_start).total_seconds() * 1000
            )

        # Worker scheduler request phases
        if task.worker_request_start and task.worker_connect_end:
            stats["worker_connect"].append(
                (task.worker_connect_end - task.worker_request_start).total_seconds() * 1000
            )
        if task.worker_connect_end and task.worker_send_end:
            stats["worker_send"].append(
                (task.worker_send_end - task.worker_connect_end).total_seconds() * 1000
            )
        if task.worker_send_end and task.worker_receive_end:
            stats["worker_receive"].append(
                (task.worker_receive_end - task.worker_send_end).total_seconds() * 1000
            )
        if task.worker_request_start and task.worker_receive_end:
            stats["worker_total"].append(
                (task.worker_receive_end - task.worker_request_start).total_seconds() * 1000
            )

        # Post-receive phases
        if task.worker_receive_end and task.deserialize_end:
            stats["deserialize"].append(
                (task.deserialize_end - task.worker_receive_end).total_seconds() * 1000
            )
        if task.deserialize_end and task.instance_conn_end:
            stats["instance_conn"].append(
                (task.instance_conn_end - task.deserialize_end).total_seconds() * 1000
            )
        if task.instance_conn_end and task.instance_create_end:
            stats["instance_create"].append(
                (task.instance_create_end - task.instance_conn_end).total_seconds() * 1000
            )
        if task.worker_receive_end and task.instance_create_end:
            stats["post_receive_total"].append(
                (task.instance_create_end - task.worker_receive_end).total_seconds() * 1000
            )
        # Gap from instance_create_end (or receive_end) to fetch_start
        if task.fetch_start:
            if task.instance_create_end:
                gap = (task.fetch_start - task.instance_create_end).total_seconds() * 1000
                if gap >= 0:
                    stats["receive_to_fetch_gap"].append(gap)
            elif task.worker_receive_end:
                gap = (task.fetch_start - task.worker_receive_end).total_seconds() * 1000
                if gap >= 0:
                    stats["receive_to_fetch_gap"].append(gap)

        # Storage connection phases
        if task.storage_connect_pre_start and task.storage_connect_pre_end:
            stats["storage_connect_pre"].append(
                (task.storage_connect_pre_end - task.storage_connect_pre_start).total_seconds() * 1000
            )
        if task.storage_connect_result_start and task.storage_connect_result_end:
            stats["storage_connect_result"].append(
                (task.storage_connect_result_end - task.storage_connect_result_start).total_seconds() * 1000
            )

        # C++ phases
        if task.fetch_start and task.fetch_end:
            stats["fetch_duration"].append(
                (task.fetch_end - task.fetch_start).total_seconds() * 1000
            )
        if task.spawn_start and task.spawn_end:
            stats["spawn_duration"].append(
                (task.spawn_end - task.spawn_start).total_seconds() * 1000
            )
        if task.input_send_start and task.input_send_end:
            stats["input_send_duration"].append(
                (task.input_send_end - task.input_send_start).total_seconds() * 1000
            )
        if task.execution_start and task.execution_end:
            stats["cpp_exec_duration"].append(
                (task.execution_end - task.execution_start).total_seconds() * 1000
            )
        if task.submit_start and task.submit_end:
            stats["submit_duration"].append(
                (task.submit_end - task.submit_start).total_seconds() * 1000
            )

        # Python phases
        if task.py_func_entry and task.py_decode_end:
            stats["py_decode_duration"].append(
                (task.py_decode_end - task.py_func_entry).total_seconds() * 1000
            )
        if task.py_config_load_start and task.py_config_load_end:
            stats["py_config_load_duration"].append(
                (task.py_config_load_end - task.py_config_load_start).total_seconds() * 1000
            )
        if task.py_cmd_build_start and task.py_cmd_build_end:
            stats["py_cmd_build_duration"].append(
                (task.py_cmd_build_end - task.py_cmd_build_start).total_seconds() * 1000
            )
        if task.py_search_start and task.py_search_end:
            stats["py_search_duration"].append(
                (task.py_search_end - task.py_search_start).total_seconds() * 1000
            )
        if task.py_func_entry and task.py_func_exit:
            stats["py_total_duration"].append(
                (task.py_func_exit - task.py_func_entry).total_seconds() * 1000
            )

        # Gaps
        if task.spawn_end and task.py_func_entry:
            gap = (task.py_func_entry - task.spawn_end).total_seconds() * 1000
            if gap >= 0:
                stats["py_startup_gap"].append(gap)
        if task.py_func_exit and task.execution_end:
            gap = (task.execution_end - task.py_func_exit).total_seconds() * 1000
            if gap >= 0:
                stats["py_shutdown_gap"].append(gap)

    # Inter-task gaps for consecutive tasks on the same worker
    if worker_task_sequences:
        for worker_id, tasks in worker_task_sequences.items():
            for i in range(1, len(tasks)):
                prev_task = tasks[i - 1]
                curr_task = tasks[i]
                next_start = curr_task.sched_fetch_start or curr_task.worker_request_start
                if prev_task.submit_end and next_start:
                    gap = (next_start - prev_task.submit_end).total_seconds() * 1000
                    if gap >= 0:
                        stats["inter_task_gap"].append(gap)
                if prev_task.execution_end and curr_task.execution_start:
                    gap = (curr_task.execution_start - prev_task.execution_end).total_seconds() * 1000
                    if gap >= 0:
                        stats["exec_to_exec_gap"].append(gap)

    results = {}
    for name, values in stats.items():
        if values:
            results[name] = {
                "count": len(values),
                "mean": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
                "total": sum(values),
            }
        else:
            results[name] = {"count": 0, "mean": 0, "min": 0, "max": 0, "total": 0}

    return results


def project_worker_timeline(
    worker: WorkerTimeline,
    startup_speedup: float,
    shutdown_speedup: float,
) -> WorkerTimeline:
    """
    Create a projected timeline for a worker where:
    1. Each task's internal duration is reduced by the speedup factors
    2. The gap between consecutive tasks is preserved
    3. Subsequent tasks are shifted earlier because previous tasks complete faster
    """
    if not worker.tasks:
        return WorkerTimeline(worker_id=worker.worker_id, tasks=[])

    projected_tasks: list[TaskTiming] = []
    cumulative_shift = timedelta(0)

    for i, task in enumerate(worker.tasks):
        projected = copy.deepcopy(task)
        task_start = get_task_start_time(task)
        task_end = get_task_end_time(task)

        if not task_start or not task_end:
            projected_tasks.append(projected)
            continue

        shift = cumulative_shift

        # Calculate the speedup savings for this task
        startup_savings = timedelta(0)
        shutdown_savings = timedelta(0)

        if task.spawn_end and task.py_func_entry:
            startup_gap = task.py_func_entry - task.spawn_end
            if startup_gap.total_seconds() > 0:
                new_startup_gap = startup_gap / startup_speedup
                startup_savings = startup_gap - new_startup_gap

        if task.py_func_exit and task.execution_end:
            shutdown_gap = task.execution_end - task.py_func_exit
            if shutdown_gap.total_seconds() > 0:
                new_shutdown_gap = shutdown_gap / shutdown_speedup
                shutdown_savings = shutdown_gap - new_shutdown_gap

        # Shift all pre-execution timestamps by cumulative shift
        if projected.sched_fetch_start:
            projected.sched_fetch_start = projected.sched_fetch_start - shift
        if projected.sched_fetch_end:
            projected.sched_fetch_end = projected.sched_fetch_end - shift
        if projected.worker_request_start:
            projected.worker_request_start = projected.worker_request_start - shift
        if projected.worker_connect_end:
            projected.worker_connect_end = projected.worker_connect_end - shift
        if projected.worker_send_end:
            projected.worker_send_end = projected.worker_send_end - shift
        if projected.worker_receive_end:
            projected.worker_receive_end = projected.worker_receive_end - shift
        if projected.deserialize_end:
            projected.deserialize_end = projected.deserialize_end - shift
        if projected.instance_conn_end:
            projected.instance_conn_end = projected.instance_conn_end - shift
        if projected.instance_create_end:
            projected.instance_create_end = projected.instance_create_end - shift
        if projected.storage_connect_pre_start:
            projected.storage_connect_pre_start = projected.storage_connect_pre_start - shift
        if projected.storage_connect_pre_end:
            projected.storage_connect_pre_end = projected.storage_connect_pre_end - shift
        if projected.fetch_start:
            projected.fetch_start = projected.fetch_start - shift
        if projected.fetch_end:
            projected.fetch_end = projected.fetch_end - shift
        if projected.spawn_start:
            projected.spawn_start = projected.spawn_start - shift
        if projected.spawn_end:
            projected.spawn_end = projected.spawn_end - shift
        if projected.input_send_start:
            projected.input_send_start = projected.input_send_start - shift
        if projected.input_send_end:
            projected.input_send_end = projected.input_send_end - shift
        if projected.execution_start:
            projected.execution_start = projected.execution_start - shift

        # Shift Python timestamps by cumulative shift + startup savings
        py_shift = shift + startup_savings
        if projected.py_func_entry:
            projected.py_func_entry = projected.py_func_entry - py_shift
        if projected.py_decode_end:
            projected.py_decode_end = projected.py_decode_end - py_shift
        if projected.py_config_load_start:
            projected.py_config_load_start = projected.py_config_load_start - py_shift
        if projected.py_config_load_end:
            projected.py_config_load_end = projected.py_config_load_end - py_shift
        if projected.py_cmd_build_start:
            projected.py_cmd_build_start = projected.py_cmd_build_start - py_shift
        if projected.py_cmd_build_end:
            projected.py_cmd_build_end = projected.py_cmd_build_end - py_shift
        if projected.py_search_start:
            projected.py_search_start = projected.py_search_start - py_shift
        if projected.py_search_end:
            projected.py_search_end = projected.py_search_end - py_shift
        if projected.py_func_exit:
            projected.py_func_exit = projected.py_func_exit - py_shift

        # Shift post-execution timestamps by cumulative shift + startup + shutdown savings
        total_task_shift = shift + startup_savings + shutdown_savings
        if projected.execution_end:
            projected.execution_end = projected.execution_end - total_task_shift
        if projected.storage_connect_result_start:
            projected.storage_connect_result_start = projected.storage_connect_result_start - total_task_shift
        if projected.storage_connect_result_end:
            projected.storage_connect_result_end = projected.storage_connect_result_end - total_task_shift
        if projected.submit_start:
            projected.submit_start = projected.submit_start - total_task_shift
        if projected.submit_end:
            projected.submit_end = projected.submit_end - total_task_shift

        # Update cumulative shift for next task
        cumulative_shift = cumulative_shift + startup_savings + shutdown_savings

        projected_tasks.append(projected)

    return WorkerTimeline(worker_id=worker.worker_id, tasks=projected_tasks)


def print_comparison_statistics(
    original_stats: dict[str, dict],
    projected_stats: dict[str, dict],
    original_job_duration: float,
    projected_job_duration: float,
    startup_speedup: float,
    shutdown_speedup: float,
) -> None:
    """Print comparison of original vs projected statistics."""
    print("\n" + "=" * 80)
    print("PROJECTED C++ EXECUTOR PERFORMANCE COMPARISON")
    print(f"Speedup factors: Startup={startup_speedup}x, Shutdown={shutdown_speedup}x")
    print("=" * 80)

    # Key metrics to compare
    comparisons = [
        ("Startup Gap", "py_startup_gap"),
        ("Shutdown Gap", "py_shutdown_gap"),
        ("Python Total", "py_total_duration"),
        ("C++ Execution", "cpp_exec_duration"),
    ]

    print("\n" + "-" * 80)
    print(f"{'Phase':<25} {'Original (ms)':>15} {'Projected (ms)':>15} {'Speedup':>12}")
    print("-" * 80)

    for label, key in comparisons:
        orig = original_stats.get(key, {})
        proj = projected_stats.get(key, {})
        orig_mean = orig.get("mean", 0)
        proj_mean = proj.get("mean", 0)
        speedup = orig_mean / proj_mean if proj_mean > 0 else float('inf')
        print(f"{label:<25} {orig_mean:>15.2f} {proj_mean:>15.2f} {speedup:>11.1f}x")

    # Calculate total task execution time
    orig_startup = original_stats.get("py_startup_gap", {}).get("mean", 0)
    orig_shutdown = original_stats.get("py_shutdown_gap", {}).get("mean", 0)
    orig_py_total = original_stats.get("py_total_duration", {}).get("mean", 0)

    proj_startup = projected_stats.get("py_startup_gap", {}).get("mean", 0)
    proj_shutdown = projected_stats.get("py_shutdown_gap", {}).get("mean", 0)
    proj_py_total = projected_stats.get("py_total_duration", {}).get("mean", 0)

    orig_task_exec = orig_startup + orig_py_total + orig_shutdown
    proj_task_exec = proj_startup + proj_py_total + proj_shutdown

    print("-" * 80)
    print(f"{'TOTAL TASK EXECUTION':<25} {orig_task_exec:>15.2f} {proj_task_exec:>15.2f} "
          f"{orig_task_exec/proj_task_exec if proj_task_exec > 0 else 0:>11.1f}x")
    print("=" * 80)

    # Breakdown of savings
    startup_savings = orig_startup - proj_startup
    shutdown_savings = orig_shutdown - proj_shutdown
    total_savings = startup_savings + shutdown_savings

    print("\nTime Savings per Task:")
    print(f"  Startup gap reduction:  {startup_savings:.2f} ms")
    print(f"  Shutdown gap reduction: {shutdown_savings:.2f} ms")
    print(f"  Total savings:          {total_savings:.2f} ms ({total_savings/orig_task_exec*100:.1f}% of execution)")

    # End-to-end job time comparison
    print("\n" + "=" * 80)
    print("END-TO-END JOB TIME COMPARISON")
    print("=" * 80)
    print(f"  Original job duration:  {format_duration(original_job_duration)} ({original_job_duration*1000:.1f} ms)")
    print(f"  Projected job duration: {format_duration(projected_job_duration)} ({projected_job_duration*1000:.1f} ms)")
    job_speedup = original_job_duration / projected_job_duration if projected_job_duration > 0 else 0
    print(f"  Job speedup:            {job_speedup:.2f}x")
    print("=" * 80)


def render_timeline(
    worker_timelines: list[WorkerTimeline],
    title: str,
    job_info: Optional[JobInfo] = None,
    output_path: Optional[Path] = None,
    figsize: tuple[int, int] = (16, 12),
    subtitle: str = "",
) -> None:
    """Render timeline visualization."""
    if not worker_timelines:
        print("No timeline data available.")
        return

    # Find time bounds
    all_times: list[datetime] = []
    for wt in worker_timelines:
        for task in wt.tasks:
            for t in [
                task.sched_fetch_start, task.sched_fetch_end,
                task.worker_request_start, task.worker_receive_end,
                task.deserialize_end, task.instance_conn_end, task.instance_create_end,
                task.storage_connect_pre_start, task.storage_connect_pre_end,
                task.storage_connect_result_start, task.storage_connect_result_end,
                task.fetch_start, task.fetch_end,
                task.spawn_start, task.spawn_end,
                task.execution_start, task.execution_end,
                task.submit_start, task.submit_end,
                task.py_func_entry, task.py_func_exit,
            ]:
                if t:
                    all_times.append(t)

    if not all_times:
        print("No tasks with timing data.")
        return

    tl_start = job_info.submit_time if (job_info and job_info.submit_time) else min(all_times)
    tl_end = max(all_times)
    if job_info and job_info.complete_time:
        tl_end = max(tl_end, job_info.complete_time)

    total_seconds = (tl_end - tl_start).total_seconds()
    if total_seconds <= 0:
        total_seconds = 1.0

    fig, ax = plt.subplots(figsize=figsize)

    colors = {
        "sched_fetch": "#facc15",
        "worker_req": "#f97316",
        "deserialize": "#fb923c",
        "instance_conn": "#c084fc",
        "instance_create": "#a78bfa",
        "storage_conn": "#06b6d4",
        "fetch": "#2ecc71",
        "spawn": "#9b59b6",
        "input_send": "#8e44ad",
        "execution": "#3498db",
        "submit": "#e74c3c",
        "py_startup": "#ff6b6b",
        "py_decode": "#4ecdc4",
        "py_config": "#a855f7",
        "py_cmd": "#6b7280",
        "py_search": "#22c55e",
        "py_shutdown": "#fbbf24",
    }

    total_tasks = sum(len(wt.tasks) for wt in worker_timelines)
    bar_height = 0.25
    row_spacing = 0.05
    task_spacing = 0.2
    current_row = 0
    worker_boundaries: list[tuple[float, float, str]] = []

    for wt in worker_timelines:
        if not wt.tasks:
            continue

        worker_start_row = current_row

        for task in wt.tasks:
            cpp_row = current_row

            # Draw sched_fetch bar
            if task.sched_fetch_start and task.sched_fetch_end:
                start_offset = (task.sched_fetch_start - tl_start).total_seconds()
                width = (task.sched_fetch_end - task.sched_fetch_start).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, cpp_row - bar_height / 2),
                        width, bar_height,
                        facecolor=colors["sched_fetch"], edgecolor="none", alpha=0.9
                    )
                    ax.add_patch(rect)

            # Draw worker request bar
            if task.worker_request_start and task.worker_receive_end:
                start_offset = (task.worker_request_start - tl_start).total_seconds()
                width = (task.worker_receive_end - task.worker_request_start).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, cpp_row - bar_height / 2),
                    width, bar_height,
                    facecolor=colors["worker_req"], edgecolor="none", alpha=0.9
                )
                ax.add_patch(rect)

            # Draw post-receive phases
            if task.worker_receive_end and task.deserialize_end:
                start_offset = (task.worker_receive_end - tl_start).total_seconds()
                width = (task.deserialize_end - task.worker_receive_end).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, cpp_row - bar_height / 2),
                        width, bar_height,
                        facecolor=colors["deserialize"], edgecolor="none", alpha=0.9
                    )
                    ax.add_patch(rect)

            if task.deserialize_end and task.instance_conn_end:
                start_offset = (task.deserialize_end - tl_start).total_seconds()
                width = (task.instance_conn_end - task.deserialize_end).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, cpp_row - bar_height / 2),
                        width, bar_height,
                        facecolor=colors["instance_conn"], edgecolor="none", alpha=0.9
                    )
                    ax.add_patch(rect)

            if task.instance_conn_end and task.instance_create_end:
                start_offset = (task.instance_conn_end - tl_start).total_seconds()
                width = (task.instance_create_end - task.instance_conn_end).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, cpp_row - bar_height / 2),
                        width, bar_height,
                        facecolor=colors["instance_create"], edgecolor="none", alpha=0.9
                    )
                    ax.add_patch(rect)

            for phase, color, start_attr, end_attr in [
                ("storage_conn_pre", "storage_conn", "storage_connect_pre_start", "storage_connect_pre_end"),
                ("fetch", "fetch", "fetch_start", "fetch_end"),
                ("spawn", "spawn", "spawn_start", "spawn_end"),
                ("input_send", "input_send", "input_send_start", "input_send_end"),
                ("execution", "execution", "execution_start", "execution_end"),
                ("storage_conn_result", "storage_conn", "storage_connect_result_start", "storage_connect_result_end"),
                ("submit", "submit", "submit_start", "submit_end"),
            ]:
                start_t = getattr(task, start_attr)
                end_t = getattr(task, end_attr)
                if start_t and end_t:
                    start_offset = (start_t - tl_start).total_seconds()
                    width = (end_t - start_t).total_seconds()
                    if width > 0:
                        rect = mpatches.Rectangle(
                            (start_offset, cpp_row - bar_height / 2),
                            width, bar_height,
                            facecolor=colors[color], edgecolor="none", alpha=0.9
                        )
                        ax.add_patch(rect)

            # Python phases row
            py_row = current_row + bar_height + row_spacing

            # Startup gap
            if task.spawn_end and task.py_func_entry:
                start_offset = (task.spawn_end - tl_start).total_seconds()
                width = (task.py_func_entry - task.spawn_end).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, py_row - bar_height / 2),
                        width, bar_height,
                        facecolor=colors["py_startup"], edgecolor="none", alpha=0.9
                    )
                    ax.add_patch(rect)

            # Decode
            if task.py_func_entry and task.py_decode_end:
                start_offset = (task.py_func_entry - tl_start).total_seconds()
                width = (task.py_decode_end - task.py_func_entry).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, py_row - bar_height / 2),
                        width, bar_height,
                        facecolor=colors["py_decode"], edgecolor="none", alpha=0.9
                    )
                    ax.add_patch(rect)

            # Config load
            if task.py_config_load_start and task.py_config_load_end:
                start_offset = (task.py_config_load_start - tl_start).total_seconds()
                width = (task.py_config_load_end - task.py_config_load_start).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, py_row - bar_height / 2),
                        width, bar_height,
                        facecolor=colors["py_config"], edgecolor="none", alpha=0.9
                    )
                    ax.add_patch(rect)

            # Command build
            if task.py_cmd_build_start and task.py_cmd_build_end:
                start_offset = (task.py_cmd_build_start - tl_start).total_seconds()
                width = (task.py_cmd_build_end - task.py_cmd_build_start).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, py_row - bar_height / 2),
                        width, bar_height,
                        facecolor=colors["py_cmd"], edgecolor="none", alpha=0.9
                    )
                    ax.add_patch(rect)

            # Search
            if task.py_search_start and task.py_search_end:
                start_offset = (task.py_search_start - tl_start).total_seconds()
                width = (task.py_search_end - task.py_search_start).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, py_row - bar_height / 2),
                        width, bar_height,
                        facecolor=colors["py_search"], edgecolor="none", alpha=0.9
                    )
                    ax.add_patch(rect)

            # Shutdown gap
            if task.py_func_exit and task.execution_end:
                start_offset = (task.py_func_exit - tl_start).total_seconds()
                width = (task.execution_end - task.py_func_exit).total_seconds()
                if width > 0:
                    rect = mpatches.Rectangle(
                        (start_offset, py_row - bar_height / 2),
                        width, bar_height,
                        facecolor=colors["py_shutdown"], edgecolor="none", alpha=0.9
                    )
                    ax.add_patch(rect)

            current_row += 2 * bar_height + row_spacing + task_spacing

        worker_boundaries.append((worker_start_row, current_row - task_spacing, wt.worker_id))

    # Worker separators
    for i, (start_row, end_row, _) in enumerate(worker_boundaries):
        if i > 0:
            ax.axhline(y=start_row - 0.3, color="gray", linestyle="-", linewidth=0.5, alpha=0.5)

    # Job event lines
    if job_info:
        if job_info.submit_time:
            offset = (job_info.submit_time - tl_start).total_seconds()
            ax.axvline(x=offset, color="blue", linestyle=":", linewidth=2)
        if job_info.submit_end_time:
            offset = (job_info.submit_end_time - tl_start).total_seconds()
            ax.axvline(x=offset, color="blue", linestyle="-", linewidth=2)
        if job_info.complete_time:
            offset = (job_info.complete_time - tl_start).total_seconds()
            ax.axvline(x=offset, color="red", linestyle="-", linewidth=2)

    ax.set_xlim(-0.5, total_seconds + 0.5)
    ax.set_ylim(-0.5, current_row)

    y_ticks = [(s + e) / 2 for s, e, _ in worker_boundaries]
    y_labels = [w for _, _, w in worker_boundaries]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels, fontsize=8)

    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Worker")

    legend_handles = [
        mpatches.Patch(color=colors["sched_fetch"], alpha=0.9, label="Sched Fetch"),
        mpatches.Patch(color=colors["worker_req"], alpha=0.9, label="Worker Request"),
        mpatches.Patch(color=colors["deserialize"], alpha=0.9, label="Deserialize"),
        mpatches.Patch(color=colors["instance_conn"], alpha=0.9, label="Inst Conn"),
        mpatches.Patch(color=colors["instance_create"], alpha=0.9, label="Inst Create"),
        mpatches.Patch(color=colors["storage_conn"], alpha=0.9, label="Storage Conn"),
        mpatches.Patch(color=colors["fetch"], alpha=0.9, label="Fetch Input"),
        mpatches.Patch(color=colors["spawn"], alpha=0.9, label="Spawn"),
        mpatches.Patch(color=colors["execution"], alpha=0.9, label="Execution"),
        mpatches.Patch(color=colors["submit"], alpha=0.9, label="Submit"),
        mpatches.Patch(color=colors["py_startup"], alpha=0.9, label="Py Startup"),
        mpatches.Patch(color=colors["py_decode"], alpha=0.9, label="Py Decode"),
        mpatches.Patch(color=colors["py_config"], alpha=0.9, label="Py Config"),
        mpatches.Patch(color=colors["py_search"], alpha=0.9, label="Py Search"),
        mpatches.Patch(color=colors["py_shutdown"], alpha=0.9, label="Py Shutdown"),
    ]
    ax.legend(handles=legend_handles, loc="upper right", fontsize=6, ncol=5)

    num_workers = len(worker_boundaries)
    full_title = f"{title}\n{num_workers} workers, {total_tasks} tasks, Duration: {format_duration(total_seconds)}"
    if subtitle:
        full_title += f"\n{subtitle}"
    ax.set_title(full_title, fontsize=12)

    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Timeline saved to {output_path}")
    else:
        plt.show()

    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(
        description="Visualize projected Spider search task timeline with C++ executor speedups (v2).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/logs/                  Visualize projected timeline
  %(prog)s /path/to/logs/ --job-id 3       Filter to specific job
  %(prog)s /path/to/logs/ -o projected.png Save to file
  %(prog)s /path/to/logs/ --startup-speedup 50 --shutdown-speedup 25  Custom speedups
""",
    )
    parser.add_argument("log_dir", type=Path, help="Directory containing log files")
    parser.add_argument("--job-id", "-j", help="Filter to specific job ID")
    parser.add_argument("--output", "-o", type=Path, help="Output file (PNG, PDF, etc.)")
    parser.add_argument(
        "--figsize",
        type=str,
        default="16,12",
        help="Figure size as width,height in inches (default: 16,12)",
    )
    parser.add_argument(
        "--startup-speedup",
        type=float,
        default=61.6,
        help="Startup gap speedup factor (default: 61.6x from Spider microbenchmark)",
    )
    parser.add_argument(
        "--shutdown-speedup",
        type=float,
        default=30.0,
        help="Shutdown gap speedup factor (default: 30.0x from Spider microbenchmark)",
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Only print comparison statistics, no visualization",
    )

    args = parser.parse_args()

    if not args.log_dir.is_dir():
        print(f"Error: {args.log_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    try:
        figsize = tuple(map(int, args.figsize.split(",")))
        if len(figsize) != 2:
            raise ValueError()
    except ValueError:
        print(f"Error: Invalid figsize format: {args.figsize}", file=sys.stderr)
        sys.exit(1)

    # Discover worker logs
    worker_logs = discover_worker_logs(args.log_dir)
    if not worker_logs:
        print("No spider_worker_*.log files found.", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(worker_logs)} worker log files.")

    # Parse query_scheduler.log for job info
    job_info: Optional[JobInfo] = None
    job_time_range: Optional[tuple[datetime, datetime]] = None

    query_sched_log = args.log_dir / "query_scheduler.log"
    if query_sched_log.exists():
        all_jobs = parse_query_scheduler_log(query_sched_log)
        if args.job_id:
            if args.job_id in all_jobs:
                job_info = all_jobs[args.job_id]
                if job_info.submit_time and job_info.complete_time:
                    job_time_range = (job_info.submit_time, job_info.complete_time)
                    print(f"Filtering to job {args.job_id}")
            else:
                print(f"Warning: Job {args.job_id} not found.")

    # Parse worker.log (Python executor)
    py_tasks: dict[str, TaskTiming] = {}
    worker_log = args.log_dir / "worker.log"
    if worker_log.exists():
        py_tasks = parse_python_worker_log(worker_log)
        print(f"Found {len(py_tasks)} Python task entries.")

    # Parse C++ worker logs
    worker_timelines: list[WorkerTimeline] = []
    all_tasks: dict[str, TaskTiming] = {}

    for log_path in worker_logs:
        worker_id, tasks = parse_worker_log(log_path)

        if job_time_range:
            tasks = filter_tasks_by_time_range(tasks, *job_time_range)

        if not tasks:
            continue

        # Merge Python timing
        for task_id, task in tasks.items():
            if task_id in py_tasks:
                merge_task_timing(task, py_tasks[task_id])

        all_tasks.update(tasks)

        sorted_tasks = sorted(
            tasks.values(),
            key=lambda t: get_task_start_time(t) or datetime.max,
        )
        worker_timelines.append(WorkerTimeline(worker_id=worker_id, tasks=sorted_tasks))

    if not worker_timelines:
        print("No tasks found.", file=sys.stderr)
        sys.exit(1)

    worker_timelines.sort(
        key=lambda wt: get_task_start_time(wt.tasks[0]) if wt.tasks else datetime.max
    )

    total_tasks = sum(len(wt.tasks) for wt in worker_timelines)
    print(f"Found {total_tasks} tasks across {len(worker_timelines)} workers.")

    # Compute original statistics
    all_original_tasks = [task for wt in worker_timelines for task in wt.tasks]
    worker_task_sequences = {wt.worker_id: wt.tasks for wt in worker_timelines}
    original_stats = compute_statistics(all_original_tasks, worker_task_sequences)

    # Compute original job duration
    original_all_times = []
    for wt in worker_timelines:
        for task in wt.tasks:
            start = get_task_start_time(task)
            end = get_task_end_time(task)
            if start:
                original_all_times.append(start)
            if end:
                original_all_times.append(end)

    original_job_start = min(original_all_times) if original_all_times else datetime.now()
    original_job_end = max(original_all_times) if original_all_times else datetime.now()
    original_job_duration = (original_job_end - original_job_start).total_seconds()

    # Project timelines with C++ executor speedup
    projected_timelines: list[WorkerTimeline] = []
    for wt in worker_timelines:
        projected = project_worker_timeline(wt, args.startup_speedup, args.shutdown_speedup)
        projected_timelines.append(projected)

    # Compute projected statistics
    all_projected_tasks = [task for wt in projected_timelines for task in wt.tasks]
    projected_worker_sequences = {wt.worker_id: wt.tasks for wt in projected_timelines}
    projected_stats = compute_statistics(all_projected_tasks, projected_worker_sequences)

    # Compute projected job duration
    projected_all_times = []
    for wt in projected_timelines:
        for task in wt.tasks:
            start = get_task_start_time(task)
            end = get_task_end_time(task)
            if start:
                projected_all_times.append(start)
            if end:
                projected_all_times.append(end)

    projected_job_start = min(projected_all_times) if projected_all_times else datetime.now()
    projected_job_end = max(projected_all_times) if projected_all_times else datetime.now()
    projected_job_duration = (projected_job_end - projected_job_start).total_seconds()

    # Print comparison statistics
    print_comparison_statistics(
        original_stats, projected_stats,
        original_job_duration, projected_job_duration,
        args.startup_speedup, args.shutdown_speedup
    )

    if not args.stats_only:
        render_timeline(
            projected_timelines,
            title="Spider Search - Projected C++ Executor Timeline (v2)",
            job_info=None,
            output_path=args.output,
            figsize=figsize,
            subtitle=f"Speedup: Startup={args.startup_speedup}x, Shutdown={args.shutdown_speedup}x",
        )


if __name__ == "__main__":
    main()
