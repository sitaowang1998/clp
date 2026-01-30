#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#   "matplotlib",
# ]
# ///

"""
Spider Search Task Timeline Analyzer v3

Visualizes detailed timing breakdown of Spider search task execution,
showing where time is spent in the C++ worker and Python task executor.

Data Sources:
- C++ Worker (spider_worker_*.log): fetch_input, spawn, input_send, execution, submit_output
- Python Task Executor (worker.log): decode, config_load, cmd_build, search
"""

import argparse
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
    # C++ phases
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
    # Python phases (from worker.log)
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
    """Job timing information from scheduler log."""

    job_id: str
    submit_time: Optional[datetime] = None
    submit_end_time: Optional[datetime] = None
    complete_time: Optional[datetime] = None


# Log line timestamp pattern: "[2026-01-30 15:17:10.214]"
LOG_TIMESTAMP_PATTERN = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]")

# C++ TIMING patterns - 5 phases
CPP_TIMING_PATTERNS = {
    "fetch_input": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"fetch_input_start=(\d+)\s+fetch_input_end=(\d+)\s+fetch_input_duration_ms=\d+"
    ),
    "spawn": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"spawn_start=(\d+)\s+spawn_end=(\d+)\s+spawn_duration_ms=\d+"
    ),
    "input_send": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"input_send_start=(\d+)\s+input_send_end=(\d+)\s+input_send_duration_ms=\d+"
    ),
    "execution": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"execution_start=(\d+)\s+execution_end=(\d+)\s+execution_duration_ms=\d+"
    ),
    "submit_output": re.compile(
        r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
        r"submit_output_start=(\d+)\s+submit_output_end=(\d+)\s+submit_output_duration_ms=\d+"
    ),
}

# Python TIMING patterns
PY_TIMING_DECODE = re.compile(
    r"\[TIMING\]\s+spider_task_id=(\S+)\s+"
    r"func_entry=(\d+)\s+decode_end=(\d+)\s+decode_duration_ms=\d+"
)
PY_TIMING_CONFIG_LOAD = re.compile(
    r"\[TIMING\]\s+spider_task_id=(\S+)\s+"
    r"config_load_start=(\d+)\s+config_load_end=(\d+)\s+config_load_duration_ms=\d+"
)
PY_TIMING_CMD_BUILD = re.compile(
    r"\[TIMING\]\s+spider_task_id=(\S+)\s+"
    r"cmd_build_start=(\d+)\s+cmd_build_end=(\d+)\s+cmd_build_duration_ms=\d+"
)
PY_TIMING_SEARCH = re.compile(
    r"\[TIMING\]\s+spider_task_id=(\S+)\s+"
    r"search_start=(\d+)\s+search_end=(\d+)\s+search_duration_ms=\d+"
)
PY_TIMING_TOTAL = re.compile(
    r"\[TIMING\]\s+spider_task_id=(\S+)\s+"
    r"func_entry=(\d+)\s+func_exit=(\d+)\s+total_func_duration_ms=\d+"
)

# Scheduler log patterns
SUBMIT_START_PATTERN = re.compile(
    r"Submitting Spider job (\S+) at (\S+) with (\d+) search tasks"
)
SUBMIT_END_PATTERN = re.compile(r"Submitted Spider job (\S+) at (\S+), submission took")
COMPLETE_PATTERN = re.compile(r"Completed job (\S+) at (\S+)")


def parse_log_timestamp(line: str) -> Optional[datetime]:
    """Extract timestamp from log line start."""
    match = LOG_TIMESTAMP_PATTERN.match(line)
    if match:
        ts_str = match.group(1)
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
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
    """Convert internal epoch timestamp (ms) to datetime using a reference point."""
    delta_ms = epoch_ms - ref_epoch_ms
    return ref_datetime + timedelta(milliseconds=delta_ms)


def discover_worker_logs(log_dir: Path) -> list[Path]:
    """Find all spider_worker_*.log files in the directory."""
    search_dirs = [log_dir]
    if (log_dir / "logs").is_dir():
        search_dirs.insert(0, log_dir / "logs")

    worker_logs = []
    for search_dir in search_dirs:
        worker_logs.extend(search_dir.glob("spider_worker_*.log"))

    return sorted(worker_logs)


def parse_scheduler_log(path: Path) -> dict[str, JobInfo]:
    """Parse scheduler log to get job timing information."""
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
    """
    Parse a single C++ worker log file for detailed search task timings.
    Returns (worker_id, {task_id: TaskTiming})
    """
    filename = path.name
    worker_uuid = filename.replace("spider_worker_", "").replace(".log", "")
    worker_id = worker_uuid[:8]

    tasks: dict[str, TaskTiming] = {}
    ref_epoch_ms: Optional[int] = None
    ref_datetime: Optional[datetime] = None

    with open(path, "r") as f:
        for line in f:
            if "[TIMING]" not in line or "search" not in line:
                continue

            log_timestamp = parse_log_timestamp(line)
            if not log_timestamp:
                continue

            # Try each C++ timing pattern
            for phase_name, pattern in CPP_TIMING_PATTERNS.items():
                match = pattern.search(line)
                if not match:
                    continue

                task_id, func_name, start_epoch_str, end_epoch_str = match.groups()
                if "search" not in func_name:
                    continue

                start_epoch_ms = int(start_epoch_str)
                end_epoch_ms = int(end_epoch_str)

                # Establish reference point
                if ref_epoch_ms is None:
                    ref_epoch_ms = end_epoch_ms
                    ref_datetime = log_timestamp

                phase_start = epoch_to_datetime(start_epoch_ms, ref_epoch_ms, ref_datetime)
                phase_end = epoch_to_datetime(end_epoch_ms, ref_epoch_ms, ref_datetime)

                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id)

                task = tasks[task_id]

                if phase_name == "fetch_input":
                    task.fetch_start = phase_start
                    task.fetch_end = phase_end
                elif phase_name == "spawn":
                    task.spawn_start = phase_start
                    task.spawn_end = phase_end
                elif phase_name == "input_send":
                    task.input_send_start = phase_start
                    task.input_send_end = phase_end
                elif phase_name == "execution":
                    task.execution_start = phase_start
                    task.execution_end = phase_end
                elif phase_name == "submit_output":
                    task.submit_start = phase_start
                    task.submit_end = phase_end

                break  # Found a match, move to next line

    return worker_id, tasks


def parse_python_worker_log(
    path: Path, ref_epoch_ms: Optional[int] = None, ref_datetime: Optional[datetime] = None
) -> tuple[dict[str, TaskTiming], Optional[int], Optional[datetime]]:
    """
    Parse worker.log for Python-level task timing.
    Returns (task_timings, ref_epoch_ms, ref_datetime)
    """
    tasks: dict[str, TaskTiming] = {}

    with open(path, "r") as f:
        for line in f:
            if "[TIMING]" not in line or "spider_search" not in line:
                continue

            log_timestamp_match = re.match(
                r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})", line
            )
            if not log_timestamp_match:
                continue
            log_timestamp = datetime.strptime(
                log_timestamp_match.group(1), "%Y-%m-%d %H:%M:%S,%f"
            )

            # Try decode pattern (includes func_entry)
            match = PY_TIMING_DECODE.search(line)
            if match:
                task_id, func_entry_str, decode_end_str = match.groups()
                func_entry = int(func_entry_str)
                decode_end = int(decode_end_str)

                if ref_epoch_ms is None:
                    ref_epoch_ms = decode_end
                    ref_datetime = log_timestamp

                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id)

                tasks[task_id].py_func_entry = epoch_to_datetime(
                    func_entry, ref_epoch_ms, ref_datetime
                )
                tasks[task_id].py_decode_end = epoch_to_datetime(
                    decode_end, ref_epoch_ms, ref_datetime
                )
                continue

            # Try config_load pattern
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

            # Try cmd_build pattern
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

            # Try search pattern
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

            # Try total (func_entry -> func_exit) pattern
            match = PY_TIMING_TOTAL.search(line)
            if match:
                task_id, entry_str, exit_str = match.groups()
                if ref_epoch_ms is None:
                    ref_epoch_ms = int(exit_str)
                    ref_datetime = log_timestamp
                if task_id not in tasks:
                    tasks[task_id] = TaskTiming(task_id=task_id)
                # Only set func_exit here; func_entry comes from decode line
                tasks[task_id].py_func_exit = epoch_to_datetime(
                    int(exit_str), ref_epoch_ms, ref_datetime
                )
                continue

    return tasks, ref_epoch_ms, ref_datetime


def merge_python_timing(
    cpp_tasks: dict[str, TaskTiming], py_tasks: dict[str, TaskTiming]
) -> int:
    """
    Merge Python timing into C++ task records.
    Returns count of merged tasks.
    """
    merged = 0
    for task_id, py_task in py_tasks.items():
        if task_id in cpp_tasks:
            cpp_task = cpp_tasks[task_id]
            cpp_task.py_func_entry = py_task.py_func_entry
            cpp_task.py_decode_end = py_task.py_decode_end
            cpp_task.py_config_load_start = py_task.py_config_load_start
            cpp_task.py_config_load_end = py_task.py_config_load_end
            cpp_task.py_cmd_build_start = py_task.py_cmd_build_start
            cpp_task.py_cmd_build_end = py_task.py_cmd_build_end
            cpp_task.py_search_start = py_task.py_search_start
            cpp_task.py_search_end = py_task.py_search_end
            cpp_task.py_func_exit = py_task.py_func_exit
            merged += 1
    return merged


def get_task_start_time(task: TaskTiming) -> Optional[datetime]:
    """Get the earliest timestamp for a task."""
    times = [t for t in [task.fetch_start, task.spawn_start, task.execution_start] if t]
    return min(times) if times else None


def get_task_end_time(task: TaskTiming) -> Optional[datetime]:
    """Get the latest timestamp for a task."""
    times = [t for t in [task.fetch_end, task.submit_end, task.execution_end] if t]
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
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs:.1f}s"


def compute_gap_analysis(all_tasks: list[TaskTiming]) -> dict[str, dict]:
    """
    Compute gap analysis statistics for all tasks.
    Returns dict with statistics for each timing gap/phase.
    """
    stats: dict[str, list[float]] = {
        "fetch_duration": [],
        "spawn_duration": [],
        "input_send_duration": [],
        "cpp_exec_duration": [],
        "submit_duration": [],
        "py_decode_duration": [],
        "py_config_load_duration": [],
        "py_cmd_build_duration": [],
        "py_search_duration": [],
        "py_startup_gap": [],  # input_send_end -> py_func_entry
        "py_shutdown_gap": [],  # py_func_exit -> cpp_exec_end
        "py_total_duration": [],
    }

    for task in all_tasks:
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

        # Gap analysis
        # Startup gap: from spawn_end to py_func_entry (Python interpreter startup)
        if task.spawn_end and task.py_func_entry:
            gap = (task.py_func_entry - task.spawn_end).total_seconds() * 1000
            if gap >= 0:  # Only count valid gaps
                stats["py_startup_gap"].append(gap)
        if task.py_func_exit and task.execution_end:
            gap = (task.execution_end - task.py_func_exit).total_seconds() * 1000
            if gap >= 0:
                stats["py_shutdown_gap"].append(gap)

    # Compute summary statistics
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


def print_gap_analysis(stats: dict[str, dict]) -> None:
    """Print gap analysis statistics to console."""
    print("\n" + "=" * 70)
    print("GAP ANALYSIS - Timing Breakdown (all values in milliseconds)")
    print("=" * 70)

    sections = [
        ("C++ Worker Phases", [
            ("Fetch Input", "fetch_duration"),
            ("Spawn Process", "spawn_duration"),
            ("Input Send", "input_send_duration"),
            ("Execution (total)", "cpp_exec_duration"),
            ("Submit Output", "submit_duration"),
        ]),
        ("Python Task Executor Phases", [
            ("Decode", "py_decode_duration"),
            ("Config Load", "py_config_load_duration"),
            ("Command Build", "py_cmd_build_duration"),
            ("Search", "py_search_duration"),
            ("Total Python", "py_total_duration"),
        ]),
        ("Gap Analysis", [
            ("Startup (spawn_end -> py_entry)", "py_startup_gap"),
            ("Shutdown (py_exit -> exec_end)", "py_shutdown_gap"),
        ]),
    ]

    for section_name, items in sections:
        print(f"\n{section_name}:")
        print("-" * 60)
        print(f"{'Phase':<30} {'Mean':>10} {'Min':>10} {'Max':>10} {'Count':>8}")
        print("-" * 60)
        for label, key in items:
            s = stats[key]
            if s["count"] > 0:
                print(
                    f"{label:<30} {s['mean']:>10.2f} {s['min']:>10.2f} "
                    f"{s['max']:>10.2f} {s['count']:>8}"
                )
            else:
                print(f"{label:<30} {'N/A':>10} {'N/A':>10} {'N/A':>10} {0:>8}")


def render_detailed_timeline(
    worker_timelines: list[WorkerTimeline],
    job_info: Optional[JobInfo] = None,
    output_path: Optional[Path] = None,
    figsize: tuple[int, int] = (16, 12),
) -> None:
    """
    Render detailed timeline showing all phases per task.
    """
    if not worker_timelines:
        print("No timeline data available.")
        return

    # Find global time bounds
    all_times: list[datetime] = []
    for wt in worker_timelines:
        for task in wt.tasks:
            for t in [task.fetch_start, task.fetch_end, task.spawn_start, task.spawn_end,
                      task.input_send_start, task.input_send_end, task.execution_start,
                      task.execution_end, task.submit_start, task.submit_end,
                      task.py_func_entry, task.py_func_exit]:
                if t:
                    all_times.append(t)

    if not all_times:
        print("No tasks found with timing data.")
        return

    if job_info and job_info.submit_time:
        tl_start = job_info.submit_time
    else:
        tl_start = min(all_times)

    if job_info and job_info.complete_time:
        tl_end = max(max(all_times), job_info.complete_time)
    else:
        tl_end = max(all_times)

    total_seconds = (tl_end - tl_start).total_seconds()
    if total_seconds <= 0:
        total_seconds = 1.0

    fig, ax = plt.subplots(figsize=figsize)

    # Color scheme
    colors = {
        "fetch": "#2ecc71",       # Green
        "spawn": "#9b59b6",       # Purple
        "input_send": "#8e44ad",  # Dark purple
        "execution": "#3498db",   # Blue
        "submit": "#e74c3c",      # Red
        # Python phases (shown within execution) - distinct colors
        "py_startup": "#ff6b6b",  # Coral red - MAJOR BOTTLENECK
        "py_decode": "#4ecdc4",   # Teal/cyan
        "py_config": "#a855f7",   # Violet
        "py_cmd": "#6b7280",      # Gray
        "py_search": "#22c55e",   # Bright green
        "py_shutdown": "#fbbf24", # Amber/yellow - shutdown gap
    }

    total_tasks = sum(len(wt.tasks) for wt in worker_timelines)
    bar_height = 0.35  # Smaller bars for stacked layout
    row_spacing = 0.1  # Gap between C++ and Python rows
    task_spacing = 0.3  # Gap between tasks
    current_row = 0
    worker_boundaries: list[tuple[float, float, str]] = []

    for wt in worker_timelines:
        if not wt.tasks:
            continue

        worker_start_row = current_row

        for task in wt.tasks:
            # Bottom row: C++ phases
            cpp_row = current_row

            # 1. Fetch
            if task.fetch_start and task.fetch_end:
                start_offset = (task.fetch_start - tl_start).total_seconds()
                width = (task.fetch_end - task.fetch_start).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, cpp_row - bar_height / 2),
                    width, bar_height,
                    facecolor=colors["fetch"], edgecolor="none", alpha=0.9
                )
                ax.add_patch(rect)

            # 2. Spawn
            if task.spawn_start and task.spawn_end:
                start_offset = (task.spawn_start - tl_start).total_seconds()
                width = (task.spawn_end - task.spawn_start).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, cpp_row - bar_height / 2),
                    width, bar_height,
                    facecolor=colors["spawn"], edgecolor="none", alpha=0.9
                )
                ax.add_patch(rect)

            # 3. Input Send
            if task.input_send_start and task.input_send_end:
                start_offset = (task.input_send_start - tl_start).total_seconds()
                width = (task.input_send_end - task.input_send_start).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, cpp_row - bar_height / 2),
                    width, bar_height,
                    facecolor=colors["input_send"], edgecolor="none", alpha=0.9
                )
                ax.add_patch(rect)

            # 4. Execution
            if task.execution_start and task.execution_end:
                start_offset = (task.execution_start - tl_start).total_seconds()
                width = (task.execution_end - task.execution_start).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, cpp_row - bar_height / 2),
                    width, bar_height,
                    facecolor=colors["execution"], edgecolor="none", alpha=0.9
                )
                ax.add_patch(rect)

            # 5. Submit
            if task.submit_start and task.submit_end:
                start_offset = (task.submit_start - tl_start).total_seconds()
                width = (task.submit_end - task.submit_start).total_seconds()
                rect = mpatches.Rectangle(
                    (start_offset, cpp_row - bar_height / 2),
                    width, bar_height,
                    facecolor=colors["submit"], edgecolor="none", alpha=0.9
                )
                ax.add_patch(rect)

            # Top row: Python phases
            py_row = current_row + bar_height + row_spacing

            # Python startup gap (from spawn_end to py_func_entry)
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

            # Python shutdown gap (from py_func_exit to execution_end)
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

            # Move to next task (2 rows per task + spacing)
            current_row += 2 * bar_height + row_spacing + task_spacing

        worker_boundaries.append((worker_start_row, current_row - task_spacing, wt.worker_id))

    # Draw worker separators
    for i, (start_row, end_row, worker_id) in enumerate(worker_boundaries):
        if i > 0:
            y = start_row - 0.5
            ax.axhline(y=y, color="gray", linestyle="-", linewidth=0.5, alpha=0.5)

    # Draw scheduler event lines
    vline_handles = []
    if job_info:
        if job_info.submit_time:
            offset = (job_info.submit_time - tl_start).total_seconds()
            ax.axvline(x=offset, color="blue", linestyle=":", linewidth=2)
            vline_handles.append(
                plt.Line2D([0], [0], color="blue", linestyle=":", linewidth=2, label="Submit Start")
            )
        if job_info.submit_end_time:
            offset = (job_info.submit_end_time - tl_start).total_seconds()
            ax.axvline(x=offset, color="blue", linestyle="-", linewidth=2)
            vline_handles.append(
                plt.Line2D([0], [0], color="blue", linestyle="-", linewidth=2, label="Submit End")
            )
        if job_info.complete_time:
            offset = (job_info.complete_time - tl_start).total_seconds()
            ax.axvline(x=offset, color="red", linestyle="-", linewidth=2)
            vline_handles.append(
                plt.Line2D([0], [0], color="red", linestyle="-", linewidth=2, label="Complete")
            )

    ax.set_xlim(-0.5, total_seconds + 0.5)
    ax.set_ylim(-1, current_row)

    # Y-axis labels
    y_ticks = []
    y_labels = []
    for start_row, end_row, worker_id in worker_boundaries:
        mid_row = (start_row + end_row) / 2
        y_ticks.append(mid_row)
        y_labels.append(worker_id)

    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels, fontsize=8)

    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Worker")

    # Legend - two columns for better fit
    legend_handles = [
        mpatches.Patch(color=colors["fetch"], alpha=0.9, label="Fetch Input"),
        mpatches.Patch(color=colors["spawn"], alpha=0.9, label="Spawn+Send"),
        mpatches.Patch(color=colors["execution"], alpha=0.6, label="Execution"),
        mpatches.Patch(color=colors["submit"], alpha=0.9, label="Submit Output"),
        mpatches.Patch(color=colors["py_startup"], alpha=0.9, label="Py Startup"),
        mpatches.Patch(color=colors["py_decode"], alpha=0.9, label="Py Decode"),
        mpatches.Patch(color=colors["py_config"], alpha=0.9, label="Py Config"),
        mpatches.Patch(color=colors["py_cmd"], alpha=0.9, label="Py Cmd Build"),
        mpatches.Patch(color=colors["py_search"], alpha=0.9, label="Py Search"),
        mpatches.Patch(color=colors["py_shutdown"], alpha=0.9, label="Py Shutdown"),
    ]
    legend_handles.extend(vline_handles)
    ax.legend(handles=legend_handles, loc="upper right", fontsize=7, ncol=2)

    num_workers = len(worker_timelines)
    title = "Spider Search Task Detailed Timeline"
    subtitle = f"{num_workers} workers, {total_tasks} tasks, Duration: {format_duration(total_seconds)}"
    ax.set_title(f"{title}\n{subtitle}", fontsize=12)

    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Timeline saved to {output_path}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Visualize detailed Spider search task timing breakdown.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/logs/                  Visualize all search tasks
  %(prog)s /path/to/logs/ --job-id 3       Filter to specific job
  %(prog)s /path/to/logs/ -o timeline.png  Save to file
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
        "--stats-only",
        action="store_true",
        help="Only print gap analysis statistics, no visualization",
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

    # Get job info from scheduler log
    job_info: Optional[JobInfo] = None
    job_time_range: Optional[tuple[datetime, datetime]] = None

    scheduler_log = args.log_dir / "query_scheduler.log"
    if not scheduler_log.exists():
        logs_subdir = args.log_dir / "logs" / "query_scheduler.log"
        if logs_subdir.exists():
            scheduler_log = logs_subdir

    if scheduler_log.exists():
        all_jobs = parse_scheduler_log(scheduler_log)
        if args.job_id:
            if args.job_id in all_jobs:
                job_info = all_jobs[args.job_id]
                if job_info.submit_time and job_info.complete_time:
                    job_time_range = (job_info.submit_time, job_info.complete_time)
                    print(f"Filtering to job {args.job_id}: {job_time_range[0]} to {job_time_range[1]}")
            else:
                print(f"Warning: Job {args.job_id} not found in scheduler log.")

    # Parse Python worker.log
    py_tasks: dict[str, TaskTiming] = {}
    worker_log = args.log_dir / "worker.log"
    if not worker_log.exists():
        logs_subdir = args.log_dir / "logs" / "worker.log"
        if logs_subdir.exists():
            worker_log = logs_subdir

    if worker_log.exists():
        py_tasks, _, _ = parse_python_worker_log(worker_log)
        print(f"Found {len(py_tasks)} Python task timing entries in worker.log.")
    else:
        print("Warning: No worker.log found. Python timing will not be shown.")

    # Parse all C++ worker logs
    worker_timelines: list[WorkerTimeline] = []
    all_cpp_tasks: dict[str, TaskTiming] = {}

    for log_path in worker_logs:
        worker_id, tasks = parse_worker_log(log_path)

        if job_time_range:
            tasks = filter_tasks_by_time_range(tasks, *job_time_range)

        if not tasks:
            continue

        all_cpp_tasks.update(tasks)

        sorted_tasks = sorted(
            tasks.values(),
            key=lambda t: get_task_start_time(t) or datetime.max,
        )

        worker_timelines.append(WorkerTimeline(worker_id=worker_id, tasks=sorted_tasks))

    if not worker_timelines:
        print("No search tasks found.", file=sys.stderr)
        sys.exit(1)

    # Merge Python timing
    merged_count = merge_python_timing(all_cpp_tasks, py_tasks)
    print(f"Merged Python timing for {merged_count} tasks.")

    # Sort workers by first task start time
    worker_timelines.sort(
        key=lambda wt: get_task_start_time(wt.tasks[0]) if wt.tasks else datetime.max
    )

    total_tasks = sum(len(wt.tasks) for wt in worker_timelines)
    print(f"Found {total_tasks} search tasks across {len(worker_timelines)} workers.")

    # Collect all tasks for statistics
    all_tasks = [task for wt in worker_timelines for task in wt.tasks]

    # Compute and print gap analysis
    stats = compute_gap_analysis(all_tasks)
    print_gap_analysis(stats)

    if not args.stats_only:
        render_detailed_timeline(
            worker_timelines,
            job_info=job_info,
            output_path=args.output,
            figsize=figsize,
        )


if __name__ == "__main__":
    main()
