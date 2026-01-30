#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#   "matplotlib",
# ]
# ///

"""
Spider Search Task Timeline Analyzer v2

Visualizes Spider search task timelines with tasks grouped by worker on the Y-axis.
Parses spider_worker_*.log files to extract [TIMING] entries for search tasks.
"""

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.collections import PatchCollection


@dataclass
class TaskTiming:
    """Timing data for a single search task."""

    task_id: str
    fetch_start: datetime | None = None
    fetch_end: datetime | None = None
    exec_start: datetime | None = None
    exec_end: datetime | None = None
    submit_start: datetime | None = None
    submit_end: datetime | None = None
    # Python-level task execution time (from worker.log)
    python_start: datetime | None = None
    python_end: datetime | None = None


@dataclass
class WorkerTimeline:
    """Timeline data for a single worker."""

    worker_id: str  # Short UUID (first 8 chars)
    tasks: list[TaskTiming] = field(default_factory=list)  # Sorted by start time


@dataclass
class JobInfo:
    """Job timing information from scheduler log."""

    job_id: str
    submit_time: datetime | None = None
    submit_end_time: datetime | None = None
    complete_time: datetime | None = None


# Log line timestamp pattern: "[2026-01-30 15:17:10.214]"
LOG_TIMESTAMP_PATTERN = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]")

# TIMING line pattern - captures phase name, start epoch, and end epoch
TIMING_PATTERN = re.compile(
    r"\[TIMING\]\s+task_id=(\S+)\s+func=(\S+)\s+"
    r"(fetch_input|execution|submit_output)_start=(\d+)\s+"
    r"\3_end=(\d+)\s+\3_duration_ms=\d+"
)

# Scheduler log patterns for job time bounds
SUBMIT_START_PATTERN = re.compile(
    r"Submitting Spider job (\S+) at (\S+) with (\d+) search tasks"
)
SUBMIT_END_PATTERN = re.compile(
    r"Submitted Spider job (\S+) at (\S+), submission took"
)
COMPLETE_PATTERN = re.compile(r"Completed job (\S+) at (\S+)")

# Python worker.log patterns for task timing
# Format: "[TASK_ID_MAP] spider_task_id=UUID search_task_id=123 job_id=1"
TASK_ID_MAP_PATTERN = re.compile(
    r"\[TASK_ID_MAP\]\s+spider_task_id=(\S+)\s+search_task_id=(\d+)\s+job_id=(\S+)"
)
# Format: "2026-01-30 15:17:11,114 spider_search [INFO] Started search_without_channel task 838 for job 1 at 2026-01-30T15:17:11.114809"
PYTHON_LOG_TIMESTAMP_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")
PYTHON_TASK_START_PATTERN = re.compile(
    r"Started (search_with(?:out)?_channel) task (\d+) for job (\S+) at (\S+)"
)
# Format: "Finished search_without_channel task 838 for job 1 at 2026-01-30T15:17:11.288805 status=2 duration=0.17s"
PYTHON_TASK_FINISH_PATTERN = re.compile(
    r"Finished (search_with(?:out)?_channel) task (\d+) for job (\S+) at (\S+)"
)


@dataclass
class PythonTaskTiming:
    """Python-level task timing from worker.log."""

    task_num: int  # Integer task ID
    job_id: str
    spider_task_id: str | None  # Spider UUID for matching
    start_time: datetime
    end_time: datetime | None = None


def parse_log_timestamp(line: str) -> datetime | None:
    """Extract timestamp from log line start."""
    match = LOG_TIMESTAMP_PATTERN.match(line)
    if match:
        ts_str = match.group(1)
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
    return None


def parse_iso_timestamp(ts_str: str) -> datetime:
    """Parse ISO format timestamp."""
    # Remove trailing comma or other punctuation
    ts_str = ts_str.rstrip(",.")
    ts_str = ts_str.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts_str)
    except ValueError:
        if "+" in ts_str:
            ts_str = ts_str.split("+")[0]
        return datetime.fromisoformat(ts_str)


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
    """
    Parse scheduler log to get job timing information.
    Returns mapping: job_id -> JobInfo
    """
    jobs: dict[str, JobInfo] = {}

    with open(path, "r") as f:
        for line in f:
            # Check submit start
            match = SUBMIT_START_PATTERN.search(line)
            if match:
                job_id, ts_str, _ = match.groups()
                if job_id not in jobs:
                    jobs[job_id] = JobInfo(job_id=job_id)
                jobs[job_id].submit_time = parse_iso_timestamp(ts_str)
                continue

            # Check submit end
            match = SUBMIT_END_PATTERN.search(line)
            if match:
                job_id, ts_str = match.groups()
                if job_id not in jobs:
                    jobs[job_id] = JobInfo(job_id=job_id)
                jobs[job_id].submit_end_time = parse_iso_timestamp(ts_str)
                continue

            # Check complete
            match = COMPLETE_PATTERN.search(line)
            if match:
                job_id, ts_str = match.groups()
                if job_id not in jobs:
                    jobs[job_id] = JobInfo(job_id=job_id)
                jobs[job_id].complete_time = parse_iso_timestamp(ts_str)
                continue

    return jobs


def parse_python_worker_log(path: Path, job_id: str | None = None) -> list[PythonTaskTiming]:
    """
    Parse worker.log for Python-level task timing.

    Args:
        path: Path to worker.log file
        job_id: Optional job ID to filter tasks

    Returns:
        List of PythonTaskTiming entries
    """
    tasks: dict[tuple[int, str], PythonTaskTiming] = {}  # (task_num, job_id) -> timing
    # Mapping from (task_num, job_id) -> spider_task_id (from TASK_ID_MAP entries)
    task_id_map: dict[tuple[int, str], str] = {}

    with open(path, "r") as f:
        for line in f:
            if "spider_search" not in line:
                continue

            # Check for TASK_ID_MAP (spider_task_id <-> search_task_id mapping)
            match = TASK_ID_MAP_PATTERN.search(line)
            if match:
                spider_task_id, task_num_str, task_job_id = match.groups()
                if job_id is not None and task_job_id != job_id:
                    continue
                task_num = int(task_num_str)
                key = (task_num, task_job_id)
                task_id_map[key] = spider_task_id
                continue

            # Check for task start
            match = PYTHON_TASK_START_PATTERN.search(line)
            if match:
                func_name, task_num_str, task_job_id, ts_str = match.groups()
                if job_id is not None and task_job_id != job_id:
                    continue
                task_num = int(task_num_str)
                start_time = parse_iso_timestamp(ts_str)
                key = (task_num, task_job_id)
                tasks[key] = PythonTaskTiming(
                    task_num=task_num,
                    job_id=task_job_id,
                    spider_task_id=None,
                    start_time=start_time,
                )
                continue

            # Check for task finish
            match = PYTHON_TASK_FINISH_PATTERN.search(line)
            if match:
                func_name, task_num_str, task_job_id, ts_str = match.groups()
                if job_id is not None and task_job_id != job_id:
                    continue
                task_num = int(task_num_str)
                key = (task_num, task_job_id)
                if key in tasks:
                    tasks[key].end_time = parse_iso_timestamp(ts_str)
                continue

    # Apply TASK_ID_MAP to link Python tasks to spider task IDs
    for key, spider_task_id in task_id_map.items():
        if key in tasks:
            tasks[key].spider_task_id = spider_task_id

    return list(tasks.values())


def match_python_timing_to_tasks(
    python_tasks: list[PythonTaskTiming],
    spider_tasks: dict[str, TaskTiming],
) -> int:
    """
    Match Python task timing to spider tasks.

    Updates spider_tasks in place with python_start/python_end times.
    Uses explicit TASK_ID_MAP when available (preferred), otherwise skips.

    Returns:
        Number of tasks matched
    """
    # Build lookup by spider_task_id for O(1) matching
    py_by_spider_id: dict[str, PythonTaskTiming] = {}
    for py_task in python_tasks:
        if py_task.spider_task_id and py_task.end_time:
            py_by_spider_id[py_task.spider_task_id] = py_task

    matched = 0
    for spider_task_id, spider_task in spider_tasks.items():
        # Match by explicit spider_task_id (from TASK_ID_MAP)
        if spider_task_id in py_by_spider_id:
            py_task = py_by_spider_id[spider_task_id]
            spider_task.python_start = py_task.start_time
            spider_task.python_end = py_task.end_time
            matched += 1

    return matched


def epoch_to_datetime(epoch_ms: int, ref_epoch_ms: int, ref_datetime: datetime) -> datetime:
    """
    Convert internal epoch timestamp (ms) to datetime using a reference point.

    The internal timestamps are milliseconds from a steady_clock epoch.
    We use the log line timestamp and its corresponding epoch value as a reference
    to calculate an offset, then apply it to convert any epoch value.

    Args:
        epoch_ms: The epoch timestamp to convert (in milliseconds)
        ref_epoch_ms: A reference epoch timestamp (in milliseconds)
        ref_datetime: The datetime corresponding to ref_epoch_ms

    Returns:
        The datetime corresponding to epoch_ms
    """
    from datetime import timedelta

    # Calculate delta from reference point
    delta_ms = epoch_ms - ref_epoch_ms
    return ref_datetime + timedelta(milliseconds=delta_ms)


def parse_worker_log(path: Path) -> tuple[str, dict[str, TaskTiming]]:
    """
    Parse a single worker log file for search task timings.
    Returns (worker_id, {task_id: TaskTiming})

    Uses internal epoch timestamps from TIMING entries for accurate phase timing.
    The log line timestamp serves as a reference to convert epoch values to datetime.
    """
    # Extract worker UUID from filename: spider_worker_<uuid>.log
    filename = path.name
    worker_uuid = filename.replace("spider_worker_", "").replace(".log", "")
    worker_id = worker_uuid[:8]  # Short form

    tasks: dict[str, TaskTiming] = {}

    # We need to establish a reference point to convert epoch timestamps to datetime.
    # Use the first TIMING entry we encounter as the reference.
    ref_epoch_ms: int | None = None
    ref_datetime: datetime | None = None

    with open(path, "r") as f:
        for line in f:
            # Only process TIMING lines for search tasks
            if "[TIMING]" not in line or "search" not in line:
                continue

            log_timestamp = parse_log_timestamp(line)
            if not log_timestamp:
                continue

            match = TIMING_PATTERN.search(line)
            if not match:
                continue

            task_id, func_name, phase, start_epoch_str, end_epoch_str = match.groups()
            start_epoch_ms = int(start_epoch_str)
            end_epoch_ms = int(end_epoch_str)

            # Only include search tasks
            if "search" not in func_name:
                continue

            # Establish reference point using the first valid TIMING entry
            # Use end_epoch as reference since log is written after phase completes
            if ref_epoch_ms is None:
                ref_epoch_ms = end_epoch_ms
                ref_datetime = log_timestamp

            # Convert epoch timestamps to datetime
            phase_start = epoch_to_datetime(start_epoch_ms, ref_epoch_ms, ref_datetime)
            phase_end = epoch_to_datetime(end_epoch_ms, ref_epoch_ms, ref_datetime)

            if task_id not in tasks:
                tasks[task_id] = TaskTiming(task_id=task_id)

            task = tasks[task_id]

            if phase == "fetch_input":
                task.fetch_start = phase_start
                task.fetch_end = phase_end
            elif phase == "execution":
                task.exec_start = phase_start
                task.exec_end = phase_end
            elif phase == "submit_output":
                task.submit_start = phase_start
                task.submit_end = phase_end

    return worker_id, tasks


def get_task_start_time(task: TaskTiming) -> datetime | None:
    """Get the earliest timestamp for a task (C++ phases only, not Python)."""
    times = [t for t in [task.fetch_start, task.exec_start, task.submit_start] if t]
    return min(times) if times else None


def get_task_end_time(task: TaskTiming) -> datetime | None:
    """Get the latest timestamp for a task (C++ phases only, not Python)."""
    times = [t for t in [task.fetch_end, task.exec_end, task.submit_end] if t]
    return max(times) if times else None


def get_task_full_start_time(task: TaskTiming) -> datetime | None:
    """Get the earliest timestamp for a task including Python timing."""
    times = [
        t
        for t in [task.fetch_start, task.exec_start, task.submit_start, task.python_start]
        if t
    ]
    return min(times) if times else None


def get_task_full_end_time(task: TaskTiming) -> datetime | None:
    """Get the latest timestamp for a task including Python timing."""
    times = [t for t in [task.fetch_end, task.exec_end, task.submit_end, task.python_end] if t]
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


def render_timeline_grouped_by_worker(
    worker_timelines: list[WorkerTimeline],
    job_info: JobInfo | None = None,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (14, 10),
) -> None:
    """
    Generate matplotlib timeline visualization with tasks grouped by worker.
    Y-axis: Workers with their tasks on adjacent rows
    X-axis: Time in seconds from earliest timestamp
    Vertical lines show scheduler events (submit start, submit end, complete).
    """
    if not worker_timelines:
        print("No timeline data available.")
        return

    # Find global time bounds (including Python timing)
    all_times: list[datetime] = []
    for wt in worker_timelines:
        for task in wt.tasks:
            start = get_task_full_start_time(task)
            end = get_task_full_end_time(task)
            if start:
                all_times.append(start)
            if end:
                all_times.append(end)

    if not all_times:
        print("No tasks found with timing data.")
        return

    # Use job submit time as reference (0) if available, otherwise earliest task time
    if job_info and job_info.submit_time:
        tl_start = job_info.submit_time
    else:
        tl_start = min(all_times)

    # Use job complete time or latest task time as end
    if job_info and job_info.complete_time:
        tl_end = max(max(all_times), job_info.complete_time)
    else:
        tl_end = max(all_times)

    total_seconds = (tl_end - tl_start).total_seconds()
    if total_seconds <= 0:
        total_seconds = 1.0

    # Create figure
    fig, ax = plt.subplots(figsize=figsize)

    # Colors for phases
    python_color = "#f39c12"  # Orange - Python execution wrapper
    fetch_color = "#2ecc71"  # Green
    exec_color = "#3498db"  # Blue
    submit_color = "#e74c3c"  # Red

    # Calculate total rows needed
    total_tasks = sum(len(wt.tasks) for wt in worker_timelines)

    # Draw task bars grouped by worker
    bar_height = 0.8
    current_row = 0
    worker_boundaries: list[tuple[int, int, str]] = []  # (start_row, end_row, worker_id)

    for wt in worker_timelines:
        if not wt.tasks:
            continue

        worker_start_row = current_row

        for task in wt.tasks:
            # Draw order: execution (bottom) -> python -> fetch/submit (top)
            # Note: Use actual widths without minimum to avoid false visual overlaps

            # 1. Draw C++ execution phase first (bottom layer)
            if task.exec_start and task.exec_end:
                start_offset = (task.exec_start - tl_start).total_seconds()
                end_offset = (task.exec_end - tl_start).total_seconds()
                width = end_offset - start_offset
                rect = mpatches.Rectangle(
                    (start_offset, current_row - bar_height / 2),
                    width,
                    bar_height,
                    facecolor=exec_color,
                    edgecolor="none",
                    alpha=0.8,
                )
                ax.add_patch(rect)

            # 2. Draw Python execution phase (shows overhead outside C++ execution)
            if task.python_start and task.python_end:
                start_offset = (task.python_start - tl_start).total_seconds()
                end_offset = (task.python_end - tl_start).total_seconds()
                width = end_offset - start_offset
                rect = mpatches.Rectangle(
                    (start_offset, current_row - bar_height / 2),
                    width,
                    bar_height,
                    facecolor=python_color,
                    edgecolor="none",
                    alpha=0.4,
                )
                ax.add_patch(rect)

            # 3. Draw fetch phase (top layer)
            if task.fetch_start and task.fetch_end:
                start_offset = (task.fetch_start - tl_start).total_seconds()
                end_offset = (task.fetch_end - tl_start).total_seconds()
                width = end_offset - start_offset
                rect = mpatches.Rectangle(
                    (start_offset, current_row - bar_height / 2),
                    width,
                    bar_height,
                    facecolor=fetch_color,
                    edgecolor="none",
                    alpha=0.8,
                )
                ax.add_patch(rect)

            # 4. Draw submit phase (top layer)
            if task.submit_start and task.submit_end:
                start_offset = (task.submit_start - tl_start).total_seconds()
                end_offset = (task.submit_end - tl_start).total_seconds()
                width = end_offset - start_offset
                rect = mpatches.Rectangle(
                    (start_offset, current_row - bar_height / 2),
                    width,
                    bar_height,
                    facecolor=submit_color,
                    edgecolor="none",
                    alpha=0.8,
                )
                ax.add_patch(rect)

            current_row += 1

        worker_boundaries.append((worker_start_row, current_row - 1, wt.worker_id))

    # Draw horizontal separator lines between workers
    for i, (start_row, end_row, worker_id) in enumerate(worker_boundaries):
        if i > 0:
            y = start_row - 0.5
            ax.axhline(y=y, color="gray", linestyle="-", linewidth=0.5, alpha=0.5)

    # Draw vertical lines for scheduler events
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

    # Set axis limits
    ax.set_xlim(-1, total_seconds + 1)
    ax.set_ylim(-1, current_row)

    # Create Y-axis labels at worker midpoints
    y_ticks = []
    y_labels = []
    for start_row, end_row, worker_id in worker_boundaries:
        mid_row = (start_row + end_row) / 2
        y_ticks.append(mid_row)
        y_labels.append(worker_id)

    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels, fontsize=8)

    # Labels
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Worker")

    # Legend
    legend_handles = [
        mpatches.Patch(color=python_color, alpha=0.4, label="Python Task"),
        mpatches.Patch(color=fetch_color, alpha=0.8, label="Fetch Input"),
        mpatches.Patch(color=exec_color, alpha=0.8, label="Execution"),
        mpatches.Patch(color=submit_color, alpha=0.8, label="Submit Output"),
    ]
    legend_handles.extend(vline_handles)
    ax.legend(handles=legend_handles, loc="upper right", fontsize=8)

    # Title with stats
    num_workers = len(worker_timelines)
    title = f"Spider Search Task Timeline (Grouped by Worker)"
    subtitle = f"{num_workers} workers, {total_tasks} tasks, Duration: {format_duration(total_seconds)}"
    ax.set_title(f"{title}\n{subtitle}", fontsize=12)

    # Grid
    ax.grid(axis="x", alpha=0.3)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Timeline saved to {output_path}")
    else:
        plt.show()


def main():
    parser = argparse.ArgumentParser(
        description="Visualize Spider search task timelines grouped by worker.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/logs/                  Visualize all search tasks
  %(prog)s /path/to/logs/ --job-id 1       Filter to specific job
  %(prog)s /path/to/logs/ -o timeline.png  Save to file
""",
    )
    parser.add_argument("log_dir", type=Path, help="Directory containing spider_worker_*.log files")
    parser.add_argument("--job-id", "-j", help="Filter to specific job ID (uses scheduler log timestamps)")
    parser.add_argument("--output", "-o", type=Path, help="Write output to file (PNG, PDF, etc.)")
    parser.add_argument(
        "--figsize",
        type=str,
        default="14,10",
        help="Figure size as width,height in inches (default: 14,10)",
    )

    args = parser.parse_args()

    if not args.log_dir.is_dir():
        print(f"Error: {args.log_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    # Parse figsize
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
    job_info: JobInfo | None = None
    job_time_range: tuple[datetime, datetime] | None = None

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
                print(f"Warning: Job {args.job_id} not found in scheduler log. Showing all tasks.")
    elif args.job_id:
        print("Warning: No scheduler log found. Showing all tasks.")

    # Parse Python worker.log for task timing
    python_tasks: list[PythonTaskTiming] = []
    worker_log = args.log_dir / "worker.log"
    if not worker_log.exists():
        logs_subdir = args.log_dir / "logs" / "worker.log"
        if logs_subdir.exists():
            worker_log = logs_subdir

    if worker_log.exists():
        python_tasks = parse_python_worker_log(worker_log, job_id=args.job_id)
        tasks_with_mapping = sum(1 for t in python_tasks if t.spider_task_id is not None)
        print(f"Found {len(python_tasks)} Python task entries in worker.log.")
        if tasks_with_mapping > 0:
            print(f"  {tasks_with_mapping} tasks have TASK_ID_MAP (can be matched to spider tasks).")
        else:
            print("  No TASK_ID_MAP entries found. Python timing cannot be matched to spider tasks.")
    else:
        print("Warning: No worker.log found. Python task timing will not be shown.")

    # Parse all worker logs
    worker_timelines: list[WorkerTimeline] = []
    total_python_matches = 0

    for log_path in worker_logs:
        worker_id, tasks = parse_worker_log(log_path)

        # Filter by job time range if specified
        if job_time_range:
            tasks = filter_tasks_by_time_range(tasks, *job_time_range)

        if not tasks:
            continue

        # Match Python timing to spider tasks (using TASK_ID_MAP)
        if python_tasks:
            total_python_matches += match_python_timing_to_tasks(python_tasks, tasks)

        # Sort tasks by start time
        sorted_tasks = sorted(
            tasks.values(),
            key=lambda t: get_task_start_time(t) or datetime.max,
        )

        worker_timelines.append(WorkerTimeline(worker_id=worker_id, tasks=sorted_tasks))

    if not worker_timelines:
        print("No search tasks found.", file=sys.stderr)
        sys.exit(1)

    # Sort workers by their first task's start time
    worker_timelines.sort(
        key=lambda wt: get_task_start_time(wt.tasks[0]) if wt.tasks else datetime.max
    )

    total_tasks = sum(len(wt.tasks) for wt in worker_timelines)
    tasks_with_python = sum(
        1 for wt in worker_timelines for t in wt.tasks if t.python_start is not None
    )
    print(f"Found {total_tasks} search tasks across {len(worker_timelines)} workers.")
    if tasks_with_python > 0:
        print(f"Matched {tasks_with_python} tasks with Python timing.")

    # Render timeline
    render_timeline_grouped_by_worker(
        worker_timelines,
        job_info=job_info,
        output_path=args.output,
        figsize=figsize,
    )


if __name__ == "__main__":
    main()
