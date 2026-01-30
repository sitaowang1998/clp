#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#   "matplotlib",
# ]
# ///

"""
Spider Search Job Timeline Analyzer

Parses query scheduler and worker logs from a log directory to generate
timeline visualizations for Spider search jobs using matplotlib.
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
class JobTimeline:
    """Timeline data for a single Spider search job."""

    job_id: str
    # Scheduler events
    submit_time: datetime | None = None
    submit_end_time: datetime | None = None
    submit_duration: float | None = None
    dispatch_time: datetime | None = None
    complete_time: datetime | None = None
    complete_duration: float | None = None
    num_tasks: int = 0
    has_aggregation: bool = False
    # Task events: task_id -> (start_time, end_time, status, duration)
    task_starts: dict[int, datetime] = field(default_factory=dict)
    task_ends: dict[int, tuple[datetime, str, float]] = field(default_factory=dict)
    # Reducer events
    reducer_start: datetime | None = None
    reducer_end: datetime | None = None
    reducer_duration: float | None = None


# Log line timestamp pattern: "2024-01-15 10:30:00,123"
LOG_TIMESTAMP_PATTERN = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")

# Scheduler log patterns
SUBMIT_START_PATTERN = re.compile(
    r"Submitting Spider job (\S+) at (\S+) with (\d+) search tasks, aggregation=(\w+)"
)
SUBMIT_END_PATTERN = re.compile(
    r"Submitted Spider job (\S+) at (\S+), submission took ([\d.]+)s"
)
DISPATCH_PATTERN = re.compile(r"Dispatched search job (\S+) at (\S+) with (\d+) tasks\.")
COMPLETE_PATTERN = re.compile(
    r"Completed job (\S+) at (\S+)(?:.*?), total duration=([\d.]+)s\."
)

# Worker log patterns
TASK_START_PATTERN = re.compile(
    r"Started (\w+) task (\d+) for job (\S+) at (\S+)"
)
TASK_END_PATTERN = re.compile(
    r"Finished (\w+) task (\d+) for job (\S+) at (\S+) status=(\S+) duration=([\d.]+)s"
)
REDUCER_START_PATTERN = re.compile(r"Started reducer task for job (\S+) at (\S+)")
REDUCER_END_PATTERN = re.compile(
    r"Reducer completed for job (\S+) at (\S+) in ([\d.]+)s"
)


def parse_iso_timestamp(ts_str: str) -> datetime:
    """Parse ISO format timestamp."""
    # Handle various ISO formats
    ts_str = ts_str.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts_str)
    except ValueError:
        # Try without timezone
        if "+" in ts_str:
            ts_str = ts_str.split("+")[0]
        return datetime.fromisoformat(ts_str)


def parse_log_timestamp(line: str) -> datetime | None:
    """Extract timestamp from log line start."""
    match = LOG_TIMESTAMP_PATTERN.match(line)
    if match:
        ts_str = match.group(1).replace(",", ".")
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
    return None


def discover_log_files(log_dir: Path) -> tuple[Path | None, Path | None]:
    """
    Discover log files in the directory.
    Returns (scheduler_log, worker_log)

    The directory may contain:
    - query_scheduler.log: Scheduler events (dispatch, complete)
    - worker.log: Consolidated worker log with all task events
    - spider_worker_*.log: Individual worker process logs (ignored, just warnings)

    The logs may be directly in the directory or in a 'logs/' subdirectory.
    """
    # Try both the directory itself and a 'logs/' subdirectory
    search_dirs = [log_dir]
    if (log_dir / "logs").is_dir():
        search_dirs.insert(0, log_dir / "logs")

    scheduler_log = None
    worker_log = None

    for search_dir in search_dirs:
        if scheduler_log is None:
            candidate = search_dir / "query_scheduler.log"
            if candidate.exists():
                scheduler_log = candidate

        if worker_log is None:
            candidate = search_dir / "worker.log"
            if candidate.exists():
                worker_log = candidate

    return scheduler_log, worker_log


def parse_scheduler_log(
    path: Path, job_filter: str | None = None
) -> dict[str, JobTimeline]:
    """Parse scheduler log for job events."""
    timelines: dict[str, JobTimeline] = {}

    with open(path, "r") as f:
        for line in f:
            # Check submit start
            match = SUBMIT_START_PATTERN.search(line)
            if match:
                job_id, ts_str, num_tasks, agg = match.groups()
                if job_filter and job_id != job_filter:
                    continue
                if job_id not in timelines:
                    timelines[job_id] = JobTimeline(job_id=job_id)
                tl = timelines[job_id]
                tl.submit_time = parse_iso_timestamp(ts_str)
                tl.num_tasks = int(num_tasks)
                tl.has_aggregation = agg.lower() == "true"
                continue

            # Check submit end
            match = SUBMIT_END_PATTERN.search(line)
            if match:
                job_id, ts_str, duration = match.groups()
                if job_filter and job_id != job_filter:
                    continue
                if job_id not in timelines:
                    timelines[job_id] = JobTimeline(job_id=job_id)
                tl = timelines[job_id]
                tl.submit_end_time = parse_iso_timestamp(ts_str)
                tl.submit_duration = float(duration)
                continue

            # Check dispatch
            match = DISPATCH_PATTERN.search(line)
            if match:
                job_id, ts_str, num_tasks = match.groups()
                if job_filter and job_id != job_filter:
                    continue
                if job_id not in timelines:
                    timelines[job_id] = JobTimeline(job_id=job_id)
                tl = timelines[job_id]
                tl.dispatch_time = parse_iso_timestamp(ts_str)
                if tl.num_tasks == 0:
                    tl.num_tasks = int(num_tasks)
                continue

            # Check complete
            match = COMPLETE_PATTERN.search(line)
            if match:
                job_id, ts_str, duration = match.groups()
                if job_filter and job_id != job_filter:
                    continue
                if job_id not in timelines:
                    timelines[job_id] = JobTimeline(job_id=job_id)
                tl = timelines[job_id]
                tl.complete_time = parse_iso_timestamp(ts_str)
                tl.complete_duration = float(duration)
                continue

    return timelines


def parse_worker_log(
    path: Path, job_filter: str | None = None
) -> dict[str, JobTimeline]:
    """Parse consolidated worker log for task and reducer events."""
    timelines: dict[str, JobTimeline] = {}

    with open(path, "r") as f:
        for line in f:
            # Check task start
            match = TASK_START_PATTERN.search(line)
            if match:
                task_type, task_id, job_id, ts_str = match.groups()
                if job_filter and job_id != job_filter:
                    continue
                if job_id not in timelines:
                    timelines[job_id] = JobTimeline(job_id=job_id)
                tl = timelines[job_id]
                tl.task_starts[int(task_id)] = parse_iso_timestamp(ts_str)
                continue

            # Check task end
            match = TASK_END_PATTERN.search(line)
            if match:
                task_type, task_id, job_id, ts_str, status, duration = match.groups()
                if job_filter and job_id != job_filter:
                    continue
                if job_id not in timelines:
                    timelines[job_id] = JobTimeline(job_id=job_id)
                tl = timelines[job_id]
                tl.task_ends[int(task_id)] = (
                    parse_iso_timestamp(ts_str),
                    status,
                    float(duration),
                )
                continue

            # Check reducer start
            match = REDUCER_START_PATTERN.search(line)
            if match:
                job_id, ts_str = match.groups()
                if job_filter and job_id != job_filter:
                    continue
                if job_id not in timelines:
                    timelines[job_id] = JobTimeline(job_id=job_id)
                tl = timelines[job_id]
                tl.reducer_start = parse_iso_timestamp(ts_str)
                continue

            # Check reducer end
            match = REDUCER_END_PATTERN.search(line)
            if match:
                job_id, ts_str, duration = match.groups()
                if job_filter and job_id != job_filter:
                    continue
                if job_id not in timelines:
                    timelines[job_id] = JobTimeline(job_id=job_id)
                tl = timelines[job_id]
                tl.reducer_end = parse_iso_timestamp(ts_str)
                tl.reducer_duration = float(duration)
                continue

    return timelines


def merge_timelines(
    scheduler_tls: dict[str, JobTimeline],
    worker_tls: dict[str, JobTimeline],
) -> dict[str, JobTimeline]:
    """Merge scheduler and worker timelines."""
    all_job_ids = set(scheduler_tls.keys()) | set(worker_tls.keys())
    result: dict[str, JobTimeline] = {}

    for job_id in all_job_ids:
        tl = JobTimeline(job_id=job_id)

        # Copy scheduler data
        if job_id in scheduler_tls:
            stl = scheduler_tls[job_id]
            tl.submit_time = stl.submit_time
            tl.submit_end_time = stl.submit_end_time
            tl.submit_duration = stl.submit_duration
            tl.dispatch_time = stl.dispatch_time
            tl.complete_time = stl.complete_time
            tl.complete_duration = stl.complete_duration
            tl.num_tasks = stl.num_tasks
            tl.has_aggregation = stl.has_aggregation

        # Copy worker data
        if job_id in worker_tls:
            wtl = worker_tls[job_id]
            tl.task_starts = wtl.task_starts
            tl.task_ends = wtl.task_ends
            tl.reducer_start = wtl.reducer_start
            tl.reducer_end = wtl.reducer_end
            tl.reducer_duration = wtl.reducer_duration

        result[job_id] = tl

    return result


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


def get_timeline_bounds(timeline: JobTimeline) -> tuple[datetime | None, datetime | None]:
    """Get the earliest and latest timestamps in a timeline."""
    timestamps: list[datetime] = []

    if timeline.submit_time:
        timestamps.append(timeline.submit_time)
    if timeline.submit_end_time:
        timestamps.append(timeline.submit_end_time)
    if timeline.dispatch_time:
        timestamps.append(timeline.dispatch_time)
    if timeline.complete_time:
        timestamps.append(timeline.complete_time)
    for ts in timeline.task_starts.values():
        timestamps.append(ts)
    for end_ts, _, _ in timeline.task_ends.values():
        timestamps.append(end_ts)
    if timeline.reducer_start:
        timestamps.append(timeline.reducer_start)
    if timeline.reducer_end:
        timestamps.append(timeline.reducer_end)

    if not timestamps:
        return None, None

    return min(timestamps), max(timestamps)


def list_jobs(timelines: dict[str, JobTimeline]) -> str:
    """List all jobs found in the logs."""
    lines = ["Jobs found:", "=" * 60]

    for job_id in sorted(timelines.keys()):
        tl = timelines[job_id]
        task_info = f"{tl.num_tasks} tasks" if tl.num_tasks else "? tasks"
        agg_info = ", with aggregation" if tl.has_aggregation else ""
        duration_info = ""
        if tl.complete_duration:
            duration_info = f", duration={format_duration(tl.complete_duration)}"
        lines.append(f"  {job_id}: {task_info}{agg_info}{duration_info}")

    if not timelines:
        lines.append("  No jobs found.")

    return "\n".join(lines)


def render_timeline_matplotlib(
    timeline: JobTimeline,
    output_path: Path | None = None,
    figsize: tuple[int, int] = (14, 10),
) -> None:
    """Generate matplotlib timeline visualization for a job."""
    # Get timeline bounds
    tl_start, tl_end = get_timeline_bounds(timeline)
    if not tl_start or not tl_end:
        print("No timeline data available.")
        return

    total_seconds = (tl_end - tl_start).total_seconds()
    if total_seconds <= 0:
        total_seconds = 1.0

    # Collect all task data and sort by start time
    all_task_ids = list(set(timeline.task_starts.keys()) | set(timeline.task_ends.keys()))
    # Sort by start time (tasks without start time go last)
    all_task_ids.sort(key=lambda tid: timeline.task_starts.get(tid, tl_end))
    num_tasks = len(all_task_ids)

    # Create figure
    fig, ax = plt.subplots(figsize=figsize)

    # Draw task bars
    bar_height = 0.8
    task_patches = []

    for i, task_id in enumerate(all_task_ids):
        start = timeline.task_starts.get(task_id)
        end_data = timeline.task_ends.get(task_id)

        if start and end_data:
            end_ts, status, duration = end_data
            start_offset = (start - tl_start).total_seconds()
            end_offset = (end_ts - tl_start).total_seconds()

            rect = mpatches.Rectangle(
                (start_offset, i - bar_height / 2),
                end_offset - start_offset,
                bar_height,
                facecolor="steelblue",
                edgecolor="none",
                alpha=0.7,
            )
            task_patches.append(rect)
        elif start:
            # Task started but not finished (incomplete)
            start_offset = (start - tl_start).total_seconds()
            rect = mpatches.Rectangle(
                (start_offset, i - bar_height / 2),
                total_seconds - start_offset,
                bar_height,
                facecolor="orange",
                edgecolor="none",
                alpha=0.5,
            )
            task_patches.append(rect)

    # Add patches to plot
    collection = PatchCollection(task_patches, match_original=True)
    ax.add_collection(collection)

    # Draw vertical lines for scheduler events
    legend_handles = []

    if timeline.submit_time:
        offset = (timeline.submit_time - tl_start).total_seconds()
        ax.axvline(x=offset, color="blue", linestyle=":", linewidth=2)
        legend_handles.append(
            plt.Line2D([0], [0], color="blue", linestyle=":", linewidth=2, label="Submit Start")
        )

    if timeline.submit_end_time:
        offset = (timeline.submit_end_time - tl_start).total_seconds()
        ax.axvline(x=offset, color="blue", linestyle="-", linewidth=2)
        legend_handles.append(
            plt.Line2D([0], [0], color="blue", linestyle="-", linewidth=2, label="Submit End")
        )

    if timeline.complete_time:
        offset = (timeline.complete_time - tl_start).total_seconds()
        ax.axvline(x=offset, color="red", linestyle="-", linewidth=2)
        legend_handles.append(
            plt.Line2D([0], [0], color="red", linestyle="-", linewidth=2, label="Complete")
        )

    # Add reducer bar if present
    if timeline.reducer_start and timeline.reducer_end:
        start_offset = (timeline.reducer_start - tl_start).total_seconds()
        end_offset = (timeline.reducer_end - tl_start).total_seconds()
        reducer_y = num_tasks + 1
        rect = mpatches.Rectangle(
            (start_offset, reducer_y - bar_height / 2),
            end_offset - start_offset,
            bar_height,
            facecolor="purple",
            edgecolor="none",
            alpha=0.7,
        )
        ax.add_patch(rect)
        ax.text(
            end_offset + 0.5,
            reducer_y,
            f"Reducer ({format_duration(timeline.reducer_duration or (end_offset - start_offset))})",
            va="center",
            fontsize=8,
        )

    # Task bar legend
    legend_handles.append(
        mpatches.Patch(color="steelblue", alpha=0.7, label="Task")
    )

    # Set axis limits
    ax.set_xlim(-1, total_seconds + 1)
    ax.set_ylim(-1, num_tasks + (3 if timeline.reducer_start else 1))

    # Labels
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Task (sorted by start time)")

    # Title with stats
    durations = [d for _, _, d in timeline.task_ends.values()] if timeline.task_ends else []
    min_dur = min(durations) if durations else 0
    max_dur = max(durations) if durations else 0
    avg_dur = sum(durations) / len(durations) if durations else 0

    title = f"Job {timeline.job_id} Timeline"
    subtitle = (
        f"{timeline.num_tasks} tasks, "
        f"Duration: {format_duration(timeline.complete_duration or total_seconds)}"
    )
    stats = (
        f"Task Duration: min={format_duration(min_dur)}, "
        f"max={format_duration(max_dur)}, avg={format_duration(avg_dur)}"
    )

    ax.set_title(f"{title}\n{subtitle}\n{stats}", fontsize=12)

    # Legend
    ax.legend(handles=legend_handles, loc="upper right", fontsize=8)

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
        description="Analyze Spider search job timelines from logs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/logs/                  Analyze all jobs
  %(prog)s /path/to/logs/ --job-id 12345   Analyze specific job
  %(prog)s /path/to/logs/ --list-jobs      List all jobs found
  %(prog)s /path/to/logs/ -o timeline.png  Save to file
""",
    )
    parser.add_argument("log_dir", type=Path, help="Directory containing log files")
    parser.add_argument("--job-id", "-j", help="Filter to specific job ID")
    parser.add_argument("--list-jobs", "-l", action="store_true", help="List all jobs found")
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

    # Discover log files
    scheduler_log, worker_log = discover_log_files(args.log_dir)

    # Parse scheduler log
    scheduler_timelines: dict[str, JobTimeline] = {}
    if scheduler_log:
        scheduler_timelines = parse_scheduler_log(scheduler_log, args.job_id)

    # Parse worker log
    worker_timelines: dict[str, JobTimeline] = {}
    if worker_log:
        worker_timelines = parse_worker_log(worker_log, args.job_id)

    # Merge timelines
    timelines = merge_timelines(scheduler_timelines, worker_timelines)

    if not timelines:
        print("No jobs found in logs.", file=sys.stderr)
        sys.exit(1)

    # Generate output
    if args.list_jobs:
        print(list_jobs(timelines))
    else:
        job_ids = sorted(timelines.keys())
        if args.job_id:
            job_ids = [args.job_id] if args.job_id in timelines else []

        if not job_ids:
            print(f"Job {args.job_id} not found in logs.", file=sys.stderr)
            sys.exit(1)

        for job_id in job_ids:
            render_timeline_matplotlib(
                timelines[job_id],
                output_path=args.output,
                figsize=figsize,
            )


if __name__ == "__main__":
    main()
