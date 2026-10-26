#!/usr/bin/env python3
"""Audit completed benchmark jobs and calculate reproducible summary statistics."""

import argparse
import json
import statistics
from typing import Any

import run as benchmark


def audit(record: dict[str, Any]) -> dict[str, Any]:
    """Check both SQL byte totals and completion counts against published archive metadata."""
    dataset = benchmark.dataset_name(record["label"])
    with benchmark.connect() as connection, connection.cursor(dictionary=True) as cursor:
        cursor.execute(
            "SELECT status,num_tasks,num_tasks_completed,uncompressed_size,compressed_size "
            "FROM compression_jobs WHERE id=%s",
            (record["job_id"],),
        )
        job = cursor.fetchone()
        cursor.execute(
            "SELECT COUNT(*) AS archives, SUM(uncompressed_size) AS raw_size, "  # noqa: S608
            f"SUM(size) AS stored_size FROM clp_{dataset}_archives"
        )
        archives = cursor.fetchone()
    valid = (
        job["status"] == benchmark.JOB_SUCCEEDED
        and job["num_tasks"] == job["num_tasks_completed"] == record["num_tasks"]
        and job["uncompressed_size"] == archives["raw_size"] == record["input_bytes"]
        and job["compressed_size"] == archives["stored_size"] == record["compressed_bytes"]
        and archives["archives"] == record["archives"]
    )
    if not valid:
        message = f"job {record['job_id']} counters disagree with archive metadata"
        raise RuntimeError(message)
    return {"job_id": record["job_id"], "passed": True, "job": job, "archives": archives}


def summarize(records: list[dict[str, Any]], backend: str) -> dict[str, Any]:
    """Summarize measured trials without including warm-ups or validation scenarios."""
    selected = [
        record for record in records if record["backend"] == backend and not record["warmup"]
    ]
    elapsed = [record["elapsed_seconds"] for record in selected]
    if not elapsed:
        message = f"no measured trials for {backend}"
        raise ValueError(message)
    return {
        "trials": len(elapsed),
        "median_seconds": statistics.median(elapsed),
        "mean_seconds": statistics.mean(elapsed),
        "stdev_seconds": statistics.stdev(elapsed),
        "min_seconds": min(elapsed),
        "max_seconds": max(elapsed),
        "median_mib_per_second": statistics.median(record["mib_per_second"] for record in selected),
        "median_worker_cpu_percent": statistics.median(
            statistics.mean(record["cpu_busy_percent"][:8]) for record in selected
        ),
        "median_worker_iowait_percent": statistics.median(
            statistics.mean(record["cpu_iowait_percent"][:8]) for record in selected
        ),
        "median_disk_busy_percent": statistics.median(
            record["disk_activity"]["sda"]["busy_percent"] for record in selected
        ),
        "task_counts": sorted({record["num_tasks"] for record in selected}),
        "observed_concurrency": sorted({record["observed_max_compressors"] for record in selected}),
        "records": selected,
    }


def main() -> None:
    """Audit the selected run batch and save statistics alongside its raw measurements."""
    parser = argparse.ArgumentParser()
    parser.add_argument("tag")
    args = parser.parse_args()
    records = [
        json.loads(line)
        for line in (benchmark.WORK / "measurements.jsonl").read_text().splitlines()
    ]
    selected = [record for record in records if f"_{args.tag}_" in record["label"]]
    audits = [audit(record) for record in selected]
    summary = {backend: summarize(selected, backend) for backend in ["celery", "spider"]}
    summary["spider_speedup"] = (
        summary["celery"]["median_seconds"] / summary["spider"]["median_seconds"]
    )
    benchmark.write(benchmark.WORK / "counter-audit.json", audits)
    benchmark.write(benchmark.WORK / "summary.json", summary)


if __name__ == "__main__":
    main()
