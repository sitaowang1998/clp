#!/usr/bin/env python3
"""
Exercise real Spider timeline tasks against deterministic local archives.

Requires an isolated MariaDB on localhost:53316 and the prototype Mongo container.
All Spider processes started here are stopped on exit; fixture data and logs remain.
"""

import argparse
import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any


def write_json(path: Path, value: object) -> None:
    """Write a configuration or evidence artifact."""
    path.write_text(json.dumps(value, indent=2) + "\n")


def wait_port(port: int, process: subprocess.Popen) -> None:
    """Wait for a service listener, failing if its process exits."""
    for _ in range(200):
        if process.poll() is not None:
            message = f"service exited with {process.returncode}"
            raise RuntimeError(message)
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)
    message = f"port {port} did not become ready"
    raise TimeoutError(message)


def mongo(container: str, expression: str) -> list[dict[str, Any]]:
    """Read or reset this experiment's isolated result database."""
    result = subprocess.run(
        [
            str(shutil.which("docker")),
            "exec",
            container,
            "mongosh",
            "--quiet",
            "--eval",
            "const d=db.getSiblingDB('timeline_spider');" + expression,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(
        result.stdout,
        object_hook=lambda value: int(value["$numberLong"]) if "$numberLong" in value else value,
    )


def parse_args() -> argparse.Namespace:
    """Build fixture archives, run experiments, and persist measured evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spider-bin-dir", required=True, type=Path)
    parser.add_argument("--core", default="build/core/clp-s", type=Path)
    parser.add_argument(
        "--tdl", default="build/rust-targets/release/libclp_tdl_package.so", type=Path
    )
    parser.add_argument(
        "--submitter", default="build/rust-targets/release/examples/timeline", type=Path
    )
    parser.add_argument("--output", default=".tmp/timeline-spider", type=Path)
    parser.add_argument("--mongo-container", default="clp-timeline-prototype-mongo")
    parser.add_argument("--rows-per-archive", type=int, default=100000)
    return parser.parse_args()


def prepare_executor(args: argparse.Namespace, work: Path) -> None:
    """Install a one-shot executor fault wrapper and the prototype task library."""
    work.mkdir(parents=True, exist_ok=True)
    core = args.core.resolve()
    (work / "bin").mkdir(exist_ok=True)
    (work / "packages/clp").mkdir(parents=True, exist_ok=True)
    library = work / "packages/clp/libclp.so"
    library.unlink(missing_ok=True)
    shutil.copy2(args.tdl.resolve(), library)
    # A one-shot crash after clp-s publishes exercises Spider's actual executor retry path.
    marker = work / "kill-executor-once"
    (work / "bin/clp-s").write_text(
        "#!/usr/bin/env python3\nimport os, pathlib, signal, subprocess, sys\n"
        f"status = subprocess.call([{str(core)!r}] + sys.argv[1:])\n"
        f"marker = pathlib.Path({str(marker)!r})\n"
        "if status == 0 and marker.exists():\n"
        "    try:\n        marker.unlink()\n"
        "    except FileNotFoundError:\n        pass\n"
        "    else:\n        os.kill(os.getppid(), signal.SIGKILL)\n"
        "sys.exit(status)\n"
    )
    (work / "bin/clp-s").chmod(0o755)


def prepare_archives(work: Path, core: Path, rows_per_archive: int) -> list[dict[str, str]]:
    """Compress independent archives and compute a raw-event reference histogram."""
    archives = work / "archives/default"
    archives.mkdir(parents=True, exist_ok=True)
    selected = []
    expected = {}
    for index in range(4):
        fixture = work / f"fixture-{index}.jsonl"
        with fixture.open("w") as output:
            for row in range(rows_per_archive):
                timestamp = 1700000000000 + (row % 100) * 1000 + index
                output.write(
                    json.dumps(
                        {
                            "timestamp": timestamp,
                            "level": "error" if row % 3 == 0 else "info",
                            "message": f"event {row}",
                        }
                    )
                    + "\n"
                )
                if row % 3 == 0:
                    bucket = timestamp // 10000 * 10000
                    expected[str(bucket)] = expected.get(str(bucket), 0) + 1
        before = set(archives.iterdir())
        subprocess.run(
            [str(core), "c", "--timestamp-key", "timestamp", str(archives), str(fixture)],
            check=True,
            capture_output=True,
        )
        created = set(archives.iterdir()) - before
        selected.extend({"id": path.name, "dataset": "default"} for path in sorted(created))
    write_json(work / "expected.json", expected)
    return selected


def prepare_configs(work: Path, spider: Path) -> None:
    """Configure the isolated Spider cluster and CLP task executor."""
    write_json(
        work / "clp.json",
        {
            "package": {"storage_engine": "clp-s"},
            "archive_output": {"storage": {"type": "fs", "directory": str(work / "archives")}},
        },
    )
    storage_endpoint = {"host": "127.0.0.1", "port": 55151}
    scheduler_endpoint = {"host": "127.0.0.1", "port": 55152}
    write_json(
        work / "storage.json",
        {
            **storage_endpoint,
            "runtime": {
                "db": {
                    "host": "127.0.0.1",
                    "port": 53316,
                    "name": "timeline",
                    "max_connections": 16,
                }
            },
        },
    )
    write_json(
        work / "scheduler.json",
        {
            **scheduler_endpoint,
            "storage_endpoint": storage_endpoint,
            "connection_pool_size": 2,
            "runtime": {
                "advertised_endpoint": scheduler_endpoint,
                "scheduler": {
                    "policy": "round_robin",
                    "config": {
                        "active_job_queue_capacity": 16,
                        "dispatch_queue_capacity": 16,
                        "ready_task_capacity": 1024,
                        "commit_ready_task_capacity": 256,
                        "cleanup_ready_task_capacity": 256,
                        "storage_poll_timeout_ms": 10,
                        "tick_interval_ms": 5,
                        "finalizing_job_expiration_timeout_sec": 300,
                    },
                },
            },
        },
    )
    write_json(
        work / "worker.json",
        {
            "storage": storage_endpoint,
            "scheduler": scheduler_endpoint,
            "connection_pool_size": 2,
            "scheduler_poll_wait_ms": 100,
            "liveness": {
                "storage_heartbeat_interval_sec": 1,
                "scheduler_heartbeat_interval_sec": 1,
            },
            "task_executor": {
                "bin_path": str(spider / "spider-task-executor"),
                "package_dir": str(work / "packages"),
                "inherited_env": ["CLP_HOME", "CLP_CONFIG_PATH", "RUST_LOG"],
            },
        },
    )


def stop_process(process: subprocess.Popen) -> None:
    """Reap a stopped service, force-killing it if shutdown times out."""
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()


@contextmanager
def cluster(work: Path, spider: Path) -> Iterator[None]:
    """Run isolated native Spider services and stop them when the experiment finishes."""
    env = {
        **os.environ,
        "SPIDER_STORAGE_DB_USERNAME": "timeline",
        "SPIDER_STORAGE_DB_PASSWORD": "timeline-local",
        "CLP_HOME": str(work),
        "CLP_CONFIG_PATH": str(work / "clp.json"),
        "RUST_LOG": "info",
    }
    processes = []
    logs = []
    try:
        for name, config, port in [
            ("spider-storage", "storage", 55151),
            ("spider-scheduler", "scheduler", 55152),
            ("spider-execution-manager", "worker", None),
        ]:
            log = (work / f"{config}.log").open("w")
            logs.append(log)
            process = subprocess.Popen(
                [str(spider / name), "--config", str(work / f"{config}.json")],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            processes.append(process)
            if port:
                wait_port(port, process)
        time.sleep(1)
        yield
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in processes:
            stop_process(process)
        for log in logs:
            log.close()


def aggregate_rows(
    rows: list[dict[str, Any]], *, require_committed: bool = False
) -> dict[str, int]:
    """Validate archive contributions and, when present, their committed histogram."""
    actual: dict[str, int] = {}
    final: dict[str, int] = {}
    committed = False
    for row in rows:
        identity = row.get("_id")
        if identity == "__clp_timeline_committed_v1":
            if set(row) != {"_id"}:
                message = "unexpected commit marker fields"
                raise RuntimeError(message)
            committed = True
            continue
        if set(row) != {"_id", "count"}:
            message = "unexpected timeline document fields"
            raise RuntimeError(message)
        if type(identity) is int:
            final[str(identity)] = row["count"]
        else:
            timestamp = str(identity["timestamp"])
            actual[timestamp] = actual.get(timestamp, 0) + row["count"]
    if require_committed and not committed:
        message = "missing timeline commit marker"
        raise RuntimeError(message)
    if committed and final != actual:
        message = "committed histogram differs from archive contributions"
        raise RuntimeError(message)
    return final if committed else actual


def verify_retry(work: Path, archive_count: int) -> None:
    """Require a retried real executor attempt and retain the corresponding worker log records."""
    records = [
        json.loads(line)["fields"] for line in (work / "worker.log").read_text().splitlines()
    ]
    fault_records = [record for record in records if str(record.get("query_job_id")) == "81002"]
    starts = sum(record["message"] == "clp-s query task started." for record in fault_records)
    completions = sum(
        record["message"] == "clp-s query task completed successfully." for record in fault_records
    )
    write_json(work / "retry-evidence.json", fault_records)
    if starts != archive_count + 1 or completions != archive_count:
        message = "executor crash did not produce exactly one retried task attempt"
        raise RuntimeError(message)


def measure_direct_core(
    args: argparse.Namespace, work: Path, selected: list[dict[str, str]]
) -> None:
    """Measure the same core commands sequentially without Spider scheduling."""
    evidence = []
    expected = json.loads((work / "expected.json").read_text())
    for index in range(6):
        collection = str(81100 + index)
        started = time.monotonic()
        for archive in selected:
            subprocess.run(
                [
                    str(args.core.resolve()),
                    "s",
                    str(work / "archives/default"),
                    "--archive-id",
                    archive["id"],
                    "level:error",
                    "--count-by-time",
                    "10000",
                    "results-cache",
                    "--uri",
                    "mongodb://127.0.0.1:27028/timeline_spider",
                    "--collection",
                    collection,
                    "--dataset",
                    "default",
                ],
                capture_output=True,
                check=True,
            )
        elapsed = time.monotonic() - started
        rows = mongo(
            args.mongo_container,
            f"print(EJSON.stringify(d.getCollection('{collection}').find().toArray()));",
        )
        matches = aggregate_rows(rows) == expected
        evidence.append(
            {
                "iteration": index,
                "warmup": index == 0,
                "elapsed_seconds": elapsed,
                "matches_reference": matches,
            }
        )
        if not matches:
            message = "sequential core histogram differs from raw-event reference"
            raise RuntimeError(message)
    write_json(work / "direct-core-evidence.json", evidence)


def run_experiments(args: argparse.Namespace, work: Path, selected: list[dict[str, str]]) -> None:
    """Measure baseline, duplicate delivery, and actual executor-death retry."""
    evidence = []
    mongo(args.mongo_container, "d.dropDatabase(); print(EJSON.stringify([]));")
    for label, job_id, fault in [
        ("baseline", 81001, False),
        ("replay", 81001, False),
        ("executor_death_after_write", 81002, True),
        *[(f"warm_baseline_{index}", 81003 + index, False) for index in range(5)],
    ]:
        if fault:
            (work / "kill-executor-once").touch()
        manifest = {
            "storage_endpoint": "http://127.0.0.1:55151",
            "mongo_uri": "mongodb://127.0.0.1:27028/timeline_spider",
            "query_job_id": job_id,
            "search": {
                "query_string": "level:error",
                "max_num_results": 1,
                "aggregation_config": {"count_by_time_bucket_size": 10000},
            },
            "archives": selected,
        }
        write_json(work / f"{label}.json", manifest)
        started = time.monotonic()
        result = subprocess.run(
            [str(args.submitter.resolve()), str(work / f"{label}.json")],
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
        (work / f"{label}.stdout").write_text(result.stdout)
        (work / f"{label}.stderr").write_text(result.stderr)
        rows = mongo(
            args.mongo_container,
            f"print(EJSON.stringify(d.getCollection('{job_id}').find().toArray()));",
        )
        write_json(work / f"{label}-rows.json", rows)
        actual = aggregate_rows(rows, require_committed=True)
        matches_reference = actual == json.loads((work / "expected.json").read_text())
        evidence.append(
            {
                "case": label,
                "exit_code": result.returncode,
                "wall_seconds": time.monotonic() - started,
                "documents": len(rows),
                "matches_reference": matches_reference,
                "total_count": sum(actual.values()),
                "submitter_output": result.stdout,
                "stderr": result.stderr,
            }
        )
        write_json(work / "evidence.json", evidence)
        if result.returncode:
            message = f"{label} failed: {result.stderr}"
            raise RuntimeError(message)
        if not matches_reference:
            message = f"{label} histogram differs from raw-event reference"
            raise RuntimeError(message)
    sys.stdout.write(json.dumps(evidence, indent=2) + "\n")


def main() -> None:
    """Build fixtures, exercise real tasks, and leave reproducible evidence."""
    args = parse_args()
    work = args.output.resolve()
    work.mkdir(parents=True, exist_ok=True)
    snapshot = work / "clp-s-core"
    shutil.copy2(args.core.resolve(), snapshot)
    args.core = snapshot
    prepare_executor(args, work)
    selected = prepare_archives(work, args.core.resolve(), args.rows_per_archive)
    write_json(
        work / "metadata.json",
        {
            "archives": len(selected),
            "events": 4 * args.rows_per_archive,
            "input_bytes": sum(path.stat().st_size for path in work.glob("fixture-*.jsonl")),
            "worker_slots": 1,
            "host_cpu_count": os.cpu_count(),
            "host": tuple(os.uname()),
            "core_sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(),
            "tdl_sha256": hashlib.sha256(
                (work / "packages/clp/libclp.so").read_bytes()
            ).hexdigest(),
            "spider_bin_dir": str(args.spider_bin_dir.resolve()),
        },
    )
    prepare_configs(work, args.spider_bin_dir.resolve())
    with cluster(work, args.spider_bin_dir.resolve()):
        run_experiments(args, work, selected)
        measure_direct_core(args, work, selected)
    verify_retry(work, len(selected))


if __name__ == "__main__":
    main()
