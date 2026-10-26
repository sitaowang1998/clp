#!/usr/bin/env python3
"""Compare filesystem compression through actual Celery and Spider services."""

import argparse
import contextlib
import hashlib
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import tarfile
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Self

import msgpack
import mysql.connector
import psutil
import zstandard

ROOT = Path(__file__).resolve().parents[3]
WORK = ROOT / "build/fs-compression-benchmark"
IMAGE = "clp-package:dev-ycx-d318"
PREFIX = "clp-fs-compression-bench"
DB_PORT = 53426
BROKER_PORT = 55726
REDIS_PORT = 56326
STORAGE_PORT = 55126
SCHEDULER_PORT = 55226
SPIDER_REV = "278d06f"
LOCAL_CREDENTIAL = "isolated-benchmark-only"
TARGET_SIZE = 64 * 1024 * 1024
JOB_TIMEOUT_SECS = 1200
JOB_SUCCEEDED = 2
ENV = {
    "CLP_HOME": "/bench",
    "CLP_CONFIG_PATH": "/bench/config.json",
    "CLP_DB_USER": "bench",
    "CLP_DB_PASS": LOCAL_CREDENTIAL,
    "CLP_LOGS_DIR": "/bench/logs",
    "CLP_LOGGING_LEVEL": "INFO",
    "CLP_DISABLE_TELEMETRY": "1",
    "BROKER_URL": f"amqp://bench:{LOCAL_CREDENTIAL}@127.0.0.1:{BROKER_PORT}//",
    "RESULT_BACKEND": f"redis://127.0.0.1:{REDIS_PORT}/0",
    "PYTHONPATH": ":".join(
        f"/src/components/{name}"
        for name in ["job-orchestration", "clp-py-utils", "clp-package-utils"]
    )
    + ":/opt/clp/lib/python3/site-packages",
    "SPIDER_STORAGE_DB_USERNAME": "bench",
    "SPIDER_STORAGE_DB_PASSWORD": LOCAL_CREDENTIAL,
    "SPIDER_EXTERNAL_RESOURCE_GROUP_ID": "compression",
    "SPIDER_RESOURCE_GROUP_PASSWORD": LOCAL_CREDENTIAL,
    "RUST_LOG": "info",
}


class ProcessSampler:
    """Observe compressor concurrency for one benchmark dataset."""

    def __init__(self, dataset: str) -> None:
        """Initialize a sampler scoped to this run's archive directory."""
        self.prefix = "/bench/archives/" + dataset
        self.stop = threading.Event()
        self.maximum = 0
        self.observed: set[int] = set()
        self.thread = threading.Thread(target=self.sample, daemon=True)

    def sample(self) -> None:
        """Count compression subprocesses without including unrelated host processes."""
        while not self.stop.is_set():
            active = []
            for process in psutil.process_iter(["cmdline"], ad_value=None):
                command = process.info["cmdline"] or []
                if (
                    len(command) > 2  # noqa: PLR2004
                    and command[:2] == ["/bench/bin/clp-s", "c"]
                    and command[2].startswith(self.prefix)
                ):
                    active.append(process.pid)
            self.maximum = max(self.maximum, len(active))
            self.observed.update(active)
            self.stop.wait(0.05)

    def __enter__(self) -> Self:
        """Start sampling when the measured job begins."""
        self.thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        """Stop sampling before validation queries begin."""
        self.stop.set()
        self.thread.join()


def run(
    command: list[str], *, capture_output: bool = False, text: bool = True
) -> subprocess.CompletedProcess[str]:
    """Run a command, preserving its arguments in the benchmark command log."""
    with (WORK / "commands.jsonl").open("a") as output:
        output.write(json.dumps(command) + "\n")
    return subprocess.run(command, check=True, capture_output=capture_output, text=text)


def write(path: Path, value: object) -> None:
    """Write a JSON benchmark artifact."""
    path.write_text(json.dumps(value, indent=2, default=str) + "\n")


def connect(database: str = "clp_bench") -> mysql.connector.MySQLConnection:
    """Connect to the isolated benchmark database."""
    return mysql.connector.connect(
        host="127.0.0.1",
        port=DB_PORT,
        user="bench",
        password=LOCAL_CREDENTIAL,
        database=database,
        autocommit=True,
        connection_timeout=5,
    )


def wait_port(port: int, timeout: int = 90) -> None:
    """Wait for a benchmark service to accept TCP connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with (
            contextlib.suppress(OSError),
            socket.create_connection(("127.0.0.1", port), timeout=1),
        ):
            return
        time.sleep(0.2)
    message = f"port {port} did not open"
    raise TimeoutError(message)


def wait_database(timeout: int = 90) -> None:
    """Wait until the database accepts authenticated queries."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with contextlib.suppress(mysql.connector.Error), connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            return
        time.sleep(0.2)
    message = "benchmark database did not become ready"
    raise TimeoutError(message)


def container_args(name: str | None = None, cpus: str = "8-15") -> list[str]:
    """Build container arguments with isolated mounts and fixed CPU affinity."""
    args = [
        "docker",
        "run",
        "--log-driver",
        "none",
        "--network",
        "host",
        "--tmpfs",
        "/tmp:rw,size=512m",  # noqa: S108 -- private container tmpfs
        "--cpuset-cpus",
        cpus,
    ]
    if name is None:
        args.append("--rm")
    else:
        args.extend(["--name", name, "-d"])
    args.extend(
        [
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-v",
            f"{ROOT}:/src:ro",
            "-v",
            f"{WORK}:/bench",
            "-v",
            f"{WORK / 'input'}:/mnt/logs:ro",
        ]
    )
    for key, value in ENV.items():
        args.extend(["-e", f"{key}={value}"])
    return args


def package_run(command: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a package command with the benchmark configuration."""
    return run([*container_args(), "--entrypoint", command[0], IMAGE, *command[1:]])


def config(backend: str) -> dict[str, Any]:
    """Build equivalent compressor settings for the selected backend."""
    result = {
        "package": {"storage_engine": "clp-s", "scheduler": backend},
        "database": {"host": "127.0.0.1", "port": DB_PORT, "names": {"clp": "clp_bench"}},
        "logs_input": {"type": "fs", "directory": "/mnt/logs"},
        "logs_directory": "/bench/logs",
        "tmp_directory": "/bench/tmp",
        "archive_output": {
            "storage": {"type": "fs", "directory": "/bench/archives"},
            "target_archive_size": TARGET_SIZE,
            "target_dictionaries_size": 16 * 1024 * 1024,
            "target_segment_size": 16 * 1024 * 1024,
            "target_encoded_file_size": 256 * 1024 * 1024,
            "compression_level": 3,
        },
        "compression_scheduler": {"jobs_poll_delay": 0.1, "max_concurrent_tasks_per_job": 0},
    }
    if backend == "spider":
        result.update(
            {
                "spider": {"host": "127.0.0.1", "port": STORAGE_PORT},
                "compression_coordinator": {
                    "job_polling_interval_millisecs": 100,
                    "max_concurrent_jobs": 1,
                    "result_polling": {"init_backoff_millisecs": 100, "max_backoff_millisecs": 100},
                },
                "query_coordinator": {},
            }
        )
    return result


def prepare(tarball: Path) -> None:
    """Prepare dataset files, service configuration, and provenance artifacts."""
    for name in ["bin", "logs", "tmp", "archives", "packages/clp", "input/spark", "input/smoke"]:
        (WORK / name).mkdir(parents=True, exist_ok=True)
    digest = hashlib.md5(tarball.read_bytes(), usedforsecurity=False).hexdigest()
    if digest != "99d441cf23d374b5870204622acd474e":
        message = f"unexpected Spark tarball MD5: {digest}"
        raise ValueError(message)
    files = []
    records = 0
    total = 0
    with tarfile.open(tarball) as archive:
        for index, member in enumerate(m for m in archive if m.isfile()):
            destination = WORK / "input/spark" / f"spark-{index:05d}.jsonl"
            if not destination.exists() or destination.stat().st_size != member.size:
                with archive.extractfile(member) as source, destination.open("wb") as output:
                    shutil.copyfileobj(source, output)
            files.append({"member": member.name, "path": destination.name, "size": member.size})
            total += member.size
            with destination.open("rb") as source:
                records += sum(1 for _ in source)
    for name in ["clp-s", "indexer", "log-converter"]:
        shutil.copy2(ROOT / "build/core" / name, WORK / "bin" / name)
    (WORK / "input/smoke/events.jsonl").write_text(
        "".join(
            json.dumps({"timestamp": 1700000000000 + i, "message": f"event {i}"}) + "\n"
            for i in range(1000)
        )
    )
    (WORK / "input/smoke/messages.log").write_text(
        "".join(f"2024-01-01 00:00:00 INFO event {i}\n" for i in range(1000))
    )
    write(
        WORK / "input-manifest.json",
        {
            "tarball": str(tarball),
            "md5": digest,
            "bytes": total,
            "records": records,
            "files": files,
        },
    )
    write(
        WORK / "provenance.json",
        {
            "git_head": run(["git", "rev-parse", "HEAD"], capture_output=True).stdout.strip(),
            "lscpu": run(["lscpu"], capture_output=True).stdout,
            "topology": run(["lscpu", "-e=CPU,CORE,SOCKET,NODE"], capture_output=True).stdout,
            "worker_cpus": "0-7",
            "control_cpus": "8-15",
            "spider_revision": SPIDER_REV,
            "package_image": IMAGE,
            "core_sha256": hashlib.sha256((WORK / "bin/clp-s").read_bytes()).hexdigest(),
            "cache_policy": (
                "warm filesystem cache; one warm-up per backend block; no cache eviction"
            ),
        },
    )
    write(
        WORK / "storage.json",
        {
            "host": "127.0.0.1",
            "port": STORAGE_PORT,
            "runtime": {
                "db": {
                    "host": "127.0.0.1",
                    "port": DB_PORT,
                    "name": "spider_bench",
                    "max_connections": 32,
                }
            },
        },
    )
    write(
        WORK / "scheduler.json",
        {
            "host": "127.0.0.1",
            "port": SCHEDULER_PORT,
            "storage_endpoint": {"host": "127.0.0.1", "port": STORAGE_PORT},
            "connection_pool_size": 2,
            "runtime": {
                "advertised_endpoint": {"host": "127.0.0.1", "port": SCHEDULER_PORT},
                "scheduler": {
                    "policy": "resource_group_round_robin",
                    "config": {
                        "active_job_list_capacity": 16,
                        "dispatch_queue_capacity": 64,
                        "ready_task_capacity": 4096,
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
    write(
        WORK / "worker.json",
        {
            "storage": {"host": "127.0.0.1", "port": STORAGE_PORT},
            "scheduler": {"host": "127.0.0.1", "port": SCHEDULER_PORT},
            "connection_pool_size": 2,
            "scheduler_poll_wait_ms": 100,
            "liveness": {
                "storage_heartbeat_interval_sec": 1,
                "scheduler_heartbeat_interval_sec": 1,
            },
            "task_executor": {
                "bin_path": "/bench/bin/spider-task-executor",
                "package_dir": "/bench/packages",
                "inherited_env": [
                    "CLP_HOME",
                    "CLP_CONFIG_PATH",
                    "CLP_DB_USER",
                    "CLP_DB_PASS",
                    "RUST_LOG",
                ],
            },
        },
    )
    print(json.dumps({"prepared_files": len(files), "input_bytes": total}), flush=True)  # noqa: T201


def dependencies() -> None:
    """Start isolated database and broker services and initialize their schemas."""
    for directory in ["database", "rabbit", "redis"]:
        (WORK / directory).mkdir(exist_ok=True)
    run(
        [
            "docker",
            "run",
            "--log-driver",
            "none",
            "-d",
            "--name",
            PREFIX + "-db",
            "-v",
            f"{WORK / 'database'}:/var/lib/mysql",
            "--cpuset-cpus",
            "8-15",
            "-p",
            f"127.0.0.1:{DB_PORT}:3306",
            "-e",
            f"MARIADB_ROOT_PASSWORD={LOCAL_CREDENTIAL}",
            "-e",
            "MARIADB_DATABASE=clp_bench",
            "-e",
            "MARIADB_USER=bench",
            "-e",
            f"MARIADB_PASSWORD={LOCAL_CREDENTIAL}",
            "mariadb:10.11.16",
        ]
    )
    run(
        [
            "docker",
            "run",
            "--log-driver",
            "none",
            "-d",
            "--name",
            PREFIX + "-rabbit",
            "-v",
            f"{WORK / 'rabbit'}:/var/lib/rabbitmq",
            "--cpuset-cpus",
            "8-15",
            "-p",
            f"127.0.0.1:{BROKER_PORT}:5672",
            "-e",
            "RABBITMQ_DEFAULT_USER=bench",
            "-e",
            f"RABBITMQ_DEFAULT_PASS={LOCAL_CREDENTIAL}",
            "rabbitmq:4.2.6",
        ]
    )
    run(
        [
            "docker",
            "run",
            "--log-driver",
            "none",
            "-d",
            "--name",
            PREFIX + "-redis",
            "-v",
            f"{WORK / 'redis'}:/data",
            "--cpuset-cpus",
            "8-15",
            "-p",
            f"127.0.0.1:{REDIS_PORT}:6379",
            "redis:7.4.8",
        ]
    )
    for port in [DB_PORT, BROKER_PORT, REDIS_PORT]:
        wait_port(port)
    wait_database()
    run(
        [
            "docker",
            "exec",
            PREFIX + "-db",
            "mariadb",
            "-uroot",
            f"-p{LOCAL_CREDENTIAL}",
            "-e",
            "CREATE DATABASE spider_bench; GRANT ALL ON spider_bench.* TO 'bench'@'%';",
        ]
    )
    write(WORK / "config.json", config("celery"))
    for script in ["initialize-orchestration-db.py", "initialize-clp-metadata-db.py"]:
        command = [
            "python3",
            "/src/components/clp-py-utils/clp_py_utils/" + script,
            "--config",
            "/bench/config.json",
        ]
        if "metadata" in script:
            command.extend(["--storage-engine", "clp-s"])
        package_run(command)


@contextlib.contextmanager
def services(backend: str, label: str) -> Iterator[list[str]]:
    """Run one backend and remove its containers when the measurement block finishes."""
    names = []

    def start(suffix: str, command: list[str], cpus: str = "8-15") -> None:
        name = PREFIX + "-" + suffix
        run(
            [
                *container_args(name, cpus),
                "--entrypoint",
                "/bin/sh",
                IMAGE,
                "-c",
                "exec "
                + shlex.join(command)
                + " > "
                + shlex.quote("/bench/logs/" + label + "-" + name + ".log")
                + " 2>&1",
            ],
            capture_output=True,
        )
        names.append(name)

    write(WORK / "config.json", config(backend))
    write(WORK / f"config-{label}.json", config(backend))
    try:
        if backend == "celery":
            start(
                "worker",
                [
                    "python3",
                    "-m",
                    "celery",
                    "-A",
                    "job_orchestration.executor.compress.celery",
                    "worker",
                    "-Q",
                    "compression",
                    "--concurrency",
                    "8",
                    "--loglevel",
                    "INFO",
                ],
                "0-7",
            )
            start(
                "coordinator",
                [
                    "python3",
                    "-m",
                    "job_orchestration.scheduler.compress.compression_scheduler",
                    "--config",
                    "/bench/config.json",
                ],
            )
        else:
            start("storage", ["/bench/bin/spider-storage", "--config", "/bench/storage.json"])
            wait_port(STORAGE_PORT)
            start("scheduler", ["/bench/bin/spider-scheduler", "--config", "/bench/scheduler.json"])
            wait_port(SCHEDULER_PORT)
            start(
                "coordinator",
                ["/bench/bin/compression-coordinator", "--config", "/bench/config.json"],
            )
            time.sleep(2)
            for index in range(8):
                start(
                    f"worker-{index}",
                    ["/bench/bin/spider-execution-manager", "--config", "/bench/worker.json"],
                    "0-7",
                )
        time.sleep(4)
        for name in names:
            state = run(
                ["docker", "inspect", "--format", "{{.State.Running}}", name],
                capture_output=True,
                text=True,
            )
            if state.stdout.strip() != "true":
                message = f"service exited: {name}"
                raise RuntimeError(message)
        runtime = []
        for name in names:
            details = json.loads(run(["docker", "inspect", name], capture_output=True).stdout)[0]
            runtime.append(
                {
                    "name": name,
                    "image_id": details["Image"],
                    "cpuset": details["HostConfig"]["CpusetCpus"],
                    "cpu_quota": details["HostConfig"]["CpuQuota"],
                    "cpu_period": details["HostConfig"]["CpuPeriod"],
                    "nano_cpus": details["HostConfig"]["NanoCpus"],
                }
            )
        write(WORK / f"runtime-{label}.json", runtime)
        yield names
    finally:
        for name in reversed(names):
            run(["docker", "stop", "-t", "15", name], capture_output=True)
            run(["docker", "rm", name], capture_output=True)


def dataset_name(label: str) -> str:
    """Validate the dataset name used in benchmark SQL identifiers."""
    dataset = label.replace("-", "_")
    if re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_]*", dataset) is None:
        message = f"invalid benchmark dataset name: {dataset}"
        raise ValueError(message)
    return dataset


def validate_record_count(dataset: str, smoke: bool) -> int:
    """Search every archive and verify that all input records remain present."""
    expected_count = (
        1000 if smoke else json.loads((WORK / "input-manifest.json").read_text())["records"]
    )
    result = run(
        [str(WORK / "bin/clp-s"), "s", str(WORK / "archives" / dataset), "*", "--count"],
        capture_output=True,
    )
    actual_count = sum(json.loads(line)["count"] for line in result.stdout.splitlines())
    if actual_count != expected_count:
        message = f"record count differs: expected {expected_count}, got {actual_count}"
        raise RuntimeError(message)
    return actual_count


def cpu_utilization(
    before_times: list[Any], after_times: list[Any]
) -> tuple[list[float], list[float]]:
    """Separate per-CPU execution time from I/O wait over a measured interval."""
    busy = []
    iowait = []
    for before, after in zip(before_times, after_times, strict=True):
        total = sum(after) - sum(before)
        idle = after.idle - before.idle
        waited = after.iowait - before.iowait
        busy.append(round(100 * (total - idle - waited) / total, 2) if total else 0)
        iowait.append(round(100 * waited / total, 2) if total else 0)
    return busy, iowait


def disk_activity(before: dict[str, Any], elapsed: float) -> dict[str, dict[str, float]]:
    """Record host block-device traffic and busy time during a compression run."""
    after = psutil.disk_io_counters(perdisk=True)
    return {
        name: {
            "read_bytes": after[name].read_bytes - before[name].read_bytes,
            "write_bytes": after[name].write_bytes - before[name].write_bytes,
            "busy_percent": 100 * (after[name].busy_time - before[name].busy_time) / elapsed / 1000,
        }
        for name in ["sda", "nvme0n1"]
        if name in before and name in after
    }


def measure(backend: str, label: str, warmup: bool, smoke: str | None = None) -> dict[str, Any]:
    """Submit a compression job and record elapsed time and output validation."""
    dataset = dataset_name(label)
    input_path = "/mnt/logs/spark"
    timestamp_key = "Timestamp"
    if smoke:
        input_path = "/mnt/logs/smoke/" + (
            "messages.log" if smoke == "unstructured" else "events.jsonl"
        )
        timestamp_key = "timestamp"
    payload = {
        "input": {
            "type": "fs",
            "dataset": dataset,
            "paths_to_compress": [input_path],
            "timestamp_key": timestamp_key,
            "unstructured": smoke == "unstructured",
            "path_prefix_to_remove": "/mnt/logs",
        },
        "output": {k: v for k, v in config(backend)["archive_output"].items() if k != "storage"},
    }
    encoded = zstandard.ZstdCompressor().compress(msgpack.packb(payload))
    cpu_before = psutil.cpu_times(percpu=True)
    disk_before = psutil.disk_io_counters(perdisk=True)
    load_before = os.getloadavg()
    with ProcessSampler(dataset) as sampler, connect() as connection:
        cursor = connection.cursor(dictionary=True)
        started = time.monotonic()
        cursor.execute("INSERT INTO compression_jobs (clp_config) VALUES (%s)", (encoded,))
        job_id = cursor.lastrowid
        while True:
            cursor.execute("SELECT * FROM compression_jobs WHERE id=%s", (job_id,))
            row = cursor.fetchone()
            if row["status"] not in [0, 1]:
                break
            if time.monotonic() - started > JOB_TIMEOUT_SECS:
                message = f"job {job_id} timed out"
                raise TimeoutError(message)
            time.sleep(0.01)
        elapsed = time.monotonic() - started
        if row["status"] != JOB_SUCCEEDED:
            message = f"compression failed: {row['status_msg']}"
            raise RuntimeError(message)
        cursor.execute(
            "SELECT COUNT(*) AS archives, SUM(uncompressed_size) AS raw_size, "  # noqa: S608
            f"SUM(size) AS stored_size FROM clp_{dataset}_archives"
        )
        archives = cursor.fetchone()
        if (
            not archives["archives"]
            or archives["raw_size"] != row["uncompressed_size"]
            or archives["stored_size"] != row["compressed_size"]
            or row["num_tasks_completed"] != row["num_tasks"]
        ):
            message = "archive metadata totals disagree with job"
            raise RuntimeError(message)
        cursor.execute(
            f"SELECT id FROM clp_{dataset}_archives ORDER BY id"  # noqa: S608 -- validated identifier
        )
        archive_ids = [item["id"] for item in cursor.fetchall()]
        if any(
            not (WORK / "archives" / dataset / archive_id).exists() for archive_id in archive_ids
        ):
            message = "published archive missing from filesystem"
            raise RuntimeError(message)
    cpu_after = psutil.cpu_times(percpu=True)
    disks = disk_activity(disk_before, elapsed)
    busy, iowait = cpu_utilization(cpu_before, cpu_after)
    actual_count = validate_record_count(dataset, smoke is not None)
    record = {
        "backend": backend,
        "label": label,
        "warmup": warmup,
        "smoke": smoke,
        "job_id": job_id,
        "elapsed_seconds": elapsed,
        "input_bytes": row["uncompressed_size"],
        "compressed_bytes": row["compressed_size"],
        "num_tasks": row["num_tasks"],
        "num_tasks_completed": row["num_tasks_completed"],
        "archives": archives["archives"],
        "mib_per_second": row["uncompressed_size"] / 1024**2 / elapsed,
        "load_before": load_before,
        "load_after": os.getloadavg(),
        "cpu_busy_percent": busy,
        "cpu_iowait_percent": iowait,
        "disk_activity": disks,
        "validated_records": actual_count,
        "observed_max_compressors": sampler.maximum,
        "observed_compressor_processes": len(sampler.observed),
        "sql_duration": row["duration"],
        "spider_id": row["spider_id"],
    }
    with (WORK / "measurements.jsonl").open("a") as output:
        output.write(json.dumps(record, default=str) + "\n")
    print(json.dumps(record, default=str), flush=True)  # noqa: T201
    return record


def main() -> None:
    """Run the requested benchmark phase."""
    os.sched_setaffinity(0, range(8, 16))
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action", choices=["prepare", "dependencies", "smoke", "baseline", "compare"]
    )
    parser.add_argument(
        "--tarball",
        type=Path,
        default=Path("/mnt/SSD-4T/sitao/zenodo-10516345/spark-event-logs.tar.gz"),
    )
    args = parser.parse_args()
    WORK.mkdir(parents=True, exist_ok=True)
    if args.action == "prepare":
        prepare(args.tarball)
    elif args.action == "dependencies":
        dependencies()
    else:
        tag = str(int(time.time()))
        backends = (
            ["celery"] if args.action == "baseline" else ["celery", "spider", "spider", "celery"]
        )
        if args.action == "smoke":
            backends = ["celery", "spider"]
        for block, backend in enumerate(backends):
            label = f"{backend}_{tag}_{block}"
            with services(backend, label):
                if args.action == "smoke":
                    for kind in ["structured", "unstructured"]:
                        measure(backend, label + "_" + kind, False, kind)
                else:
                    for iteration in range(4):
                        measure(backend, label + f"_{iteration}", iteration == 0)


if __name__ == "__main__":
    main()
