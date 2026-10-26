#!/usr/bin/env python3
"""Exercise filesystem compression failure, empty-input, and restart contracts."""

import time
from typing import Any

import msgpack
import run as benchmark
import zstandard

SUCCEEDED = 2
FAILED = 3


def submit(dataset: str, path: str) -> int:
    """Submit a filesystem job through the public SQL contract."""
    payload = {
        "input": {
            "type": "fs",
            "dataset": dataset,
            "paths_to_compress": [path],
            "timestamp_key": "Timestamp",
            "unstructured": False,
        },
        "output": {
            key: value
            for key, value in benchmark.config("spider")["archive_output"].items()
            if key != "storage"
        },
    }
    encoded = zstandard.ZstdCompressor().compress(msgpack.packb(payload))
    with benchmark.connect() as connection, connection.cursor() as cursor:
        cursor.execute("INSERT INTO compression_jobs (clp_config) VALUES (%s)", (encoded,))
        return int(cursor.lastrowid)


def wait(job_id: int, *, running: bool = False) -> dict[str, Any]:
    """Wait for submission or completion while retaining the full persisted state."""
    deadline = time.monotonic() + benchmark.JOB_TIMEOUT_SECS
    with benchmark.connect() as connection, connection.cursor(dictionary=True) as cursor:
        while time.monotonic() < deadline:
            cursor.execute("SELECT * FROM compression_jobs WHERE id=%s", (job_id,))
            row = cursor.fetchone()
            if running and row["status"] == 1 and row["spider_id"] is not None:
                return row
            if row["status"] not in [0, 1]:
                return row
            time.sleep(0.01)
    message = f"job {job_id} did not reach the expected state"
    raise TimeoutError(message)


def require(condition: bool, message: str) -> None:
    """Fail the validation run when an externally observed contract is violated."""
    if not condition:
        raise RuntimeError(message)


def check_empty_and_missing(tag: str) -> list[dict[str, Any]]:
    """Verify empty directories, zero-byte files, and invalid input paths."""
    (benchmark.WORK / "input/empty").mkdir(exist_ok=True)
    (benchmark.WORK / "input/zero.jsonl").write_text("")
    results = []
    for kind, path, expected in [
        ("empty", "/mnt/logs/empty", SUCCEEDED),
        ("zero", "/mnt/logs/zero.jsonl", SUCCEEDED),
        ("missing", "/mnt/logs/missing-file.jsonl", FAILED),
    ]:
        row = wait(submit(f"verify_{kind}_{tag}", path))
        require(row["status"] == expected, f"{kind}: unexpected status {row['status_msg']}")
        if expected == SUCCEEDED:
            require(row["uncompressed_size"] == 0, f"{kind}: nonzero byte count")
            require(row["num_tasks_completed"] == row["num_tasks"], f"{kind}: incomplete counters")
        results.append({key: value for key, value in row.items() if key != "clp_config"})
    return results


def check_restart(tag: str) -> dict[str, Any]:
    """Restart only the coordinator and verify that the existing Spider job completes once."""
    dataset = "verify_restart_" + tag
    job_id = submit(dataset, "/mnt/logs/spark")
    before = wait(job_id, running=True)
    require(before["status"] == 1, "restart job completed before interruption")
    with benchmark.ProcessSampler(dataset) as sampler:
        deadline = time.monotonic() + 60
        while sampler.maximum == 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        require(sampler.maximum > 0, "no compressor started before restart deadline")
    name = benchmark.PREFIX + "-coordinator"
    benchmark.run(["docker", "kill", "--signal", "KILL", name], capture_output=True)
    benchmark.run(["docker", "start", name], capture_output=True)
    after = wait(job_id)
    require(after["status"] == SUCCEEDED, "recovered compression failed: " + after["status_msg"])
    require(after["spider_id"] == before["spider_id"], "restart submitted another Spider job")
    require(after["num_tasks_completed"] == after["num_tasks"], "recovered counters are incomplete")
    count = benchmark.validate_record_count(dataset, False)
    return {
        "job_id": job_id,
        "spider_id": after["spider_id"],
        "validated_records": count,
        "num_tasks": after["num_tasks"],
        "num_tasks_completed": after["num_tasks_completed"],
        "status": after["status"],
        "compressors_observed_before_restart": sampler.maximum,
    }


def main() -> None:
    """Run the contract checks against isolated Spider services."""
    tag = str(int(time.time()))
    with benchmark.services("spider", "verify_" + tag):
        results = {"empty_and_missing": check_empty_and_missing(tag), "restart": check_restart(tag)}
    benchmark.write(benchmark.WORK / "verification.json", results)


if __name__ == "__main__":
    main()
