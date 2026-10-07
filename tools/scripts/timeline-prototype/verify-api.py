#!/usr/bin/env python3
"""
Check actual HTTP/SSE results from the Spider experiment and terminal-state gating.

Uses an isolated query_jobs table in timeline_api on the prototype MariaDB container.
Requires Python msgpack and the API binary built from the current branch.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

import msgpack


def sql(statement: str) -> None:
    """Execute SQL against this prototype's isolated database container."""
    subprocess.run(
        [
            str(shutil.which("docker")),
            "exec",
            "-i",
            "clp-timeline-prototype-spider-db",
            "mariadb",
            "-uroot",
            "-ptimeline-local",
        ],
        input=statement,
        text=True,
        check=True,
        capture_output=True,
    )


def request(job: int) -> tuple[int, str]:
    """Fetch the production SSE endpoint, preserving error responses."""
    try:
        with urlopen(f"http://127.0.0.1:55153/query_results/{job}", timeout=10) as response:
            return response.status, response.read().decode()
    except HTTPError as error:
        return error.code, error.read().decode()


def require(condition: bool, message: str) -> None:
    """Stop the experiment on a failed semantic assertion."""
    if not condition:
        raise RuntimeError(message)


def seed(work: Path) -> None:
    """Seed minimal orchestration records for results already produced by Spider."""
    config = json.loads((work / "baseline.json").read_text())["search"]
    encoded = msgpack.packb(config).hex()
    sql(
        "CREATE DATABASE IF NOT EXISTS timeline_api;"  # noqa: S608 -- only hex bytes vary.
        "GRANT ALL ON timeline_api.* TO 'timeline'@'%';"
        "CREATE TABLE IF NOT EXISTS timeline_api.query_jobs "
        "(id BIGINT PRIMARY KEY, status INT, job_config LONGBLOB);"
        "DELETE FROM timeline_api.query_jobs;"
        f"INSERT INTO timeline_api.query_jobs VALUES (81001,2,UNHEX('{encoded}'));"
    )


def measure(work: Path) -> list[dict[str, object]]:
    """Check exact SSE counts, non-success states, and polling until success."""
    expected = json.loads((work / "expected.json").read_text())
    started = time.monotonic()
    status, body = request(81001)
    rows = [
        json.loads(line.removeprefix("data: "))
        for line in body.splitlines()
        if line.startswith("data: ")
    ]
    actual = {str(row["timestamp"]): row["count"] for row in rows}
    require(status == HTTPStatus.OK and actual == expected, "SSE histogram differs from reference")
    (work / "api-success.sse").write_text(body)
    evidence: list[dict[str, object]] = [
        {
            "case": "success_sse",
            "status": status,
            "buckets": len(rows),
            "count": sum(actual.values()),
            "elapsed_seconds": time.monotonic() - started,
        }
    ]
    for state, name in [(3, "failed"), (4, "cancelling"), (5, "cancelled"), (6, "killed")]:
        sql(f"UPDATE timeline_api.query_jobs SET status={state} WHERE id=81001;")  # noqa: S608 -- fixed integer states.
        status, body = request(81001)
        require(
            status == HTTPStatus.INTERNAL_SERVER_ERROR and "data:" not in body,
            f"{name} exposed timeline results",
        )
        evidence.append({"case": name, "http_status": status, "body": body})
    sql("UPDATE timeline_api.query_jobs SET status=1 WHERE id=81001;")
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(request, 81001)
        time.sleep(0.3)
        require(not future.done(), "running job returned final results")
        sql("UPDATE timeline_api.query_jobs SET status=2 WHERE id=81001;")
        status, body = future.result(timeout=10)
        require(
            status == HTTPStatus.OK and "data:" in body, "success transition did not release SSE"
        )
    evidence.append(
        {"case": "running_then_success", "blocked_before_success": True, "http_status": status}
    )
    return evidence


def main() -> None:
    """Start the production API server and persist checked SSE evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="build/rust-targets/release/api_server", type=Path)
    parser.add_argument("--output", default=".tmp/timeline-spider", type=Path)
    args = parser.parse_args()
    work = args.output.resolve()
    seed(work)
    config = {
        "package": {"storage_engine": "clp-s"},
        "api_server": {"host": "127.0.0.1", "port": 55153},
        "database": {"host": "127.0.0.1", "port": 53316, "names": {"clp": "timeline_api"}},
        "results_cache": {"host": "127.0.0.1", "port": 27028, "db_name": "timeline_spider"},
    }
    (work / "api.json").write_text(json.dumps(config, indent=2))
    with (work / "api.log").open("w") as log:
        process = subprocess.Popen(
            [str(args.api.resolve()), "--config", str(work / "api.json")],
            cwd=work,
            stdout=log,
            stderr=subprocess.STDOUT,
            env={**os.environ, "CLP_DB_USER": "timeline", "CLP_DB_PASS": "timeline-local"},
        )
        try:
            time.sleep(2)
            require(process.poll() is None, "API process exited during startup")
            evidence = measure(work)
            output = json.dumps(evidence, indent=2) + "\n"
            (work / "api-evidence.json").write_text(output)
            sys.stdout.write(output)
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    main()
