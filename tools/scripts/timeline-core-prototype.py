#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["pymongo==4.18.2"]
# ///
"""Exercise real clp-s timeline persistence against an isolated test MongoDB."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median
from typing import Any

from bson.int64 import Int64
from bson.objectid import ObjectId
from pymongo import MongoClient

TIMESTAMPS = (-1001, -1000, -999, -1, 0, 1, 999, 1000, 1001, 1999, 2000)


def check(condition: bool, description: str) -> None:
    """Raise an error when an observed result violates the expected behavior."""
    if not condition:
        raise RuntimeError(description)


@dataclass(frozen=True)
class Archive:
    """An archive's on-disk location and search identity."""

    directory: Path
    identifier: str


@dataclass(frozen=True)
class SearchOptions:
    """Core search and results-cache options varied by the experiment."""

    dataset: str = "default"
    extra: tuple[str, ...] = ()
    aggregation: tuple[str, ...] = ("--count-by-time", "1000")
    uri_suffix: str = ""


DEFAULT_SEARCH_OPTIONS = SearchOptions()


class Experiment:
    """Isolated archives, MongoDB namespace, and correctness checks for timeline writes."""

    def __init__(self, binary: Path, uri: str, output: Path) -> None:
        """Create a unique database namespace for one experiment run."""
        self.binary = binary.resolve()
        self.output = output
        self.output.mkdir(parents=True, exist_ok=True)
        self.database_name = f"timeline_core_{time.time_ns()}"
        self.client: MongoClient[dict[str, Any]] = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.database = self.client[self.database_name]
        self.core_uri = f"{uri}/{self.database_name}?appName=timeline-core&retryWrites=false"
        self.outcomes: dict[str, Any] = {}
        self.expected = dict(Counter(int(ts / 1000) * 1000 for ts in TIMESTAMPS))
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        records = [
            {
                "timestamp": (epoch + timedelta(milliseconds=ts)).isoformat(
                    timespec="milliseconds"
                ),
                "level": "error",
            }
            for ts in TIMESTAMPS
        ]
        self.archives = [self.compress(records, f"input-{index}") for index in range(2)]

    def compress(self, records: list[dict[str, Any]], name: str) -> Archive:
        """Compress a JSON fixture and return its single-archive selector."""
        source = self.output / f"{name}.jsonl"
        source.write_text(
            "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
        )
        directory = self.output / f"archives-{time.time_ns()}-{name}"
        subprocess.run(
            [str(self.binary), "c", str(directory), str(source), "--timestamp-key", "timestamp"],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        return Archive(directory, next(directory.iterdir()).name)

    def run(
        self,
        archive: Archive,
        collection: str,
        *,
        options: SearchOptions = DEFAULT_SEARCH_OPTIONS,
        expect_success: bool = True,
    ) -> float:
        """Search one archive, check its exit status, and return elapsed seconds."""
        command = [
            str(self.binary),
            "s",
            str(archive.directory),
            "*",
            "--archive-id",
            archive.identifier,
            *options.aggregation,
            *options.extra,
            "results-cache",
            "--uri",
            self.core_uri + options.uri_suffix,
            "--collection",
            collection,
            "--dataset",
            options.dataset,
            "--batch-size",
            "1",
        ]
        started = time.monotonic()
        result = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
        check(
            (result.returncode == 0) == expect_success,
            f"Unexpected search status {result.returncode}: {result.stderr}",
        )
        return time.monotonic() - started

    def counts(self, collection: str) -> dict[int, int]:
        """Aggregate persisted per-archive partials by timestamp."""
        return {
            int(row["_id"]): int(row["count"])
            for row in self.database[collection].aggregate(
                [
                    {"$group": {"_id": "$timestamp", "count": {"$sum": "$count"}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        }

    def fault(self, mode: str | dict[str, int], data: dict[str, Any]) -> None:
        """Configure faults only for this experiment's core client update commands."""
        self.client.admin.command(
            {
                "configureFailPoint": "failCommand",
                "mode": mode,
                "data": {"failCommands": ["update"], "appName": "timeline-core", **data},
            }
        )

    def check_replay(self) -> None:
        """Check sequential and overlapping retries preserve partial counts."""
        archive = self.archives[0]
        self.run(archive, "replay")
        check(self.counts("replay") == self.expected, "initial counts differ")
        self.run(archive, "replay")
        check(self.counts("replay") == self.expected, "retry changed counts")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(self.run, archive, "replay") for _ in range(4)]
            for future in futures:
                future.result()
        check(self.counts("replay") == self.expected, "overlapping attempts changed counts")
        self.outcomes["repeated_and_overlapping_attempts"] = "passed"
        for document in self.database.replay.find():
            check(isinstance(document["timestamp"], Int64), "timestamp is not BSON int64")
            check(isinstance(document["count"], Int64), "count is not BSON int64")
            check(
                list(document["_id"]) == ["dataset", "archive_id", "timestamp"],
                "unexpected partial identity",
            )
        self.outcomes["bson_types_and_identity"] = "passed"
        self.run(self.archives[1], "replay")
        self.run(archive, "replay", options=SearchOptions(dataset="other"))
        check(
            self.counts("replay") == {key: value * 3 for key, value in self.expected.items()},
            "cross-archive or cross-dataset counts differ",
        )
        check(
            self.database.replay.count_documents({}) == len(self.expected) * 3,
            "partial identity collided across archives or datasets",
        )
        self.outcomes["cross_archive_and_dataset_counts"] = "passed"

    def check_bounds(self) -> None:
        """Check inclusive timestamp filtering and searches with no matches."""
        self.run(
            self.archives[0],
            "bounded",
            options=SearchOptions(extra=("--tge", "1", "--tle", "1000")),
        )
        check(self.counts("bounded") == {0: 2, 1000: 1}, "inclusive bound counts differ")
        self.outcomes["inclusive_time_bounds"] = "passed"
        self.run(self.archives[0], "empty", options=SearchOptions(extra=("--tge", "999999")))
        check(self.counts("empty") == {}, "empty search persisted counts")
        self.outcomes["empty_results"] = "passed"

    def check_missing_timestamps(self) -> None:
        """Record the core's existing epoch-zero treatment of missing timestamps."""
        archive = self.compress(
            [
                {"level": "error"},
                {"timestamp": "1970-01-01T00:00:01.000+00:00", "level": "error"},
            ],
            "missing-timestamp",
        )
        self.run(archive, "missing")
        observed = self.counts("missing")
        check(observed == {0: 1, 1000: 1}, "missing timestamp epoch-zero behavior changed")
        self.run(archive, "missing-bounded", options=SearchOptions(extra=("--tge", "1")))
        check(
            self.counts("missing-bounded") == {1000: 1}, "missing timestamp bound behavior changed"
        )
        self.outcomes["missing_timestamp_uses_epoch_zero"] = {
            "passed": True,
            "observed_counts": observed,
            "note": "Existing core behavior; a missing timestamp contributes to the zero bucket.",
        }

    def check_plain_count(self) -> None:
        """Check ordinary count aggregation retains its existing insert-only result format."""
        options = SearchOptions(aggregation=("--count",))
        for expected_documents in (1, 2):
            self.run(self.archives[0], "plain-count", options=options)
            documents = list(self.database["plain-count"].find())
            check(len(documents) == expected_documents, "ordinary count did not insert a document")
            for document in documents:
                check(
                    set(document) == {"_id", "archive_id", "count"}, "ordinary count schema changed"
                )
                check(isinstance(document["_id"], ObjectId), "ordinary count identity changed")
                check(document["count"] == len(TIMESTAMPS), "ordinary count differs")
                check(
                    document["archive_id"] == self.archives[0].identifier, "count archive differs"
                )
        self.outcomes["ordinary_count_retains_insert_semantics"] = "passed"

    def check_write_failures(self) -> None:
        """Check failure propagation and repair of partial or unacknowledged writes."""
        archive = self.archives[0]
        try:
            self.fault({"skip": 1}, {"errorCode": 11600})
            self.run(archive, "partial", expect_success=False)
        finally:
            self.fault("off", {})
        partial_count = self.database.partial.count_documents({})
        check(0 < partial_count < len(self.expected), f"unexpected partial count {partial_count}")
        self.run(archive, "partial")
        check(self.counts("partial") == self.expected, "retry did not repair partial writes")
        self.outcomes["partial_write_then_retry"] = {
            "passed": True,
            "persisted_before_retry": partial_count,
        }
        try:
            self.fault(
                {"times": 1},
                {"writeConcernError": {"code": 64, "errmsg": "injected acknowledgment failure"}},
            )
            self.run(archive, "ack", expect_success=False)
        finally:
            self.fault("off", {})
        check(self.database.ack.count_documents({}) > 0, "failed acknowledgment did not persist")
        self.run(archive, "ack")
        check(self.counts("ack") == self.expected, "retry after failed acknowledgment differs")
        self.outcomes["committed_write_with_failed_acknowledgment"] = "passed"
        self.run(
            archive,
            "unacknowledged",
            options=SearchOptions(uri_suffix="&w=0"),
            expect_success=False,
        )
        self.outcomes["unacknowledged_writes_rejected"] = "passed"

    def execute(self) -> None:
        """Run the correctness experiment and persist its report and timings."""
        try:
            self.check_replay()
            self.check_bounds()
            self.check_write_failures()
            self.check_missing_timestamps()
            self.check_plain_count()
            durations = [self.run(self.archives[0], "timing") for _ in range(6)]
            report = {
                "database": self.database_name,
                "binary": str(self.binary),
                "events_per_archive": len(TIMESTAMPS),
                "buckets_per_archive": len(self.expected),
                "expected_single_archive": self.expected,
                "cases": self.outcomes,
                "warm_write_seconds": durations[1:],
                "warm_write_median_seconds": median(durations[1:]),
            }
            rendered = json.dumps(report, indent=2) + "\n"
            (self.output / "results.json").write_text(rendered, encoding="utf-8")
            sys.stdout.write(rendered)
        finally:
            self.client.close()


def main() -> None:
    """Parse command-line options and execute the isolated experiment."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("build/core/clp-s"))
    parser.add_argument("--uri", default="mongodb://127.0.0.1:27028")
    parser.add_argument("--output", type=Path, default=Path("build/timeline-core-prototype"))
    args = parser.parse_args()
    Experiment(args.binary, args.uri, args.output).execute()


if __name__ == "__main__":
    main()
