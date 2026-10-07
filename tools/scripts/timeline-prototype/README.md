# Spider timeline prototype harness

This harness submits real archive-search task graphs through `QueryJobSubmitter`, executes the CLP
TDL task in Spider, and compares the persisted histogram with a reference computed from raw events.
It bypasses the unfinished production coordinator's archive-planning loop. SQL job status is seeded
separately for the HTTP test; it is not evidence of production orchestration integration.

The [prototype report](https://app.notion.com/p/3f204e4d9e6b81b99c40d27f06e32296)
describes the implementation, findings, measurements, validation, and remaining production work.

## Prerequisites

- Build `build/core/clp-s` with the timeline changes.
- Build native Spider storage, scheduler, execution-manager, and task-executor binaries compatible
  with the Spider revision in the root Cargo manifest.
- Docker, Python 3, and `msgpack` (only for the HTTP verifier).
- Available local ports 53316, 55151, 55152, 55153, and 27028.

Build the CLP artifacts using the repository Rust toolchain:

```bash
source build/toolchains/rust/env
cargo build --release -p query-coordinator --example timeline
cargo build --release -p clp-tdl-package --lib
cargo build --release -p api-server --bin api_server
```

Provision isolated local services (the credentials are disposable test credentials):

```bash
docker run -d --name clp-timeline-prototype-spider-db \
  -p 127.0.0.1:53316:3306 \
  -e MARIADB_ROOT_PASSWORD=timeline-local -e MARIADB_DATABASE=timeline \
  -e MARIADB_USER=timeline -e MARIADB_PASSWORD=timeline-local mariadb:10.11.16
docker run -d --name clp-timeline-prototype-mongo \
  -p 127.0.0.1:27028:27017 mongo:8.0 --setParameter enableTestCommands=1
```

## Run and inspect

```bash
python3 tools/scripts/timeline-prototype/run.py --spider-bin-dir /path/to/spider/target/release
python3 tools/scripts/timeline-prototype/verify-api.py
```

Run the separate core publication and MongoDB failure cases with:

```bash
uv run --script tools/scripts/timeline-core-prototype.py
```

The default fixture has four independent archives containing 100,000 events each. Every third event
matches `level:error`; timestamps cover ten 10-second buckets. The manifest deliberately sets the
ordinary result limit to one, so exact timeline counts also verify that the limit is ignored.

`run.py` checks a baseline, complete replay into the same result collection, an actual task-executor
SIGKILL immediately after successful publication, and five subsequent warm baselines. It also
measures one discarded warmup and five sequential runs of the same core commands without Spider.
A temporary CLP
wrapper injects the one-shot executor crash; the production task runner, Spider retry, and real MongoDB
writes are used. This is an executor crash, not an execution-manager loss or a network partition.
The core binary and TDL library are copied before execution so concurrent builds cannot replace them.
These are warm filesystem-cache measurements on one worker slot, not a distributed scaling study or
a comparison with Celery. Spider timing includes graph submission and completion polling; the harness's
separate wall-clock timing also includes result verification through `docker exec mongosh`.

The API verifier starts the production API binary against the actual Spider result collection. It
checks SSE counts, rejects failed/cancelling/cancelled/killed jobs despite persisted partial results,
and verifies that a running request waits until SQL status transitions to success.

Artifacts are under `.tmp/timeline-spider`: fixture inputs, archives, generated configs, raw result
rows, service logs, submitter outputs, `evidence.json`, and `api-evidence.json`. The harness validates
counts, verifies the extra retry attempt in worker logs, and stops its native services on exit.
The checked-in `results/` directory preserves the measured prototype run's compact evidence.
`validation.json` records test outcomes and the repository Python lint and C++ tooling limitations.
The recorded Spider services came from source commit
`aabef3926f51a4f05354046edae329812b008d1b`, while the CLP client dependency pins `86cfba7`.
That combination passed these trials; matching client/service revisions remain unverified.
Each run resets only the `timeline_spider` MongoDB
database and `timeline_api.query_jobs` test table. The Docker containers remain available for inspection.

To remove only the prototype services:

```bash
docker rm -f clp-timeline-prototype-spider-db clp-timeline-prototype-mongo
```
