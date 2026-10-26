"""Tests compression backend ownership and CLI completion reporting."""

from unittest.mock import Mock, patch

import pytest
from clp_package_utils.general import generate_docker_compose_container_config
from clp_package_utils.scripts.native.compress import print_compression_job_status
from clp_py_utils.clp_config import ClpConfig
from clp_py_utils.zstd_msgpack import serialize
from job_orchestration.scheduler.compress import compression_scheduler
from job_orchestration.scheduler.constants import CompressionJobStatus


@pytest.mark.parametrize("backend", ["celery", "spider"])
def test_compose_config_only_enables_deployed_compression_coordinator(backend: str) -> None:
    """Unused coordinator settings must not prevent Celery from accepting compression jobs."""
    config = ClpConfig.model_validate(
        {
            "package": {"storage_engine": "clp-s", "scheduler": backend},
            "spider": {},
            "compression_coordinator": {},
            "query_coordinator": {},
        }
    )

    container_config = generate_docker_compose_container_config(config)

    assert (container_config.compression_coordinator is not None) == (backend == "spider")
    assert config.compression_coordinator is not None


@pytest.mark.parametrize("coordinator_enabled", [False, True])
@pytest.mark.parametrize("input_type", ["fs", "s3", "s3_object_metadata"])
def test_scheduler_routes_only_supported_inputs_to_deployed_coordinator(
    coordinator_enabled: bool, input_type: str
) -> None:
    """Filesystem and ingestion inputs yield to Rust, while direct S3 remains with Celery."""
    config = ClpConfig.model_validate(
        {
            "package": {"storage_engine": "clp-s"},
            "spider": {} if coordinator_enabled else None,
            "compression_coordinator": {} if coordinator_enabled else None,
        }
    )
    input_config: dict[str, object] = {
        "type": input_type,
        "dataset": "default",
        "paths_to_compress": ["/mnt/logs/app.json"],
        "bucket": "logs",
        "key_prefix": "app/",
        "aws_authentication": {"type": "default"},
        "ingestion_job_id": 1,
        "s3_object_metadata_ids": [1],
    }
    job = {
        "id": 1,
        "clp_config": serialize(
            {"input": input_config, "output": config.archive_output.model_dump()}
        ),
    }

    with (
        patch.object(compression_scheduler, "_process_fs_input_paths", return_value=[]) as fs,
        patch.object(compression_scheduler, "_process_s3_input") as s3,
        patch.object(compression_scheduler, "_process_s3_object_metadata_input") as metadata,
        patch.object(compression_scheduler, "_ensure_dataset_exists"),
        patch.object(compression_scheduler, "_batch_and_submit_tasks") as submit,
    ):
        compression_scheduler._schedule_job(  # noqa: SLF001
            config, {"table_prefix": "clp_"}, Mock(), Mock(), job, set()
        )

    expected_dispatch = not coordinator_enabled or input_type == "s3"
    assert submit.called == expected_dispatch
    assert fs.called == (expected_dispatch and input_type == "fs")
    assert s3.called == (expected_dispatch and input_type == "s3")
    assert metadata.called == (expected_dispatch and input_type == "s3_object_metadata")


@pytest.mark.parametrize("duration", [0.0, None])
def test_empty_successful_job_reports_zero_sizes_without_dividing_by_zero(
    duration: float | None,
) -> None:
    """An empty successful job must not turn CLI completion into an exception."""
    with patch("clp_package_utils.scripts.native.compress.logger") as logger:
        print_compression_job_status(  # type: ignore[no-untyped-call]
            {
                "status": CompressionJobStatus.SUCCEEDED,
                "uncompressed_size": 0,
                "compressed_size": 0,
                "duration": duration,
            }
        )

    logger.info.assert_called_once()
    assert "(0.00x)" in logger.info.call_args.args[0]
