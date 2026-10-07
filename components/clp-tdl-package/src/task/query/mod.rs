//! The query tasks: the `#[task]` wrappers Spider invokes and their implementations.

use clp_rust_utils::job_config::QueryJobId;
use clp_rust_utils::task_io::query::ClpSQueryOption;
use clp_rust_utils::task_io::query::OutputHandle;
use clp_rust_utils::task_io::query::TimelineTaskOutput;
use clp_rust_utils::types::ArchiveId;
use non_empty_string::NonEmptyString;
use spider_tdl::TaskContext;
use spider_tdl::TdlError;
use spider_tdl::task;

mod commit;
mod search;

#[task(name = "query::clp_s_search")]
pub(crate) fn clp_s_search_task(
    ctx: TaskContext,
    query_job_id: QueryJobId,
    clp_s_query_option: ClpSQueryOption,
    dataset: Option<NonEmptyString>,
    archive_id: ArchiveId,
    output_handle: OutputHandle,
) -> Result<(), TdlError> {
    search::search(
        &ctx,
        crate::common::spider_task_executor_config(),
        query_job_id,
        &clp_s_query_option,
        archive_id,
        dataset.as_ref().map(NonEmptyString::as_str),
        &output_handle,
    )
    .map_err(|e| TdlError::ExecutionError(format!("{e:#}")))
}

#[task(name = "query::clp_s_timeline_search")]
pub(crate) fn clp_s_timeline_search_task(
    ctx: TaskContext,
    query_job_id: QueryJobId,
    clp_s_query_option: ClpSQueryOption,
    dataset: Option<NonEmptyString>,
    archive_id: ArchiveId,
    output_handle: OutputHandle,
) -> Result<TimelineTaskOutput, TdlError> {
    if clp_s_query_option
        .count_by_time_bucket_size_millisecs
        .is_none()
    {
        return Err(TdlError::ExecutionError(
            "timeline task requires a bucket width".to_owned(),
        ));
    }
    search::search(
        &ctx,
        crate::common::spider_task_executor_config(),
        query_job_id,
        &clp_s_query_option,
        archive_id,
        dataset.as_ref().map(NonEmptyString::as_str),
        &output_handle,
    )
    .map_err(|error| TdlError::ExecutionError(format!("{error:#}")))?;
    Ok(TimelineTaskOutput {
        query_job_id,
        output_handle,
    })
}

#[task(name = "query::commit_timeline")]
pub(crate) fn commit_timeline_task(ctx: TaskContext) -> Result<(), TdlError> {
    let outputs = ctx.get_task_graph_outputs()?.ok_or_else(|| {
        TdlError::ExecutionError("timeline commit requires task graph outputs".to_owned())
    })?;
    let outputs = outputs
        .iter()
        .map(|output| rmp_serde::from_slice(output))
        .collect::<Result<Vec<TimelineTaskOutput>, _>>()
        .map_err(|error| TdlError::DeserializationError(error.to_string()))?;
    crate::common::runtime()
        .block_on(commit::commit(outputs))
        .map_err(|error| TdlError::ExecutionError(format!("{error:#}")))
}
