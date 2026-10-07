//! Submits explicitly selected archives to Spider for reproducible timeline experiments.

use std::error::Error;
use std::time::Duration;
use std::time::Instant;

use clp_rust_utils::job_config::SearchJobConfig;
use clp_rust_utils::task_io::query::ClpSQueryOption;
use clp_rust_utils::task_io::query::OutputHandle;
use clp_rust_utils::types::ArchiveId;
use non_empty_string::NonEmptyString;
use query_coordinator::query_job_submitter::ArchiveMetadata;
use query_coordinator::query_job_submitter::QueryJobOutcome;
use query_coordinator::query_job_submitter::QueryJobSubmitter;
use serde::Deserialize;
use spider_client::SpiderClient;
use spider_core::task::ExecutionPolicy;
use spider_core::types::resource_group::ExternalResourceGroupCredentials;

#[derive(Deserialize)]
struct Experiment {
    storage_endpoint: String,
    mongo_uri: NonEmptyString,
    query_job_id: i32,
    search: SearchJobConfig,
    archives: Vec<Archive>,
}

#[derive(Deserialize)]
struct Archive {
    id: ArchiveId,
    dataset: Option<NonEmptyString>,
}

/// Runs one experiment from its JSON configuration and prints the terminal outcome and timing.
///
/// # Errors
///
/// Returns an error if configuration loading, validation, submission, or execution fails.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("expected experiment JSON path")?;
    let experiment: Experiment = serde_json::from_slice(&std::fs::read(path)?)?;
    let option = ClpSQueryOption::try_from(&experiment.search)?;
    let client = SpiderClient::builder(experiment.storage_endpoint.parse()?)
        .connect()
        .await?;
    let resource_group = client
        .add_or_verify_resource_group(ExternalResourceGroupCredentials::new(
            "timeline-prototype".to_owned(),
            b"timeline-prototype-local-only".to_vec(),
        ))
        .await?;
    let started = Instant::now();
    let job = client
        .submit_query_job(
            experiment.query_job_id,
            resource_group,
            option,
            OutputHandle::ResultsCache {
                uri: experiment.mongo_uri,
            },
            experiment
                .archives
                .into_iter()
                .map(|archive| {
                    (
                        ArchiveMetadata {
                            id: archive.id,
                            dataset: archive.dataset,
                            size: 0,
                        },
                        ExecutionPolicy {
                            max_num_retry: 3,
                            ..ExecutionPolicy::default()
                        },
                    )
                })
                .collect(),
        )
        .await?;
    println!("submitted_spider_job={job}");
    let outcome = client
        .run_query_job_to_completion(job, Duration::from_millis(20))
        .await?;
    println!(
        "{}",
        serde_json::json!({
            "spider_job_id": job.to_string(),
            "query_job_id": experiment.query_job_id,
            "outcome": outcome,
            "elapsed_millis": started.elapsed().as_millis(),
        })
    );
    if outcome != QueryJobOutcome::Succeeded {
        return Err(format!("query did not succeed: {outcome:?}").into());
    }
    Ok(())
}
