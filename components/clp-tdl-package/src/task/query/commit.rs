//! Publication of completed timeline histograms.

use std::time::Instant;

use anyhow::Context;
use clp_rust_utils::task_io::query::OutputHandle;
use clp_rust_utils::task_io::query::TimelineTaskOutput;
use clp_rust_utils::timeline::Bucket;
use clp_rust_utils::timeline::COMMITTED_ID;
use clp_rust_utils::timeline::aggregate;
use futures::TryStreamExt;
use mongodb::Client;
use mongodb::Collection;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::options::Acknowledgment;
use mongodb::options::ClientOptions;
use mongodb::options::ReadPreference;
use mongodb::options::ReplaceOneModel;
use mongodb::options::SelectionCriteria;

/// Reduces successful archive contributions and publishes the completed histogram.
///
/// Reads use the primary so a secondary cannot omit acknowledged archive contributions.
///
/// # Errors
///
/// Returns an error for absent or inconsistent outputs, unsupported output destinations,
/// a missing URI database, or unacknowledged writes. Forwards [`ClientOptions::parse`],
/// [`Client::with_options`], and [`publish`]'s errors.
pub(super) async fn commit(outputs: Vec<TimelineTaskOutput>) -> anyhow::Result<()> {
    let output = outputs
        .first()
        .context("timeline commit received no task outputs")?;
    if outputs.iter().any(|candidate| candidate != output) {
        anyhow::bail!("timeline task outputs have inconsistent result destinations");
    }
    let OutputHandle::ResultsCache { uri } = &output.output_handle else {
        anyhow::bail!("timeline commit requires results-cache output");
    };
    let started = Instant::now();
    tracing::info!(
        query_job_id = output.query_job_id,
        "Started timeline commit."
    );
    let mut options = ClientOptions::parse(uri.as_str()).await?;
    options.selection_criteria = Some(SelectionCriteria::ReadPreference(ReadPreference::Primary));
    options.app_name = Some("clp-timeline-commit".to_owned());
    let database = options
        .default_database
        .clone()
        .context("timeline URI must name a database")?;
    if options
        .write_concern
        .as_ref()
        .is_some_and(|concern| concern.w == Some(Acknowledgment::Nodes(0)))
    {
        anyhow::bail!("timeline commit requires acknowledged writes");
    }
    let client = Client::with_options(options)?;
    let collection = client
        .database(&database)
        .collection(&output.query_job_id.to_string());
    publish(&client, collection).await?;
    tracing::info!(
        query_job_id = output.query_job_id,
        elapsed_seconds = started.elapsed().as_secs_f64(),
        "Completed timeline commit."
    );
    Ok(())
}

/// Writes validated buckets in bounded batches, then publishes the completion marker.
///
/// Source contributions are retained. Numeric bucket IDs cannot collide with their object IDs,
/// and replacement upserts make publication safe to repeat after acknowledgement loss. The
/// query and its archive inputs must remain immutable throughout retries.
///
/// # Errors
///
/// Forwards [`aggregate`], [`TryStreamExt::try_next`], [`Bucket::try_from_aggregate`],
/// [`Client::bulk_write`], and [`Collection::replace_one`]'s errors.
async fn publish(client: &Client, collection: Collection<Document>) -> anyhow::Result<()> {
    const BATCH_SIZE: usize = 1_000;
    let mut cursor = aggregate(collection.clone()).await?;
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    while let Some(document) = cursor.try_next().await? {
        let bucket = Bucket::try_from_aggregate(&document)?;
        batch.push(
            ReplaceOneModel::builder()
                .namespace(collection.namespace())
                .filter(doc! { "_id": bucket.timestamp })
                .replacement(doc! { "_id": bucket.timestamp, "count": bucket.count })
                .upsert(true)
                .build(),
        );
        if batch.len() == BATCH_SIZE {
            client.bulk_write(std::mem::take(&mut batch)).await?;
        }
    }
    if !batch.is_empty() {
        client.bulk_write(batch).await?;
    }
    collection
        .replace_one(doc! { "_id": COMMITTED_ID }, doc! { "_id": COMMITTED_ID })
        .upsert(true)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
