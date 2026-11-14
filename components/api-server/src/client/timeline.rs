//! Retrieval of committed timeline results.

use async_stream::try_stream;
use clp_rust_utils::timeline;
use clp_rust_utils::timeline::Bucket;
use futures::Stream;
use futures::TryStreamExt;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::options::DatabaseOptions;
use mongodb::options::Hint;
use mongodb::options::ReadPreference;
use mongodb::options::SelectionCriteria;

use super::Client;
use super::ClientError;

impl Client {
    pub(super) async fn fetch_timeline(
        &self,
        query_job_id: u64,
    ) -> Result<impl Stream<Item = Result<String, ClientError>> + use<>, ClientError> {
        let started = std::time::Instant::now();
        let database = self.mongodb_client.database_with_options(
            &self.config.results_cache.db_name,
            DatabaseOptions::builder()
                .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
                .build(),
        );
        let timeline_name = timeline::collection_name(
            query_job_id
                .try_into()
                .map_err(|_| ClientError::MalformedData)?,
        );
        let collection = database.collection::<Document>(&timeline_name);
        collection
            .find_one(doc! { "_id": timeline::COMMIT_MARKER_ID })
            .await?
            .ok_or(ClientError::MalformedData)?;
        Ok(try_stream! {
            let mut num_buckets = 0_u64;
            let mut cursor = collection
                .find(doc! { "_id": { "$type": "number" } })
                .sort(doc! { "_id": 1 })
                .hint(Hint::Name("_id_".to_owned()))
                .await?;
            while let Some(document) = cursor.try_next().await? {
                let bucket = Bucket::try_from_committed_document(&document)
                    .map_err(ClientError::from)?;
                yield serialize_bucket(bucket.timestamp(), bucket.count())?;
                num_buckets += 1;
            }
            tracing::debug!(
                query_job_id,
                num_buckets,
                elapsed_micros = started.elapsed().as_micros(),
                "Finished reading timeline results."
            );
        })
    }
}

fn serialize_bucket(timestamp: i64, count: i64) -> Result<String, ClientError> {
    const MAX_SAFE_INTEGER: i64 = (1_i64 << 53) - 1;
    if !(-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&timestamp) || count > MAX_SAFE_INTEGER {
        return Err(ClientError::MalformedData);
    }
    Ok(serde_json::to_string(&serde_json::json!({
        "timestamp": timestamp,
        "count": count,
    }))?)
}
