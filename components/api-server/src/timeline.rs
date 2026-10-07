//! Retrieval of committed timelines and aggregation of legacy contributions.

use clp_rust_utils::timeline::Bucket;
use clp_rust_utils::timeline::COMMITTED_ID;
use clp_rust_utils::timeline::aggregate;
use clp_rust_utils::timeline::final_bucket_filter;
use futures::StreamExt;
use futures::stream::BoxStream;
use mongodb::Collection;
use mongodb::bson::Document;
use mongodb::bson::doc;

use crate::error::ClientError;

/// Streams completed buckets in timestamp order without reducing committed results again.
///
/// The caller must check job success before calling this function. A commit marker selects
/// final buckets, stored under integer IDs independently of archive contributions. Older Spider
/// jobs and Celery reducer output retain the aggregation fallback. Reads never publish data.
///
/// # Returns
///
/// A stream of ordinary JSON buckets on success.
///
/// # Errors
///
/// * Forwards [`Collection::find_one`], [`Collection::find`], and [`aggregate`]'s errors.
/// * Stream items forward cursor errors and [`serialize_bucket`]'s validation errors.
pub async fn fetch(
    collection: Collection<Document>,
) -> Result<BoxStream<'static, Result<String, ClientError>>, ClientError> {
    let committed = collection
        .find_one(doc! { "_id": COMMITTED_ID })
        .await?
        .is_some();
    let cursor = if committed {
        collection
            .find(final_bucket_filter())
            .sort(doc! { "_id": 1 })
            .await?
    } else {
        aggregate(collection).await?
    };
    Ok(cursor
        .map(move |result| serialize_bucket(&result?, committed))
        .boxed())
}

/// Serializes a validated bucket without exposing BSON wrappers or internal metadata.
///
/// # Returns
///
/// A JSON bucket on success.
///
/// # Errors
///
/// * [`ClientError::MalformedData`] if the bucket is malformed or its aggregate overflowed.
/// * Forwards [`serde_json::to_string`]'s errors.
fn serialize_bucket(document: &Document, committed: bool) -> Result<String, ClientError> {
    let bucket = if committed {
        Bucket::try_from_final(document)
    } else {
        Bucket::try_from_aggregate(document)
    }
    .map_err(|_| ClientError::MalformedData)?;
    Ok(serde_json::to_string(&bucket)?)
}

#[cfg(test)]
mod tests;
