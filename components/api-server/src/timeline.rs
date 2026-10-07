//! Retrieval and lazy caching of complete count-by-time results.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;

use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::BoxStream;
use mongodb::Collection;
use mongodb::bson::Bson;
use mongodb::bson::Document;
use mongodb::bson::doc;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::error::ClientError;

/// Persists complete timelines and coalesces concurrent cache misses within an API client.
///
/// Client clones must share this instance. Other API replicas share persisted cache entries but
/// may independently compute the same immutable histogram on simultaneous misses.
pub struct Cache {
    fills: Mutex<HashMap<String, Weak<Mutex<()>>>>,
    max_bytes: usize,
}

impl Cache {
    /// Returns completed timeline buckets, lazily caching a fully validated histogram.
    ///
    /// The caller must check that the query succeeded, including for cache hits. The cache lives
    /// on an existing contribution so collection retention removes it without separate cleanup.
    /// Oversized timelines stream without caching. Failed publication does not fail valid results.
    ///
    /// # Returns
    ///
    /// A stream of ordinary JSON buckets in timestamp order on success.
    ///
    /// # Errors
    ///
    /// * Forwards [`read_anchor`], [`aggregate`], and [`serialize_bucket`]'s errors.
    /// * Stream items forward cursor and [`serialize_bucket`]'s errors for oversized timelines.
    pub async fn fetch(
        &self,
        collection: Collection<Document>,
    ) -> Result<BoxStream<'static, Result<String, ClientError>>, ClientError> {
        let Some(anchor) = read_anchor(collection.clone()).await? else {
            return Ok(futures::stream::empty().boxed());
        };
        if let Some(buckets) = read_cached(&anchor) {
            return Ok(futures::stream::iter(buckets.into_iter().map(Ok)).boxed());
        }

        let lock = {
            let mut fills = self.fills.lock().await;
            // Weak values alone would leave one namespace key per historical query.
            fills.retain(|_, lock| lock.strong_count() > 0);
            let entry = fills.entry(collection.namespace().to_string()).or_default();
            let lock = entry.upgrade().unwrap_or_else(|| {
                let lock = Arc::new(Mutex::new(()));
                *entry = Arc::downgrade(&lock);
                lock
            });
            drop(fills);
            lock
        };
        let _guard = lock.lock().await;
        // Another request may have populated the cache while this request waited.
        let Some(anchor) = read_anchor(collection.clone()).await? else {
            return Ok(futures::stream::empty().boxed());
        };
        if let Some(buckets) = read_cached(&anchor) {
            return Ok(futures::stream::iter(buckets.into_iter().map(Ok)).boxed());
        }

        let mut cursor = aggregate(collection.clone()).await?;
        let mut buckets = Vec::new();
        let mut bytes = 0_usize;
        while let Some(document) = cursor.try_next().await? {
            let bucket = serialize_bucket(&document)?;
            // A BSON array string has a length prefix, terminator, type byte, and decimal index.
            // 32 bytes exceeds that overhead for any array fitting the 8 MiB cache budget.
            bytes = bytes.saturating_add(bucket.len()).saturating_add(32);
            buckets.push(bucket);
            if bytes > self.max_bytes {
                // Drop the fill guard on return; this cursor belongs to the requesting stream.
                return Ok(futures::stream::iter(buckets.into_iter().map(Ok))
                    .chain(cursor.map(|result| serialize_bucket(&result?)))
                    .boxed());
            }
        }
        if let Some(id) = anchor.get("_id") {
            let cache = doc! { CACHE_FIELD: &buckets };
            // Updating an existing source row without upsert cannot resurrect an expired
            // collection. Late identical worker replacements may evict this optional cache.
            if let Err(error) = collection
                .update_one(doc! { "_id": id.clone() }, doc! { "$set": cache })
                .await
            {
                tracing::warn!(error = % error, "Failed to cache timeline results.");
            }
        }
        Ok(futures::stream::iter(buckets.into_iter().map(Ok)).boxed())
    }
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            fills: Mutex::default(),
            // Leave ample room for the original bucket and BSON envelope below MongoDB's
            // document limit. Publication remains optional if the source row is unusually large.
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

const CACHE_FIELD: &str = "__clp_timeline_cache_v1";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Bucket {
    timestamp: i64,
    count: i64,
}

/// Finds the first source document using the existing unique ID index.
///
/// # Returns
///
/// Its ID and optional cached histogram, or `None` for an empty collection, on success.
///
/// # Errors
///
/// Forwards [`mongodb::Collection::find_one`]'s errors.
async fn read_anchor(collection: Collection<Document>) -> Result<Option<Document>, ClientError> {
    Ok(collection
        .find_one(doc! {})
        .sort(doc! { "_id": 1 })
        .projection(doc! { "_id": 1, CACHE_FIELD: 1 })
        .await?)
}

/// Returns a structurally valid cached histogram, treating malformed cache data as a miss.
fn read_cached(document: &Document) -> Option<Vec<String>> {
    let values = document.get_array(CACHE_FIELD).ok()?;
    // An anchor is itself a contribution, so a valid histogram cannot be empty.
    if values.is_empty() {
        return None;
    }
    let mut previous = None;
    let mut buckets = Vec::with_capacity(values.len());
    for value in values {
        let serialized = value.as_str()?;
        let bucket: Bucket = serde_json::from_str(serialized).ok()?;
        if bucket.count < 0 || previous.is_some_and(|timestamp| timestamp >= bucket.timestamp) {
            return None;
        }
        previous = Some(bucket.timestamp);
        // Normalize whitespace as SSE data must stay on one line, even for a repaired cache.
        buckets.push(serde_json::to_string(&bucket).ok()?);
    }
    Some(buckets)
}

/// Combines contributions while tracking malformed operands that MongoDB's sum would ignore.
///
/// # Returns
///
/// A cursor of grouped, timestamp-ordered documents on success.
///
/// # Errors
///
/// Forwards [`mongodb::Collection::aggregate`]'s errors.
async fn aggregate(
    collection: Collection<Document>,
) -> Result<mongodb::Cursor<Document>, ClientError> {
    let pipeline = [
        doc! {
            "$group": {
                "_id": "$timestamp",
                "count": { "$sum": "$count" },
                "invalid": { "$max": { "$cond": [
                    { "$and": [
                        { "$in": [{ "$type": "$timestamp" }, ["int", "long"]] },
                        { "$in": [{ "$type": "$count" }, ["int", "long"]] },
                        { "$gte": ["$count", 0] },
                    ] },
                    0,
                    1,
                ] } },
            },
        },
        doc! { "$sort": { "_id": 1 } },
    ];
    Ok(collection.aggregate(pipeline).await?)
}

/// Serializes a validated aggregate without exposing BSON extended JSON or internal fields.
///
/// # Returns
///
/// A JSON bucket on success.
///
/// # Errors
///
/// * [`ClientError::MalformedData`] if a contribution was invalid, or if the sum overflowed signed
///   64-bit integers (which `MongoDB` promotes to a double).
/// * Forwards [`serde_json::to_string`]'s errors on failure.
fn serialize_bucket(document: &Document) -> Result<String, ClientError> {
    let integer = |field| match document.get(field) {
        Some(Bson::Int32(value)) => Ok(i64::from(*value)),
        Some(Bson::Int64(value)) => Ok(*value),
        _ => Err(ClientError::MalformedData),
    };
    if integer("invalid")? != 0 {
        return Err(ClientError::MalformedData);
    }
    let bucket = Bucket {
        timestamp: integer("_id")?,
        count: integer("count")?,
    };
    if bucket.count < 0 {
        return Err(ClientError::MalformedData);
    }
    Ok(serde_json::to_string(&bucket)?)
}

#[cfg(test)]
mod tests;
