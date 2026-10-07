//! Retrieval of complete count-by-time results from the results cache.

use futures::Stream;
use futures::StreamExt;
use mongodb::Collection;
use mongodb::bson::Bson;
use mongodb::bson::Document;
use mongodb::bson::doc;
use serde::Serialize;

use crate::error::ClientError;

/// Combines bucket contributions and returns complete buckets in timestamp order.
///
/// The caller must ensure the producing job has succeeded before invoking this function.
/// Legacy reducer output (one document per bucket) uses the same representation.
///
/// # Returns
///
/// A stream of ordinary JSON objects containing integer `timestamp` and `count` fields on success.
///
/// # Errors
///
/// * Forwards [`mongodb::Collection::aggregate`]'s errors on failure.
/// * Stream items forward cursor errors and [`serialize_bucket`]'s errors.
pub async fn fetch(
    collection: Collection<Document>,
) -> Result<impl Stream<Item = Result<String, ClientError>> + use<>, ClientError> {
    // MongoDB's $sum silently ignores nonnumeric inputs. Carry validity through the grouping
    // so corrupt contributions cannot quietly produce a plausible partial count.
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
    let cursor = collection.aggregate(pipeline).await?;
    Ok(cursor.map(|result| serialize_bucket(&result?)))
}

#[derive(Serialize)]
struct Bucket {
    timestamp: i64,
    count: i64,
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
