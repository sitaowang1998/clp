//! Aggregation and validation of count-by-time results.

use mongodb::Collection;
use mongodb::bson::Bson;
use mongodb::bson::Document;
use mongodb::bson::doc;
use serde::Serialize;

mod error;

pub use error::Error;

/// Reserved document identity indicating that all final buckets have been published.
pub const COMMITTED_ID: &str = "__clp_timeline_committed_v1";

/// A validated timeline bucket with an integral timestamp and nonnegative count.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct Bucket {
    pub timestamp: i64,
    pub count: i64,
}

impl Bucket {
    /// Validates an aggregated bucket, including the validity of its source contributions.
    ///
    /// # Returns
    ///
    /// A bucket on success.
    ///
    /// # Errors
    ///
    /// * [`Error`] if the aggregate validity flag is missing, nonintegral, or nonzero.
    /// * Forwards [`Self::try_from_final`]'s errors.
    pub fn try_from_aggregate(document: &Document) -> Result<Self, Error> {
        if integer(document, "invalid")? != 0 {
            return Err(Error);
        }
        Self::try_from_final(document)
    }

    /// Validates a published bucket whose identity is its timestamp.
    ///
    /// # Returns
    ///
    /// A bucket on success.
    ///
    /// # Errors
    ///
    /// [`Error`] if either value is missing or nonintegral, or the count is negative.
    pub fn try_from_final(document: &Document) -> Result<Self, Error> {
        let bucket = Self {
            timestamp: integer(document, "_id")?,
            count: integer(document, "count")?,
        };
        if bucket.count < 0 {
            return Err(Error);
        }
        Ok(bucket)
    }
}

/// Combines source contributions, excluding already published buckets and the commit marker.
///
/// Integer identities are reserved for final buckets. Compact Spider contributions use object
/// identities; legacy Celery buckets use object IDs and a top-level timestamp. Invalid remaining
/// identities and operands are retained and flagged rather than silently discarded.
///
/// # Returns
///
/// A cursor of grouped, timestamp-ordered documents on success. Each document must be validated
/// with [`Bucket::try_from_aggregate`] before publication or use.
///
/// # Errors
///
/// Forwards [`mongodb::Collection::aggregate`]'s errors.
pub async fn aggregate(
    collection: Collection<Document>,
) -> mongodb::error::Result<mongodb::Cursor<Document>> {
    let pipeline = [
        doc! {
            "$match": { "$expr": { "$and": [
                { "$not": [{ "$in": [{ "$type": "$_id" }, ["int", "long"]] }] },
                { "$ne": ["$_id", COMMITTED_ID] },
            ] } },
        },
        doc! {
            "$project": {
                "timestamp": { "$cond": [
                    { "$eq": [{ "$type": "$_id" }, "object"] },
                    "$_id.timestamp",
                    "$timestamp",
                ] },
                "valid_identity": { "$in": [{ "$type": "$_id" }, ["object", "objectId"]] },
                "count": 1,
            },
        },
        doc! {
            "$group": {
                "_id": "$timestamp",
                "count": { "$sum": "$count" },
                "invalid": { "$max": { "$cond": [
                    { "$and": [
                        "$valid_identity",
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
    collection.aggregate(pipeline).await
}

/// Selects published timeline buckets by their reserved integral identity type.
#[must_use]
pub fn final_bucket_filter() -> Document {
    doc! { "_id": { "$type": ["int", "long"] } }
}

/// Reads a signed integer without accepting floating-point coercion.
///
/// # Returns
///
/// The integer on success.
///
/// # Errors
///
/// [`Error`] if the field is missing or does not contain a BSON integer.
fn integer(document: &Document, field: &str) -> Result<i64, Error> {
    match document.get(field) {
        Some(Bson::Int32(value)) => Ok(i64::from(*value)),
        Some(Bson::Int64(value)) => Ok(*value),
        _ => Err(Error),
    }
}

#[cfg(test)]
mod tests;
