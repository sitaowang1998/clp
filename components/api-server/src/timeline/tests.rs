//! Tests for timeline result validation and MongoDB aggregation.

use futures::TryStreamExt;
use mongodb::bson::Bson;
use mongodb::bson::doc;

use super::CACHE_FIELD;
use super::Cache;
use super::read_cached;
use super::serialize_bucket;

#[test]
fn cached_json_is_normalized_for_sse() {
    assert_eq!(
        read_cached(&doc! {
            CACHE_FIELD: ["{\n  \"count\": 3,\n  \"timestamp\": 0\n}"],
        }),
        Some(vec![r#"{"timestamp":0,"count":3}"#.to_owned()])
    );
}

#[test]
fn serialize_plain_integer_json() -> anyhow::Result<()> {
    let result = serialize_bucket(&doc! {
        "_id": i64::MIN,
        "count": i64::MAX,
        "invalid": 0,
    })?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&result)?,
        serde_json::json!({ "timestamp": i64::MIN, "count": i64::MAX }),
    );
    Ok(())
}

#[test]
fn reject_invalid_aggregate() {
    for document in [
        doc! { "_id": 0, "count": 0, "invalid": 1 },
        doc! { "_id": 0, "count": 1e20, "invalid": 0 },
        doc! { "_id": "0", "count": 1, "invalid": 0 },
        doc! { "_id": 0, "count": -1, "invalid": 0 },
    ] {
        assert!(serialize_bucket(&document).is_err(), "{document:?}");
    }
}

/// Exercises the actual aggregation against a disposable collection in a live `MongoDB` server.
#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn aggregate_timeline_in_mongodb() -> anyhow::Result<()> {
    let Ok(uri) = std::env::var("CLP_TEST_MONGODB_URI") else {
        eprintln!("skipping MongoDB integration test: CLP_TEST_MONGODB_URI is unset");
        return Ok(());
    };
    let client = mongodb::Client::with_uri_str(uri).await?;
    let collection = client
        .database("clp_timeline_tests")
        .collection(&format!("timeline_{}", mongodb::bson::oid::ObjectId::new()));
    let cache = Cache::default();

    let empty: Vec<String> = cache.fetch(collection.clone()).await?.try_collect().await?;
    assert_eq!(empty, Vec::<String>::new());

    collection
        .insert_many([
            doc! { "timestamp": 1000_i64, "count": 2_i64, "archive_id": "a" },
            doc! { "timestamp": -1000_i64, "count": 4_i64, "archive_id": "a" },
            doc! { "timestamp": 1000_i64, "count": 3_i64, "archive_id": "b" },
            // Legacy reducer documents have no archive ID and often use BSON int32.
            doc! { "timestamp": 0, "count": 7 },
        ])
        .await?;
    let results: Vec<String> = cache.fetch(collection.clone()).await?.try_collect().await?;
    let actual: Vec<serde_json::Value> = results
        .iter()
        .map(|result| serde_json::from_str(result).expect("bucket JSON"))
        .collect();
    assert_eq!(
        actual,
        vec![
            serde_json::json!({ "timestamp": -1000, "count": 4 }),
            serde_json::json!({ "timestamp": 0, "count": 7 }),
            serde_json::json!({ "timestamp": 1000, "count": 5 }),
        ]
    );
    collection.drop().await?;

    for invalid in [
        doc! { "timestamp": 0, "count": "1" },
        doc! { "timestamp": 0, "count": 1.0 },
        doc! { "timestamp": 0, "count": -1 },
        doc! { "timestamp": 0 },
        doc! { "timestamp": 0, "count": Bson::Null },
        doc! { "timestamp": "0", "count": 1 },
        doc! { "count": 1 },
    ] {
        collection.insert_one(invalid.clone()).await?;
        let result = cache.fetch(collection.clone()).await;
        assert!(
            matches!(result, Err(crate::error::ClientError::MalformedData)),
            "accepted corrupt bucket: {invalid:?}"
        );
        collection.drop().await?;
    }

    collection
        .insert_many([
            doc! { "timestamp": 0, "count": i64::MAX },
            doc! { "timestamp": 0, "count": 1_i64 },
        ])
        .await?;
    assert!(matches!(
        cache.fetch(collection.clone()).await,
        Err(crate::error::ClientError::MalformedData)
    ));
    collection.drop().await?;
    Ok(())
}
