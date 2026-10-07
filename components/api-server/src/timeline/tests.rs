//! Tests for timeline result validation and MongoDB aggregation.

use futures::TryStreamExt;
use mongodb::bson::Bson;
use mongodb::bson::doc;

use super::fetch;
use super::serialize_bucket;

#[test]
fn serialize_plain_integer_json() -> anyhow::Result<()> {
    let result = serialize_bucket(
        &doc! {
            "_id": i64::MIN,
            "count": i64::MAX,
            "invalid": 0,
        },
        false,
    )?;
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
        assert!(serialize_bucket(&document, false).is_err(), "{document:?}");
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

    let empty: Vec<String> = fetch(collection.clone()).await?.try_collect().await?;
    assert_eq!(empty, Vec::<String>::new());

    collection
        .insert_many([
            doc! {
                "_id": { "dataset": "default", "archive_id": "a", "timestamp": 1000_i64 },
                "count": 2_i64,
            },
            doc! {
                "_id": { "dataset": "default", "archive_id": "a", "timestamp": -1000_i64 },
                "count": 4_i64,
            },
            doc! {
                "_id": { "dataset": "other", "archive_id": "a", "timestamp": 1000_i64 },
                "count": 3_i64,
                // Old prototype rows may retain duplicates; the identity is authoritative.
                "timestamp": 999_i64,
            },
            // Legacy reducer documents have no archive ID and often use BSON int32.
            doc! { "timestamp": 0, "count": 7 },
        ])
        .await?;
    let results: Vec<String> = fetch(collection.clone()).await?.try_collect().await?;
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
        doc! { "_id": { "timestamp": "0" }, "timestamp": 0, "count": 1 },
        doc! { "_id": { "timestamp": Bson::Null }, "timestamp": 0, "count": 1 },
        doc! { "_id": { "archive_id": "a" }, "timestamp": 0, "count": 1 },
        doc! { "_id": { "timestamp": [0] }, "count": 1 },
        doc! { "_id": { "timestamp": 0_i64 }, "count": -1 },
        doc! { "_id": { "timestamp": 0_i64 }, "count": "1" },
    ] {
        collection.insert_one(invalid.clone()).await?;
        let result = fetch(collection.clone())
            .await?
            .try_collect::<Vec<_>>()
            .await;
        assert!(
            matches!(result, Err(crate::error::ClientError::MalformedData)),
            "accepted corrupt bucket: {invalid:?}"
        );
        collection.drop().await?;
    }

    collection
        .insert_many([
            doc! { "_id": { "timestamp": 0_i64 }, "count": i64::MAX },
            doc! { "timestamp": 0, "count": 1_i64 },
        ])
        .await?;
    assert!(matches!(
        fetch(collection.clone())
            .await?
            .try_collect::<Vec<_>>()
            .await,
        Err(crate::error::ClientError::MalformedData)
    ));
    collection.drop().await?;
    Ok(())
}

/// Committed reads select indexed final buckets and never run an aggregation, even concurrently.
#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn committed_reads_do_not_aggregate_or_publish() -> anyhow::Result<()> {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use clp_rust_utils::timeline::COMMITTED_ID;
    use mongodb::event::EventHandler;
    use mongodb::event::command::CommandEvent;
    use mongodb::options::ClientOptions;

    let uri = std::env::var("CLP_TEST_MONGODB_URI")?;
    let aggregates = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let observed_aggregates = Arc::clone(&aggregates);
    let observed_writes = Arc::clone(&writes);
    let mut options = ClientOptions::parse(uri).await?;
    options.command_event_handler = Some(EventHandler::callback(move |event| {
        if let CommandEvent::Started(event) = event {
            if event.command_name == "aggregate" {
                observed_aggregates.fetch_add(1, Ordering::SeqCst);
            }
            if ["update", "insert", "delete", "findAndModify"]
                .contains(&event.command_name.as_str())
            {
                observed_writes.fetch_add(1, Ordering::SeqCst);
            }
        }
    }));
    let client = mongodb::Client::with_options(options)?;
    let collection = client.database("clp_timeline_tests").collection(&format!(
        "committed_{}",
        mongodb::bson::oid::ObjectId::new()
    ));
    collection
        .insert_many([
            // The API must not read or validate source contributions after successful publication.
            doc! { "_id": { "timestamp": 0_i64 }, "count": "not a final bucket" },
            doc! { "_id": 1000_i64, "count": 9_i64 },
            doc! { "_id": -1000_i64, "count": 7_i64 },
            doc! { "_id": COMMITTED_ID },
        ])
        .await?;
    writes.store(0, Ordering::SeqCst);
    let requests = (0..8).map(|_| async {
        fetch(collection.clone())
            .await?
            .try_collect::<Vec<_>>()
            .await
    });
    for buckets in futures::future::try_join_all(requests).await? {
        assert_eq!(
            buckets,
            [
                r#"{"timestamp":-1000,"count":7}"#,
                r#"{"timestamp":1000,"count":9}"#
            ]
        );
    }
    assert_eq!(aggregates.load(Ordering::SeqCst), 0);
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    collection
        .delete_many(doc! { "_id": { "$type": "long" } })
        .await?;
    let empty: Vec<String> = fetch(collection.clone()).await?.try_collect().await?;
    assert_eq!(empty, Vec::<String>::new());
    collection
        .insert_one(doc! { "_id": 0_i64, "count": -1_i64 })
        .await?;
    assert!(matches!(
        fetch(collection.clone())
            .await?
            .try_collect::<Vec<_>>()
            .await,
        Err(crate::error::ClientError::MalformedData)
    ));
    collection.drop().await?;
    Ok(())
}
