//! Validation and live aggregation regression tests.

use futures::TryStreamExt;
use mongodb::bson::Bson;
use mongodb::bson::doc;

use super::Bucket;
use super::COMMITTED_ID;
use super::aggregate;

#[test]
fn validate_final_buckets() -> anyhow::Result<()> {
    assert_eq!(
        Bucket::try_from_final(&doc! { "_id": i64::MIN, "count": i64::MAX })?,
        Bucket {
            timestamp: i64::MIN,
            count: i64::MAX,
        }
    );
    for document in [
        doc! { "_id": 0, "count": -1 },
        doc! { "_id": "0", "count": 1 },
        doc! { "_id": 0, "count": 1.0 },
        doc! { "_id": 0 },
    ] {
        assert!(Bucket::try_from_final(&document).is_err(), "{document:?}");
    }
    Ok(())
}

#[test]
fn reject_invalid_aggregate_flags() {
    for document in [
        doc! { "_id": 0, "count": 1 },
        doc! { "_id": 0, "count": 1, "invalid": 1 },
        doc! { "_id": 0, "count": 1, "invalid": 0.0 },
    ] {
        assert!(
            Bucket::try_from_aggregate(&document).is_err(),
            "{document:?}"
        );
    }
}

/// Exercises source selection, source validation, and overflow against a live server.
#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn aggregate_sources_in_mongodb() -> anyhow::Result<()> {
    let uri = std::env::var("CLP_TEST_MONGODB_URI")?;
    let client = mongodb::Client::with_uri_str(uri).await?;
    let collection = client
        .database("clp_timeline_tests")
        .collection(&format!("shared_{}", mongodb::bson::oid::ObjectId::new()));
    collection
        .insert_many([
            doc! { "_id": { "archive_id": "a", "timestamp": 0_i64 }, "count": 2_i64 },
            doc! {
                "_id": { "archive_id": "b", "timestamp": 0_i64 },
                "timestamp": 999_i64,
                "count": 3_i64,
            },
            doc! { "timestamp": -1000, "count": 7 },
        ])
        .await?;
    let expected = vec![
        Bucket {
            timestamp: -1000,
            count: 7,
        },
        Bucket {
            timestamp: 0,
            count: 5,
        },
    ];
    for published in [false, true] {
        if published {
            collection
                .insert_many([
                    doc! { "_id": -1000_i64, "count": 7_i64 },
                    doc! { "_id": 0_i64, "count": 5_i64 },
                    doc! { "_id": COMMITTED_ID, "count": 2_i64 },
                ])
                .await?;
        }
        let documents: Vec<_> = aggregate(collection.clone()).await?.try_collect().await?;
        let actual = documents
            .iter()
            .map(Bucket::try_from_aggregate)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(actual, expected, "published: {published}");
    }
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
        doc! { "_id": "malformed", "timestamp": 0, "count": 1 },
        doc! { "_id": 1.5, "timestamp": 0, "count": 1 },
    ] {
        collection.insert_one(invalid.clone()).await?;
        let documents: Vec<_> = aggregate(collection.clone()).await?.try_collect().await?;
        assert!(
            documents
                .iter()
                .any(|document| Bucket::try_from_aggregate(document).is_err()),
            "accepted corrupt contribution: {invalid:?}"
        );
        collection.drop().await?;
    }
    collection
        .insert_many([
            doc! { "_id": { "timestamp": 0_i64 }, "count": i64::MAX },
            doc! { "timestamp": 0, "count": 1_i64 },
        ])
        .await?;
    let documents: Vec<_> = aggregate(collection.clone()).await?.try_collect().await?;
    assert_eq!(documents.len(), 1);
    assert!(Bucket::try_from_aggregate(&documents[0]).is_err());
    collection.drop().await?;
    Ok(())
}
