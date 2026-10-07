//! Commit publication tests against a disposable MongoDB collection.

use std::sync::Arc;
use std::sync::Mutex;

use clp_rust_utils::task_io::query::OutputHandle;
use clp_rust_utils::task_io::query::TimelineTaskOutput;
use clp_rust_utils::timeline::COMMITTED_ID;
use clp_rust_utils::timeline::final_bucket_filter;
use futures::TryStreamExt;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::event::EventHandler;
use mongodb::event::command::CommandEvent;
use mongodb::options::Acknowledgment;
use mongodb::options::ClientOptions;
use mongodb::options::WriteConcern;
use non_empty_string::NonEmptyString;

use super::commit;
use super::publish;

#[tokio::test]
async fn reject_invalid_commit_destinations() -> anyhow::Result<()> {
    assert!(commit(vec![]).await.is_err());
    let base = TimelineTaskOutput {
        query_job_id: 1,
        output_handle: OutputHandle::File,
    };
    assert!(commit(vec![base.clone()]).await.is_err());
    assert!(
        commit(vec![
            base.clone(),
            TimelineTaskOutput {
                query_job_id: 2,
                ..base
            }
        ])
        .await
        .is_err()
    );
    for uri in [
        "mongodb://localhost:27017",
        "mongodb://localhost:27017/test?w=0",
    ] {
        assert!(
            commit(vec![TimelineTaskOutput {
                query_job_id: 1,
                output_handle: OutputHandle::ResultsCache {
                    uri: NonEmptyString::try_from(uri.to_owned()).expect("test URI is nonempty")
                },
            }])
            .await
            .is_err()
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI and MongoDB 8.0"]
async fn publish_retries_partial_writes_and_marks_empty_results() -> anyhow::Result<()> {
    let uri = std::env::var("CLP_TEST_MONGODB_URI")?;
    let client = mongodb::Client::with_uri_str(uri).await?;
    let collection = client
        .database("clp_timeline_tests")
        .collection::<Document>(&format!("commit_{}", mongodb::bson::oid::ObjectId::new()));
    let result = async {
        // Put the invalid bucket after a full batch to prove a failed publication may have
        // partial final rows but must not have a completion marker.
        collection
            .insert_many((0_i64..1_001).map(|timestamp| {
                doc! {
                    "_id": { "dataset": "default", "archive_id": "a", "timestamp": timestamp },
                    "count": if timestamp == 1_000 { -1_i64 } else { 1_i64 },
                }
            }))
            .await?;
        assert!(publish(&client, collection.clone()).await.is_err());
        assert_eq!(
            collection.find_one(doc! { "_id": COMMITTED_ID }).await?,
            None
        );
        assert_eq!(
            collection.count_documents(final_bucket_filter()).await?,
            1_000
        );
        collection
            .update_one(
                doc! { "_id.timestamp": 1_000_i64 },
                doc! { "$set": { "count": 2_i64 } },
            )
            .await?;
        publish(&client, collection.clone()).await?;
        let first: Vec<Document> = collection
            .find(final_bucket_filter())
            .sort(doc! { "_id": 1 })
            .await?
            .try_collect()
            .await?;
        assert_eq!(first.len(), 1_001);
        assert_eq!(
            first.last(),
            Some(&doc! { "_id": 1_000_i64, "count": 2_i64 })
        );
        assert!(
            collection
                .find_one(doc! { "_id": COMMITTED_ID })
                .await?
                .is_some()
        );
        publish(&client, collection.clone()).await?;
        let second: Vec<Document> = collection
            .find(final_bucket_filter())
            .sort(doc! { "_id": 1 })
            .await?
            .try_collect()
            .await?;
        assert_eq!(first, second);
        assert_eq!(collection.count_documents(doc! {}).await?, 2_003);
        collection.delete_many(doc! {}).await?;
        publish(&client, collection.clone()).await?;
        assert_eq!(collection.count_documents(doc! {}).await?, 1);
        assert!(
            collection
                .find_one(doc! { "_id": COMMITTED_ID })
                .await?
                .is_some()
        );
        anyhow::Ok(())
    }
    .await;
    collection.drop().await?;
    result
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI and MongoDB 8.0"]
async fn malformed_and_overflowing_contributions_do_not_publish() -> anyhow::Result<()> {
    let uri = std::env::var("CLP_TEST_MONGODB_URI")?;
    let client = mongodb::Client::with_uri_str(uri).await?;
    let collection = client
        .database("clp_timeline_tests")
        .collection::<Document>(&format!(
            "commit_invalid_{}",
            mongodb::bson::oid::ObjectId::new()
        ));
    let result = async {
        for rows in [
            vec![doc! { "_id": { "archive_id": "a" }, "timestamp": 0_i64, "count": 1_i64 }],
            vec![
                doc! { "_id": { "archive_id": "a", "timestamp": 0_i64 }, "count": i64::MAX },
                doc! { "_id": { "archive_id": "b", "timestamp": 0_i64 }, "count": 1_i64 },
            ],
        ] {
            collection.insert_many(rows).await?;
            assert!(publish(&client, collection.clone()).await.is_err());
            assert_eq!(
                collection.find_one(doc! { "_id": COMMITTED_ID }).await?,
                None
            );
            collection.delete_many(doc! {}).await?;
        }
        anyhow::Ok(())
    }
    .await;
    collection.drop().await?;
    result
}

/// Both full and partial publication batches must preserve the marker's requested durability.
#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI and MongoDB 8.0"]
async fn publication_preserves_explicit_write_concern() -> anyhow::Result<()> {
    let uri = std::env::var("CLP_TEST_MONGODB_URI")?;
    let commands = Arc::new(Mutex::new(Vec::new()));
    let observed_commands = Arc::clone(&commands);
    let mut options = ClientOptions::parse(uri).await?;
    options.write_concern = Some(
        WriteConcern::builder()
            .w(Acknowledgment::Nodes(1))
            .journal(true)
            .build(),
    );
    options.command_event_handler = Some(EventHandler::callback(move |event| {
        if let CommandEvent::Started(event) = event
            && ["bulkWrite", "update"].contains(&event.command_name.as_str())
        {
            observed_commands
                .lock()
                .expect("command capture lock poisoned")
                .push((event.command_name, event.command));
        }
    }));
    let client = mongodb::Client::with_options(options)?;
    let collection = client.database("clp_timeline_tests").collection(&format!(
        "commit_durability_{}",
        mongodb::bson::oid::ObjectId::new()
    ));
    let result = async {
        collection
            .insert_many((0_i64..1_001).map(|timestamp| {
                doc! {
                    "_id": { "dataset": "default", "archive_id": "a", "timestamp": timestamp },
                    "count": 1_i64,
                }
            }))
            .await?;
        publish(&client, collection.clone()).await?;
        let captured = commands.lock().expect("command capture lock poisoned");
        assert_eq!(
            captured
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            ["bulkWrite", "bulkWrite", "update"]
        );
        for (name, command) in captured.iter() {
            assert_eq!(
                command.get_document("writeConcern")?,
                &doc! { "w": 1, "j": true },
                "missing requested durability on {name}"
            );
        }
        drop(captured);
        anyhow::Ok(())
    }
    .await;
    collection.drop().await?;
    result
}
