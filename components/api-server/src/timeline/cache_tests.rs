//! Live tests for lazy timeline cache publication, reuse, and invalidation.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::TryStreamExt;
use mongodb::Collection;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::event::EventHandler;
use mongodb::event::command::CommandEvent;
use mongodb::options::ClientOptions;

use super::CACHE_FIELD;
use super::Cache;

struct Fixture {
    collection: Collection<Document>,
    aggregates: Arc<AtomicUsize>,
}

impl Fixture {
    /// Creates a uniquely named collection with aggregate-command monitoring.
    async fn new() -> anyhow::Result<Option<Self>> {
        let Ok(uri) = std::env::var("CLP_TEST_MONGODB_URI") else {
            eprintln!("skipping MongoDB cache integration test: CLP_TEST_MONGODB_URI is unset");
            return Ok(None);
        };
        let aggregates = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&aggregates);
        let mut options = ClientOptions::parse(uri).await?;
        options.command_event_handler = Some(EventHandler::callback(move |event| {
            if let CommandEvent::Started(event) = event
                && event.command_name == "aggregate"
            {
                observed.fetch_add(1, Ordering::SeqCst);
            }
        }));
        let client = mongodb::Client::with_options(options)?;
        let collection = client
            .database("clp_timeline_tests")
            .collection(&format!("cache_{}", mongodb::bson::oid::ObjectId::new()));
        Ok(Some(Self {
            collection,
            aggregates,
        }))
    }

    /// Seeds compact per-archive contributions without duplicated top-level identity fields.
    async fn seed(&self) -> anyhow::Result<()> {
        self.collection
            .insert_many([
                doc! {
                    "_id": { "dataset": "default", "archive_id": "a", "timestamp": -1000_i64 },
                    "count": 3_i64,
                },
                doc! {
                    "_id": { "dataset": "default", "archive_id": "b", "timestamp": -1000_i64 },
                    "count": 4_i64,
                },
                doc! {
                    "_id": { "dataset": "default", "archive_id": "c", "timestamp": 0_i64 },
                    "count": 9_i64,
                },
            ])
            .await?;
        Ok(())
    }

    /// Reads a complete timeline and parses its ordinary JSON representation.
    async fn read(&self, cache: &Cache) -> anyhow::Result<Vec<serde_json::Value>> {
        let buckets: Vec<String> = cache
            .fetch(self.collection.clone())
            .await?
            .try_collect()
            .await?;
        Ok(buckets
            .iter()
            .map(|bucket| serde_json::from_str(bucket))
            .collect::<Result<_, _>>()?)
    }

    /// Checks the known source totals without depending on the cache representation.
    fn assert_buckets(buckets: &[serde_json::Value]) {
        assert_eq!(
            buckets,
            [
                serde_json::json!({ "timestamp": -1000, "count": 7 }),
                serde_json::json!({ "timestamp": 0, "count": 9 }),
            ]
        );
    }

    /// Returns whether any source row holds a persisted cache.
    async fn has_cache(&self) -> anyhow::Result<bool> {
        Ok(self
            .collection
            .find_one(doc! { CACHE_FIELD: { "$exists": true } })
            .await?
            .is_some())
    }
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn concurrent_misses_and_restart_use_one_aggregation() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.seed().await?;
    let cache = Cache::default();
    let barrier = tokio::sync::Barrier::new(8);
    let requests = (0..8).map(|_| async {
        barrier.wait().await;
        fixture.read(&cache).await
    });
    for buckets in futures::future::try_join_all(requests).await? {
        Fixture::assert_buckets(&buckets);
    }
    assert_eq!(fixture.aggregates.load(Ordering::SeqCst), 1);
    assert!(fixture.has_cache().await?);
    assert_eq!(fixture.collection.estimated_document_count().await?, 3);
    // A new cache instance has no local fill state and must discover persisted results.
    Fixture::assert_buckets(&fixture.read(&Cache::default()).await?);
    assert_eq!(fixture.aggregates.load(Ordering::SeqCst), 1);
    fixture.collection.drop().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn malformed_cache_and_worker_replacement_recompute() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.seed().await?;
    let cache = Cache::default();
    let corrupt = [
        mongodb::bson::Bson::Array(Vec::new()),
        mongodb::bson::Bson::String("invalid envelope".to_owned()),
        mongodb::bson::Bson::Array(vec!["not JSON".into()]),
        mongodb::bson::Bson::Array(vec![r#"{"timestamp":0,"count":-1}"#.into()]),
        mongodb::bson::Bson::Array(vec![
            r#"{"timestamp":0,"count":1}"#.into(),
            r#"{"timestamp":-1,"count":1}"#.into(),
        ]),
    ];
    for (index, value) in corrupt.into_iter().enumerate() {
        fixture
            .collection
            .update_one(
                doc! { "_id": { "dataset": "default", "archive_id": "a", "timestamp": -1000_i64 } },
                doc! { "$set": { CACHE_FIELD: value } },
            )
            .await?;
        Fixture::assert_buckets(&fixture.read(&cache).await?);
        assert_eq!(fixture.aggregates.load(Ordering::SeqCst), index + 1);
    }
    let before = fixture.aggregates.load(Ordering::SeqCst);
    fixture
        .collection
        .replace_one(
            doc! { "_id": { "dataset": "default", "archive_id": "a", "timestamp": -1000_i64 } },
            doc! {
                "_id": { "dataset": "default", "archive_id": "a", "timestamp": -1000_i64 },
                "count": 3_i64,
            },
        )
        .await?;
    assert!(!fixture.has_cache().await?);
    Fixture::assert_buckets(&fixture.read(&cache).await?);
    assert_eq!(fixture.aggregates.load(Ordering::SeqCst), before + 1);
    fixture.collection.drop().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn oversized_histogram_streams_without_publication() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.seed().await?;
    let cache = Cache {
        max_bytes: 1,
        ..Default::default()
    };
    for expected_queries in 1..=2 {
        Fixture::assert_buckets(&fixture.read(&cache).await?);
        assert_eq!(fixture.aggregates.load(Ordering::SeqCst), expected_queries);
        assert!(!fixture.has_cache().await?);
    }
    fixture.collection.drop().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn malformed_late_bucket_never_publishes_partial_cache() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.seed().await?;
    // This invalid group follows both valid groups in timestamp order.
    fixture
        .collection
        .insert_one(doc! { "_id": 4, "timestamp": 1000, "count": "invalid" })
        .await?;
    let cache = Cache::default();
    assert!(fixture.read(&cache).await.is_err());
    assert!(!fixture.has_cache().await?);
    fixture.collection.delete_one(doc! { "_id": 4 }).await?;
    Fixture::assert_buckets(&fixture.read(&cache).await?);
    assert_eq!(fixture.aggregates.load(Ordering::SeqCst), 2);
    fixture.collection.drop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn collection_expiring_before_publication_is_not_recreated() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.seed().await?;
    let uri = std::env::var("CLP_TEST_MONGODB_URI")?;
    let mut options = ClientOptions::parse(uri).await?;
    let collection_to_drop = fixture.collection.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&drops);
    options.command_event_handler = Some(EventHandler::callback(move |event| {
        if let CommandEvent::Started(event) = event
            && event.command_name == "update"
        {
            // CommandStarted fires before the update is sent. The separate client does not
            // invoke this callback, so deletion deterministically completes before publication.
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current()
                    .block_on(async { collection_to_drop.drop().await })
            })
            .expect("drop test collection before cache publication");
            observed.fetch_add(1, Ordering::SeqCst);
        }
    }));
    let client = mongodb::Client::with_options(options)?;
    let database = client.database(&fixture.collection.namespace().db);
    let collection = database.collection(&fixture.collection.namespace().coll);
    let values: Vec<String> = Cache::default()
        .fetch(collection)
        .await?
        .try_collect()
        .await?;
    let buckets = values
        .iter()
        .map(|value| serde_json::from_str(value))
        .collect::<Result<Vec<_>, _>>()?;
    Fixture::assert_buckets(&buckets);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(
        !database
            .list_collection_names()
            .await?
            .contains(&fixture.collection.namespace().coll)
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn failed_publication_preserves_results_and_retries_next_read() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture.seed().await?;
    // Reject only cache publication at the server, without shared failpoints or network timing.
    fixture
        .collection
        .client()
        .database(&fixture.collection.namespace().db)
        .run_command(doc! {
            "collMod": fixture.collection.name(),
            "validator": { CACHE_FIELD: { "$exists": false } },
            "validationLevel": "strict",
            "validationAction": "error",
        })
        .await?;
    let cache = Cache::default();
    for expected_queries in 1..=2 {
        Fixture::assert_buckets(&fixture.read(&cache).await?);
        assert_eq!(fixture.aggregates.load(Ordering::SeqCst), expected_queries);
        assert!(!fixture.has_cache().await?);
    }
    fixture.collection.drop().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn empty_collection_does_not_create_cache_or_aggregate() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    assert_eq!(
        fixture.read(&Cache::default()).await?,
        Vec::<serde_json::Value>::new()
    );
    assert_eq!(fixture.aggregates.load(Ordering::SeqCst), 0);
    let names = fixture
        .collection
        .client()
        .database(&fixture.collection.namespace().db)
        .list_collection_names()
        .await?;
    assert!(!names.contains(&fixture.collection.namespace().coll));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires CLP_TEST_MONGODB_URI"]
async fn failed_cursor_never_publishes_partial_cache() -> anyhow::Result<()> {
    // The default initial aggregate batch holds 101 documents, forcing a getMore here.
    const NUM_BUCKETS: i64 = 256;

    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    fixture
        .collection
        .insert_many((0..NUM_BUCKETS).map(|timestamp| {
            doc! {
                "_id": { "dataset": "default", "archive_id": "a", "timestamp": timestamp },
                "count": 1_i64,
            }
        }))
        .await?;
    let uri = std::env::var("CLP_TEST_MONGODB_URI")?;
    let mut options = ClientOptions::parse(uri).await?;
    let database = fixture
        .collection
        .client()
        .database(&fixture.collection.namespace().db);
    let kills = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&kills);
    options.command_event_handler = Some(EventHandler::callback(move |event| {
        if let CommandEvent::Started(event) = event
            && event.command_name == "getMore"
            && observed
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            let cursor_id = event
                .command
                .get_i64("getMore")
                .expect("cursor ID in getMore command");
            let collection_name = event
                .command
                .get_str("collection")
                .expect("collection in getMore command");
            // Kill only this test's cursor through a separate client before its next batch is
            // requested. Unlike a global failpoint, this cannot affect unrelated requests.
            let reply = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    database
                        .run_command(doc! {
                            "killCursors": collection_name,
                            "cursors": [cursor_id],
                        })
                        .await
                })
            })
            .expect("kill test cursor before getMore");
            assert_eq!(
                reply
                    .get_array("cursorsKilled")
                    .expect("killed cursor list"),
                &[mongodb::bson::Bson::Int64(cursor_id)]
            );
        }
    }));
    let client = mongodb::Client::with_options(options)?;
    let collection = client
        .database(&fixture.collection.namespace().db)
        .collection(&fixture.collection.namespace().coll);
    let cache = Cache::default();
    let error = cache
        .fetch(collection.clone())
        .await
        .err()
        .expect("interrupted cursor must fail");
    assert!(
        matches!(error, crate::error::ClientError::Mongo(_)),
        "{error:?}"
    );
    assert_eq!(kills.load(Ordering::SeqCst), 1);
    assert!(!fixture.has_cache().await?);
    // The failed fill must release its lock and leave no persistent partial histogram.
    let values: Vec<String> = cache.fetch(collection).await?.try_collect().await?;
    let actual = values
        .iter()
        .map(|value| serde_json::from_str::<serde_json::Value>(value))
        .collect::<Result<Vec<_>, _>>()?;
    let expected: Vec<_> = (0..NUM_BUCKETS)
        .map(|timestamp| serde_json::json!({ "timestamp": timestamp, "count": 1 }))
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(kills.load(Ordering::SeqCst), 1);
    assert!(fixture.has_cache().await?);
    fixture.collection.drop().await?;
    Ok(())
}
