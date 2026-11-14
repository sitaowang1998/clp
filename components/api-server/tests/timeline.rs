//! Timeline retrieval against disposable MySQL and MongoDB databases.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use api_server::client::Client;
use api_server::client::ClientError;
use api_server::client::QueryConfig;
use api_server::routes::AppState;
use api_server::routes::from_app_state;
use api_server::webui_client::WebuiClient;
use axum::body::Body;
use axum::http::Request;
use axum::http::StatusCode;
use clp_rust_utils::clp_config::package::config::ApiServer;
use clp_rust_utils::clp_config::package::config::Config;
use clp_rust_utils::clp_config::package::config::Package;
use clp_rust_utils::clp_config::package::config::QueryJobPollingConfig;
use clp_rust_utils::clp_config::package::config::ResultsCache;
use clp_rust_utils::clp_config::package::config::StorageEngine;
use clp_rust_utils::job_config::QueryJobStatus;
use clp_rust_utils::timeline::COMMIT_MARKER_ID;
use clp_rust_utils::timeline::collection_name;
use clp_rust_utils::types::PositiveI64;
use futures::TryStreamExt;
use http_body_util::BodyExt;
use mongodb::bson::Bson;
use mongodb::bson::Document;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb::event::EventHandler;
use mongodb::event::command::CommandEvent;
use mongodb::options::ClientOptions;
use serde_json::Value;
use serde_json::json;
use sqlx::ConnectOptions;
use sqlx::mysql::MySqlConnectOptions;
use tower::ServiceExt;

struct Fixture {
    config: Config,
    mongodb: mongodb::Client,
    sql: sqlx::MySqlPool,
    admin_sql: sqlx::MySqlPool,
    commands: Arc<Mutex<Vec<Document>>>,
    mongo_options: ClientOptions,
}

impl Fixture {
    async fn new() -> anyhow::Result<Option<Self>> {
        let (Ok(mongodb_uri), Ok(sql_uri)) = (
            std::env::var("CLP_TEST_MONGODB_URI"),
            std::env::var("CLP_TEST_MYSQL_URI"),
        ) else {
            eprintln!(
                "Skipping live timeline coverage: set CLP_TEST_MONGODB_URI and CLP_TEST_MYSQL_URI."
            );
            return Ok(None);
        };
        let name = format!("timeline_api_{}", ObjectId::new().to_hex());
        let commands = Arc::new(Mutex::new(Vec::new()));
        let events = Arc::clone(&commands);
        let mut mongo_options = ClientOptions::parse(mongodb_uri).await?;
        mongo_options.app_name = Some(name.clone());
        mongo_options.command_event_handler = Some(EventHandler::callback(move |event| {
            if let CommandEvent::Started(event) = event {
                events
                    .lock()
                    .expect("command mutex is not poisoned")
                    .push(event.command);
            }
        }));
        let mongodb = mongodb::Client::with_options(mongo_options.clone())?;
        let sql_options = sql_uri
            .parse::<MySqlConnectOptions>()?
            .disable_statement_logging();
        let admin_sql = sqlx::MySqlPool::connect_with(sql_options.clone()).await?;
        sqlx::query(&format!("CREATE DATABASE `{name}`"))
            .execute(&admin_sql)
            .await?;
        let sql = sqlx::MySqlPool::connect_with(sql_options.database(&name)).await?;
        sqlx::query(
            "CREATE TABLE query_jobs (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, status INT \
             NOT NULL DEFAULT 0, type INT NOT NULL, job_config LONGBLOB NOT NULL)",
        )
        .execute(&sql)
        .await?;
        let config = Config {
            package: Package {
                storage_engine: StorageEngine::ClpS,
            },
            results_cache: ResultsCache {
                db_name: name,
                ..ResultsCache::default()
            },
            api_server: Some(ApiServer {
                query_job_polling: QueryJobPollingConfig {
                    initial_backoff_ms: 5,
                    max_backoff_ms: 10,
                },
                ..ApiServer::default()
            }),
            ..Config::default()
        };
        Ok(Some(Self {
            config,
            mongodb,
            sql,
            admin_sql,
            commands,
            mongo_options,
        }))
    }

    fn client(&self) -> Client {
        Client::new(self.config.clone(), self.mongodb.clone(), self.sql.clone())
    }

    fn independent_client(&self) -> anyhow::Result<Client> {
        let mongodb = mongodb::Client::with_options(self.mongo_options.clone())?;
        Ok(Client::new(self.config.clone(), mongodb, self.sql.clone()))
    }

    async fn app(&self) -> anyhow::Result<axum::Router> {
        Ok(from_app_state(AppState {
            client: self.client(),
            webui_client: WebuiClient::new(
                self.config.clone(),
                self.sql.clone(),
                self.mongodb.clone(),
            )
            .await,
        })?)
    }

    async fn submit(&self, timeline: bool) -> anyhow::Result<u64> {
        Ok(self
            .client()
            .submit_query(QueryConfig {
                query_string: "*".to_owned(),
                datasets: None,
                max_num_results: NonZeroU32::new(1),
                time_range_begin_millisecs: None,
                time_range_end_millisecs: None,
                ignore_case: false,
                buffer_results_in_mongodb: true,
                count_by_time_bucket_size_millisecs: if timeline {
                    PositiveI64::new(100)
                } else {
                    None
                },
            })
            .await?)
    }

    async fn set_status(&self, job: u64, status: QueryJobStatus) -> anyhow::Result<()> {
        sqlx::query("UPDATE query_jobs SET status = ? WHERE id = ?")
            .bind(status)
            .bind(job)
            .execute(&self.sql)
            .await?;
        Ok(())
    }

    fn collection(&self, name: &str) -> mongodb::Collection<Document> {
        self.mongodb
            .database(&self.config.results_cache.db_name)
            .collection(name)
    }

    async fn read(&self, job: u64, timeline: bool) -> Result<Vec<String>, ClientError> {
        self.client()
            .fetch_results(job, false, true, timeline)
            .await?
            .try_collect()
            .await
    }

    async fn read_timeline_sse(&self, job: u64) -> anyhow::Result<Vec<Value>> {
        let response = self
            .app()
            .await?
            .oneshot(
                Request::get(format!("/query_results/{job}?timeline=true")).body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = String::from_utf8(response.into_body().collect().await?.to_bytes().to_vec())?;
        let rows = body
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    async fn assert_committed_reads(&self, collection_name: &str) -> anyhow::Result<()> {
        let commands = self
            .commands
            .lock()
            .expect("command mutex is not poisoned")
            .clone();
        assert!(
            commands.iter().all(|command| ![
                "aggregate",
                "insert",
                "update",
                "delete",
                "bulkWrite"
            ]
            .iter()
            .any(|name| command.contains_key(name))),
            "unexpected read command: {commands:?}"
        );
        let find = commands
            .iter()
            .find(|command| {
                command.get_str("find") == Ok(collection_name) && command.contains_key("sort")
            })
            .expect("final buckets use a sorted find");
        let mut find = find.clone();
        for key in ["$db", "lsid", "$readPreference"] {
            find.remove(key);
        }
        let plan = self
            .mongodb
            .database(&self.config.results_cache.db_name)
            .run_command(doc! { "explain": find })
            .await?;
        let plan = serde_json::to_string(plan.get_document("queryPlanner")?)?;
        assert!(
            plan.contains("_id_") && !plan.contains("\"SORT\"") && !plan.contains("COLLSCAN"),
            "{plan}"
        );
        Ok(())
    }

    async fn cleanup(self) -> anyhow::Result<()> {
        self.mongodb
            .database(&self.config.results_cache.db_name)
            .drop()
            .await?;
        self.sql.close().await;
        sqlx::query(&format!(
            "DROP DATABASE `{}`",
            self.config.results_cache.db_name
        ))
        .execute(&self.admin_sql)
        .await?;
        self.admin_sql.close().await;
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI and CLP_TEST_MYSQL_URI with database creation permission"]
async fn committed_timeline_and_normal_results_are_isolated_and_read_only() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let job = fixture.submit(true).await?;
    fixture.set_status(job, QueryJobStatus::Succeeded).await?;
    fixture
        .collection(&job.to_string())
        .insert_many([
            doc! {
                "_id": { "archive_id": "archive", "log_event_idx": 1_i64 },
                "dataset": "default", "orig_file_path": "",
                "timestamp": 200_i64, "message": "newest"
            },
            doc! {
                "_id": { "orig_file_id": "file", "log_event_idx": 0_i64 },
                "orig_file_path": "/logs/input.log", "timestamp": 100_i64, "message": "older"
            },
            doc! {
                "_id": { "dataset": "a", "archive_id": "b", "timestamp": 900_i64 },
                "count": 99_i64
            },
            doc! { "_id": 900_i64, "count": 99_i64 },
            doc! { "_id": COMMIT_MARKER_ID },
        ])
        .await?;
    let collection = fixture.collection(&collection_name(i32::try_from(job)?));
    collection
        .insert_many([
            doc! {
                "_id": { "dataset": "a", "archive_id": "b", "timestamp": 100_i64 },
                "count": 7_i64
            },
            doc! { "_id": 100_i64, "count": 7_i64 },
            doc! { "_id": -100_i32, "count": 2_i32 },
            doc! { "_id": 0_i64, "count": 0_i64 },
            doc! { "_id": COMMIT_MARKER_ID },
        ])
        .await?;
    let expected = vec![
        json!({ "timestamp": -100, "count": 2 }),
        json!({ "timestamp": 0, "count": 0 }),
        json!({ "timestamp": 100, "count": 7 }),
    ];
    fixture
        .commands
        .lock()
        .expect("command mutex is not poisoned")
        .clear();
    assert_eq!(fixture.read(job, false).await?, ["newest"]);
    let start = Instant::now();
    let first = fixture.independent_client()?;
    let second = fixture.independent_client()?;
    let (first_rows, second_rows) = tokio::try_join!(
        async {
            first
                .fetch_results(job, true, false, true)
                .await?
                .try_collect::<Vec<_>>()
                .await
        },
        async {
            second
                .fetch_results(job, false, true, true)
                .await?
                .try_collect::<Vec<_>>()
                .await
        },
    )?;
    for rows in [first_rows, second_rows, fixture.read(job, true).await?] {
        let values = rows
            .into_iter()
            .map(|row| serde_json::from_str::<Value>(&row))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(values, expected);
    }
    eprintln!(
        "committed concurrent/repeated retrieval elapsed: {:?}",
        start.elapsed()
    );
    fixture.assert_committed_reads(collection.name()).await?;
    let rows = fixture.read_timeline_sse(job).await?;
    assert_eq!(rows, expected);
    fixture.cleanup().await
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI and CLP_TEST_MYSQL_URI with database creation permission"]
async fn omitted_timeline_selector_returns_normal_results_for_timeline_job() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let job = fixture.submit(true).await?;
    fixture.set_status(job, QueryJobStatus::Succeeded).await?;
    fixture
        .collection(&job.to_string())
        .insert_one(doc! {
            "_id": { "archive_id": "archive", "log_event_idx": 0_i64 },
            "timestamp": 100_i64, "message": "normal"
        })
        .await?;
    fixture
        .collection(&collection_name(i32::try_from(job)?))
        .insert_many([
            doc! { "_id": COMMIT_MARKER_ID },
            doc! { "_id": 100_i64, "count": 1_i64 },
        ])
        .await?;
    let response = fixture
        .app()
        .await?
        .oneshot(Request::get(format!("/query_results/{job}")).body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(response.into_body().collect().await?.to_bytes().to_vec())?;
    let rows = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .collect::<Vec<_>>();
    assert_eq!(rows, ["normal"]);
    fixture.cleanup().await
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI and CLP_TEST_MYSQL_URI with database creation permission"]
async fn timeline_reads_do_not_fall_back_to_obsolete_collections() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let job = fixture.submit(true).await?;
    fixture.set_status(job, QueryJobStatus::Succeeded).await?;
    let collection = fixture.collection(&job.to_string());
    for documents in [
        vec![doc! { "_id": ObjectId::new(), "timestamp": 100_i64, "count": 4_i64 }],
        vec![doc! {
            "_id": { "dataset": "a", "archive_id": "archive", "timestamp": 100_i64 },
            "count": 3_i64
        }],
        vec![
            doc! { "_id": COMMIT_MARKER_ID },
            doc! { "_id": 100_i64, "count": 7_i64 },
        ],
    ] {
        collection.delete_many(doc! {}).await?;
        collection.insert_many(documents).await?;
        assert!(matches!(
            fixture.read(job, true).await,
            Err(ClientError::MalformedData)
        ));
    }
    fixture.cleanup().await
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI and CLP_TEST_MYSQL_URI with database creation permission"]
async fn lifecycle_gates_timeline_reads_and_preserves_plain_search() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let job = fixture.submit(true).await?;
    fixture
        .collection(&collection_name(i32::try_from(job)?))
        .insert_one(doc! { "_id": COMMIT_MARKER_ID })
        .await?;
    for status in [
        QueryJobStatus::Failed,
        QueryJobStatus::CancelRequested,
        QueryJobStatus::Cancelling,
        QueryJobStatus::Cancelled,
        QueryJobStatus::Killed,
    ] {
        fixture.set_status(job, status).await?;
        assert!(matches!(
            fixture.read(job, true).await,
            Err(ClientError::QueryNotSucceeded)
        ));
    }
    for status in [QueryJobStatus::Pending, QueryJobStatus::Running] {
        fixture.set_status(job, status).await?;
        let read = fixture.read(job, true);
        tokio::pin!(read);
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut read)
                .await
                .is_err()
        );
        fixture.set_status(job, QueryJobStatus::Succeeded).await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), read).await??,
            Vec::<String>::new()
        );
    }
    assert!(matches!(
        fixture.read(u64::MAX, true).await,
        Err(ClientError::QueryJobNotFound(_))
    ));
    let normal_job = fixture.submit(false).await?;
    fixture
        .set_status(normal_job, QueryJobStatus::Succeeded)
        .await?;
    fixture
        .collection(&normal_job.to_string())
        .insert_one(doc! {
            "_id": { "archive_id": "archive", "log_event_idx": 0_i64 },
            "timestamp": 0_i64, "message": "plain"
        })
        .await?;
    assert_eq!(fixture.read(normal_job, false).await?, ["plain"]);
    assert_eq!(
        fixture
            .client()
            .fetch_results(normal_job, false, false, false)
            .await?
            .try_collect::<Vec<_>>()
            .await?,
        ["plain"]
    );
    assert!(matches!(
        fixture.read(normal_job, true).await,
        Err(ClientError::InvalidInput(_))
    ));
    fixture.cleanup().await
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI and CLP_TEST_MYSQL_URI with database creation permission"]
async fn malformed_and_unsafe_timeline_values_fail_retrieval() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let job = fixture.submit(true).await?;
    fixture.set_status(job, QueryJobStatus::Succeeded).await?;
    let collection = fixture.collection(&collection_name(i32::try_from(job)?));
    for invalid in [
        doc! { "_id": 0.5, "count": 1_i64 },
        doc! { "_id": 0_i64, "count": -1_i64 },
        doc! { "_id": 0_i64, "count": 1.5 },
        doc! { "_id": 0_i64 },
        doc! { "_id": 0_i64, "count": 9_007_199_254_740_992_i64 },
        doc! { "_id": 9_007_199_254_740_992_i64, "count": 1_i64 },
        doc! { "_id": -9_007_199_254_740_992_i64, "count": 1_i64 },
    ] {
        collection.delete_many(doc! {}).await?;
        collection
            .insert_many([doc! { "_id": COMMIT_MARKER_ID }, invalid.clone()])
            .await?;
        assert!(fixture.read(job, true).await.is_err(), "accepted {invalid}");
    }
    collection.delete_many(doc! {}).await?;
    collection
        .insert_many([
            doc! { "_id": COMMIT_MARKER_ID },
            doc! { "_id": -9_007_199_254_740_991_i64, "count": 9_007_199_254_740_991_i64 },
            doc! { "_id": 9_007_199_254_740_991_i64, "count": 0_i64 },
        ])
        .await?;
    let rows = fixture
        .read(job, true)
        .await?
        .into_iter()
        .map(|row| serde_json::from_str::<Value>(&row))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        rows,
        [
            json!({ "timestamp": -9_007_199_254_740_991_i64, "count": 9_007_199_254_740_991_i64 }),
            json!({ "timestamp": 9_007_199_254_740_991_i64, "count": 0 }),
        ]
    );
    fixture.cleanup().await
}

#[tokio::test]
#[ignore = "requires MongoDB enableTestCommands=1 plus CLP_TEST_MONGODB_URI and CLP_TEST_MYSQL_URI"]
async fn interrupted_cursor_fails_sse_and_allows_retry() -> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let job = fixture.submit(true).await?;
    fixture.set_status(job, QueryJobStatus::Succeeded).await?;
    let collection = fixture.collection(&collection_name(i32::try_from(job)?));
    collection
        .insert_many((0..2000_i64).map(|timestamp| {
            doc! {
                "_id": timestamp, "count": 1_i64,
            }
        }))
        .await?;
    collection
        .insert_one(doc! { "_id": COMMIT_MARKER_ID })
        .await?;
    let response = fixture
        .app()
        .await?
        .oneshot(Request::get(format!("/query_results/{job}?timeline=true")).body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    fixture
        .mongodb
        .database("admin")
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": { "times": 1 },
            "data": {
                "failCommands": ["getMore"],
                "errorCode": 43,
                "appName": &fixture.config.results_cache.db_name,
            },
        })
        .await?;
    let delivery = response.into_body().collect().await;
    fixture
        .mongodb
        .database("admin")
        .run_command(doc! {
            "configureFailPoint": "failCommand", "mode": "off",
        })
        .await?;
    assert!(
        delivery.is_err(),
        "a failed cursor must not finish the SSE stream successfully"
    );
    assert_eq!(fixture.read(job, true).await?.len(), 2000);
    fixture.cleanup().await
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI and CLP_TEST_MYSQL_URI with database creation permission"]
async fn timeline_reads_require_the_commit_marker_without_merging_contributions()
-> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let job = fixture.submit(true).await?;
    fixture.set_status(job, QueryJobStatus::Succeeded).await?;
    let collection = fixture.collection(&collection_name(i32::try_from(job)?));
    collection
        .insert_many([
            doc! {
                "_id": { "dataset": "a", "archive_id": "archive", "timestamp": 100_i64 },
                "count": 2_i64
            },
            doc! { "_id": 100_i64, "count": 9_i64 },
        ])
        .await?;
    fixture
        .commands
        .lock()
        .expect("command mutex is not poisoned")
        .clear();
    assert!(matches!(
        fixture.read(job, true).await,
        Err(ClientError::MalformedData)
    ));
    assert!(
        fixture
            .commands
            .lock()
            .expect("command mutex is not poisoned")
            .iter()
            .all(|command| !command.contains_key("aggregate"))
    );
    let response = fixture
        .app()
        .await?
        .oneshot(Request::get(format!("/query_results/{job}?timeline=true")).body(Body::empty())?)
        .await?;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    collection
        .insert_one(doc! { "_id": COMMIT_MARKER_ID })
        .await?;
    assert_eq!(
        fixture.read_timeline_sse(job).await?,
        [json!({ "timestamp": 100, "count": 9 })]
    );
    fixture.cleanup().await
}

#[tokio::test]
#[ignore = "requires CLP_TEST_MONGODB_URI and CLP_TEST_MYSQL_URI with database creation permission"]
async fn normal_results_accept_core_identities_and_exclude_obsolete_and_timeline_rows()
-> anyhow::Result<()> {
    let Some(fixture) = Fixture::new().await? else {
        return Ok(());
    };
    let job = fixture.submit(false).await?;
    fixture.set_status(job, QueryJobStatus::Succeeded).await?;
    let collection = fixture.collection(&job.to_string());
    for identity in [
        Bson::Document(doc! { "archive_id": "archive", "log_event_idx": 1_i64 }),
        Bson::Document(doc! { "orig_file_id": "file", "log_event_idx": 2_i64 }),
    ] {
        collection.delete_many(doc! {}).await?;
        let document = doc! { "_id": identity, "timestamp": 100_i64, "message": "normal" };
        collection.insert_one(document.clone()).await?;
        assert_eq!(fixture.read(job, false).await?, ["normal"]);
        let rows = fixture
            .client()
            .fetch_results(job, true, true, false)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let rows = rows
            .into_iter()
            .map(|row| serde_json::from_str::<Value>(&row))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(rows, [serde_json::to_value(document)?]);
    }
    for invalid in [
        doc! { "_id": ObjectId::new(), "timestamp": 100_i64, "message": "obsolete normal" },
        doc! { "_id": ObjectId::new(), "timestamp": 100_i64, "count": 4_i64 },
        doc! { "_id": { "dataset": "a", "archive_id": "b" }, "message": "invalid" },
        doc! {
            "_id": { "archive_id": "b", "log_event_idx": 0_i64 },
            "count": 1_i64, "message": "invalid"
        },
        doc! {
            "_id": { "archive_id": "b", "log_event_idx": 0_i64, "timestamp": 0_i64 },
            "message": "invalid"
        },
    ] {
        collection.delete_many(doc! {}).await?;
        collection.insert_one(invalid.clone()).await?;
        assert_eq!(
            fixture.read(job, false).await?,
            Vec::<String>::new(),
            "accepted {invalid}"
        );
    }
    fixture.cleanup().await
}
