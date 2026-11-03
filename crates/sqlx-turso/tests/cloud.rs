//! Tests against a real Turso Cloud database.
//!
//! Skipped unless `TURSO_DATABASE_URL` (e.g. `libsql://db-org.turso.io`) and
//! `TURSO_AUTH_TOKEN` are set. Each test works on uniquely named tables and drops them.
#![cfg(any(feature = "serverless", feature = "sync"))]

use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::{Connection, Executor, Row};
use sqlx_core::error::ErrorKind;
use sqlx_turso::{TursoConnectOptions, TursoConnection, TursoPoolOptions};

fn cloud() -> Option<(String, String)> {
    match (
        std::env::var("TURSO_DATABASE_URL"),
        std::env::var("TURSO_AUTH_TOKEN"),
    ) {
        (Ok(url), Ok(token)) if !url.is_empty() && !token.is_empty() => Some((url, token)),
        _ => {
            eprintln!("skipping: TURSO_DATABASE_URL / TURSO_AUTH_TOKEN not set");
            None
        }
    }
}

fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // The counter keeps names unique even when the clock doesn't (macOS: microseconds).
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("{prefix}_{}_{nanos}_{n}", std::process::id())
}

#[cfg(feature = "serverless")]
mod serverless {
    use super::*;

    async fn connect(url: &str, token: &str) -> TursoConnection {
        let opts = TursoConnectOptions::new().remote(url).auth_token(token);
        TursoConnection::connect_with(&opts).await.unwrap()
    }

    #[tokio::test]
    async fn crud_transactions_and_errors() {
        let Some((url, token)) = cloud() else { return };
        let mut conn = connect(&url, &token).await;
        assert!(conn.is_remote());

        let t = unique("sqlx_turso_crud");
        conn.execute(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {t} (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, data BLOB)"
        )))
        .await
        .unwrap();

        let result: sqlx::Result<()> = async {
            let (id, name): (i64, String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {t} (name, data) VALUES (?, ?) RETURNING id, name"
            )))
            .bind("a")
            .bind(vec![0_u8, 1, 2])
            .fetch_one(&mut conn)
            .await?;
            assert_eq!((id, name.as_str()), (1, "a"));

            let done = sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {t} (name) VALUES (?)"
            )))
            .bind("b")
            .execute(&mut conn)
            .await?;
            assert_eq!(done.rows_affected(), 1);
            assert_eq!(done.last_insert_rowid(), 2);

            // Rolled-back transaction leaves no trace; nested savepoints work.
            {
                let mut tx = conn.begin().await?;
                tx.execute(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t} (name) VALUES ('c')"
                )))
                .await?;
                {
                    let mut inner = tx.begin().await?;
                    inner
                        .execute(sqlx::AssertSqlSafe(format!(
                            "INSERT INTO {t} (name) VALUES ('d')"
                        )))
                        .await?;
                    inner.rollback().await?;
                }
                tx.commit().await?;
            }
            {
                let mut tx = conn.begin().await?;
                tx.execute(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t} (name) VALUES ('e')"
                )))
                .await?;
                // dropped: rolled back before the next statement
            }

            let names: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT name FROM {t} ORDER BY id"
            )))
            .fetch_all(&mut conn)
            .await?;
            assert_eq!(names, ["a", "b", "c"]);

            let err = conn
                .execute(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t} (name) VALUES ('a')"
                )))
                .await
                .unwrap_err();
            assert_eq!(
                err.as_database_error().unwrap().kind(),
                ErrorKind::UniqueViolation
            );

            let row = sqlx::query(sqlx::AssertSqlSafe(format!(
                "SELECT data FROM {t} WHERE id = 1"
            )))
            .fetch_one(&mut conn)
            .await?;
            assert_eq!(row.get::<Vec<u8>, _>(0), vec![0, 1, 2]);

            Ok(())
        }
        .await;

        conn.execute(sqlx::AssertSqlSafe(format!("DROP TABLE IF EXISTS {t}")))
            .await
            .unwrap();
        result.unwrap();
    }

    #[tokio::test]
    async fn url_with_auth_token_and_pool() {
        let Some((url, token)) = cloud() else { return };
        let sep = if url.contains('?') { '&' } else { '?' };
        let pool = TursoPoolOptions::new()
            .max_connections(3)
            .connect(&format!("{url}{sep}authToken={token}"))
            .await
            .unwrap();

        let n: i64 = sqlx::query_scalar("SELECT 1 + 1")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 2);
        pool.close().await;
    }

    fn with_token(url: &str, token: &str) -> String {
        let sep = if url.contains('?') { '&' } else { '?' };
        format!("{url}{sep}authToken={token}")
    }

    #[tokio::test]
    async fn connect_and_exists_contact_the_server() {
        use sqlx_core::migrate::MigrateDatabase;
        let Some((url, token)) = cloud() else { return };

        let bad = TursoConnectOptions::new()
            .remote(&url)
            .auth_token("not-a-valid-token");
        assert!(TursoConnection::connect_with(&bad).await.is_err());
        assert!(
            TursoPoolOptions::new()
                .acquire_timeout(std::time::Duration::from_secs(10))
                .connect_with(bad)
                .await
                .is_err()
        );

        assert!(
            sqlx_turso::Turso::database_exists(&with_token(&url, "not-a-valid-token"))
                .await
                .is_err()
        );
        assert!(
            sqlx_turso::Turso::database_exists(&with_token(&url, &token))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn per_connection_settings_are_rejected() {
        let Some((url, token)) = cloud() else { return };
        let opts = TursoConnectOptions::new().remote(&url).auth_token(&token);

        for opts in [
            opts.clone().foreign_keys(true),
            opts.clone().pragma("cache_size", "100"),
        ] {
            match TursoConnection::connect_with(&opts).await {
                Err(sqlx::Error::Configuration(_)) => {}
                other => panic!("expected a configuration error, got {other:?}"),
            }
        }

        // The server default applies: foreign keys are enforced.
        let mut conn = TursoConnection::connect_with(&opts).await.unwrap();
        let fk: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(fk, 1);
    }

    #[tokio::test]
    async fn pool_release_does_not_ping_the_server() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let Some((url, token)) = cloud() else { return };

        // The token callback runs once per HTTP request.
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = requests.clone();
        let opts = TursoConnectOptions::new()
            .remote(&url)
            .auth_token_fn(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                let token = token.clone();
                async move { Ok(token) }
            });
        let pool = TursoPoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .unwrap();

        for _ in 0..3 {
            let before = requests.load(Ordering::SeqCst);
            let mut conn = pool.acquire().await.unwrap();
            let n: i64 = sqlx::query_scalar("SELECT 1 + 1")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
            assert_eq!(n, 2);
            drop(conn);
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            assert_eq!(requests.load(Ordering::SeqCst), before + 1);
        }
        pool.close().await;
    }

    #[tokio::test]
    async fn statements_after_stream_reset_do_not_autocommit() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let Some((url, token)) = cloud() else { return };

        // A failing token callback makes the client drop its HTTP stream, and with it the
        // server-side transaction, exactly like a transport failure.
        let fail = Arc::new(AtomicBool::new(false));
        let switch = fail.clone();
        let opts = TursoConnectOptions::new()
            .remote(&url)
            .auth_token_fn(move || {
                let fail = switch.load(Ordering::SeqCst);
                let token = token.clone();
                async move {
                    if fail {
                        Err("token unavailable".into())
                    } else {
                        Ok(token)
                    }
                }
            });
        let mut conn = TursoConnection::connect_with(&opts).await.unwrap();
        let t = unique("sqlx_turso_reset");
        conn.execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {t} (x INTEGER)")))
            .await
            .unwrap();

        let result: sqlx::Result<()> = async {
            {
                // Read-only before the reset, so the abandoned server-side transaction
                // holds no write lock while it waits to expire.
                let mut tx = conn.begin_with("BEGIN").await?;
                let _: i64 =
                    sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {t}")))
                        .fetch_one(&mut *tx)
                        .await?;

                fail.store(true, Ordering::SeqCst);
                assert!(tx.execute("SELECT 1").await.is_err());
                fail.store(false, Ordering::SeqCst);

                for _ in 0..2 {
                    let err = tx
                        .execute(sqlx::AssertSqlSafe(format!("INSERT INTO {t} VALUES (1)")))
                        .await
                        .unwrap_err();
                    let kind = err
                        .as_database_error()
                        .and_then(|e| e.try_downcast_ref::<sqlx_turso::TursoError>())
                        .map(|e| e.turso_kind());
                    assert_eq!(kind, Some(sqlx_turso::TursoErrorKind::Aborted), "{err}");
                }
                tx.rollback().await?;
            }

            let n: i64 =
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {t}")))
                    .fetch_one(&mut conn)
                    .await?;
            assert_eq!(n, 0);
            assert!(!conn.is_in_transaction());
            Ok(())
        }
        .await;

        fail.store(false, Ordering::SeqCst);
        conn.execute(sqlx::AssertSqlSafe(format!("DROP TABLE IF EXISTS {t}")))
            .await
            .unwrap();
        result.unwrap();
    }

    #[tokio::test]
    async fn transactions_work_with_every_remote_scheme() {
        let Some((url, token)) = cloud() else { return };
        let host = url.split_once("://").map_or(url.as_str(), |(_, rest)| rest);
        for scheme in ["libsql", "turso", "https"] {
            let opts = TursoConnectOptions::new()
                .remote(format!("{scheme}://{host}"))
                .auth_token(&token);
            let mut conn = TursoConnection::connect_with(&opts).await.unwrap();
            let mut tx = conn
                .begin()
                .await
                .unwrap_or_else(|e| panic!("{scheme}: {e}"));
            let n: i64 = sqlx::query_scalar("SELECT 1")
                .fetch_one(&mut *tx)
                .await
                .unwrap();
            assert_eq!(n, 1);
            tx.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn migrations_run_on_concurrent_default_url() {
        use sqlx_core::migrate::{Migration, MigrationType, Migrator};
        use sqlx_core::sql_str::SqlSafeStr;
        let Some((url, token)) = cloud() else { return };

        // With concurrent writes the default BEGIN is BEGIN CONCURRENT, which refuses DDL
        // (and which servers without concurrent writes reject); migrations must not use it.
        let host = url.split_once("://").map_or(url.as_str(), |(_, rest)| rest);
        let opts = TursoConnectOptions::new()
            .remote(format!("turso://{host}"))
            .auth_token(&token)
            .concurrent_writes(true);
        let mut conn = TursoConnection::connect_with(&opts).await.unwrap();

        let t = unique("sqlx_turso_mig");
        let table = unique("sqlx_turso_migrations");
        let sql = |s: String| sqlx::AssertSqlSafe(s).into_sql_str();
        let mut migrator = Migrator::with_migrations(vec![
            Migration::new(
                1,
                "create".into(),
                MigrationType::ReversibleUp,
                sql(format!("CREATE TABLE {t} (id INTEGER PRIMARY KEY, v TEXT)")),
                false,
            ),
            Migration::new(
                1,
                "create".into(),
                MigrationType::ReversibleDown,
                sql(format!("DROP TABLE {t}")),
                false,
            ),
            Migration::new(
                2,
                "alter".into(),
                MigrationType::ReversibleUp,
                sql(format!(
                    "ALTER TABLE {t} ADD COLUMN w INTEGER; INSERT INTO {t} (v) VALUES ('x')"
                )),
                false,
            ),
            Migration::new(
                2,
                "alter".into(),
                MigrationType::ReversibleDown,
                sql(format!("DELETE FROM {t}")),
                false,
            ),
        ]);
        migrator.dangerous_set_table_name(table.clone());

        let result: anyhow::Result<()> = async {
            migrator.run(&mut conn).await?;
            // Right after DDL, Turso Cloud occasionally returns rows wider than the
            // (stale) column metadata when other streams change the schema concurrently;
            // the driver reports that as a retryable schema error.
            let mut attempts = 0;
            let row = loop {
                attempts += 1;
                match sqlx::query(sqlx::AssertSqlSafe(format!("SELECT * FROM {t}")))
                    .fetch_one(&mut conn)
                    .await
                {
                    Err(sqlx::Error::Database(e))
                        if attempts < 5
                            && e.try_downcast_ref::<sqlx_turso::TursoError>().is_some_and(
                                |e| e.turso_kind() == sqlx_turso::TursoErrorKind::Schema,
                            ) =>
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                    other => break other?,
                }
            };
            assert_eq!(row.len(), 3);
            migrator.undo(&mut conn, 0).await?;
            let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM sqlite_master WHERE name = '{t}'"
            )))
            .fetch_one(&mut conn)
            .await?;
            assert_eq!(n, 0);
            Ok(())
        }
        .await;

        conn.execute(sqlx::AssertSqlSafe(format!(
            "DROP TABLE IF EXISTS {t}; DROP TABLE IF EXISTS {table}"
        )))
        .await
        .unwrap();
        result.unwrap();
    }
}

#[cfg(feature = "sync")]
mod sync {
    use super::*;

    fn replica(dir: &std::path::Path, name: &str, url: &str, token: &str) -> TursoConnectOptions {
        TursoConnectOptions::new()
            .filename(dir.join(name))
            .sync_url(url)
            .auth_token(token)
    }

    #[tokio::test]
    async fn push_from_one_replica_and_pull_into_another() {
        let Some((url, token)) = cloud() else { return };
        let dir = tempfile::tempdir().unwrap();
        let t = unique("sqlx_turso_sync");

        let a = replica(dir.path(), "a.db", &url, &token);
        let b = replica(dir.path(), "b.db", &url, &token);

        let mut conn_a = TursoConnection::connect_with(&a).await.unwrap();
        conn_a
            .execute(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {t} (id INTEGER PRIMARY KEY, v TEXT); INSERT INTO {t} (v) VALUES ('x')"
            )))
            .await
            .unwrap();
        a.push().await.unwrap();

        let result: sqlx::Result<()> = async {
            let mut conn_b = TursoConnection::connect_with(&b).await?;
            b.pull().await?;
            let v: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT v FROM {t}")))
                .fetch_one(&mut conn_b)
                .await?;
            assert_eq!(v, "x");
            let _stats = b.sync_stats().await?;
            Ok(())
        }
        .await;

        conn_a
            .execute(sqlx::AssertSqlSafe(format!("DROP TABLE IF EXISTS {t}")))
            .await
            .unwrap();
        a.push().await.unwrap();
        result.unwrap();
    }

    #[tokio::test]
    async fn sync_ops_require_sync_configuration() {
        let opts = TursoConnectOptions::new().in_memory();
        assert!(opts.push().await.is_err());
    }
}
