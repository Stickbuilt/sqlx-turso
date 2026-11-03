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
    format!("{prefix}_{}_{nanos}", std::process::id())
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
