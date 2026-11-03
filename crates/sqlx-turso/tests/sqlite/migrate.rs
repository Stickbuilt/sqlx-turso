//! Port of `sqlx/tests/sqlite/migrate.rs`. `#[sqlx::test(migrations = false)]` is replaced
//! by [`test_conn`], which hands each test a pooled connection to its own fresh database
//! file.

use sqlx::Executor;
use sqlx::Row;
use sqlx::pool::PoolConnection;
use sqlx_core::migrate::Migrator;
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection, TursoPoolOptions};
use tempfile::TempDir;

use crate::common::fixture_dir;

async fn test_conn() -> anyhow::Result<(PoolConnection<Turso>, TempDir)> {
    let dir = tempfile::tempdir()?;
    let options = TursoConnectOptions::new()
        .filename(dir.path().join("test.db"))
        .create_if_missing(true);
    let pool = TursoPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?;
    Ok((pool.acquire().await?, dir))
}

#[tokio::test]
async fn simple() -> anyhow::Result<()> {
    let (mut conn, _db) = test_conn().await?;
    clean_up(&mut conn).await?;

    let migrator = Migrator::new(fixture_dir().join("migrations_simple")).await?;

    // run migration
    migrator.run(&mut conn).await?;

    // check outcome
    let res: String = conn
        .fetch_one("SELECT some_payload FROM migrations_simple_test")
        .await?
        .get(0);
    assert_eq!(res, "110_suffix");

    // running it a 2nd time should still work
    migrator.run(&mut conn).await?;

    Ok(())
}

#[tokio::test]
async fn reversible() -> anyhow::Result<()> {
    let (mut conn, _db) = test_conn().await?;
    clean_up(&mut conn).await?;

    let migrator = Migrator::new(fixture_dir().join("migrations_reversible")).await?;

    // run migration
    migrator.run(&mut conn).await?;

    // check outcome
    let res: i64 = conn
        .fetch_one("SELECT some_payload FROM migrations_reversible_test")
        .await?
        .get(0);
    assert_eq!(res, 101);

    // roll back nothing (last version)
    migrator.undo(&mut conn, 20220721125033).await?;

    // check outcome
    let res: i64 = conn
        .fetch_one("SELECT some_payload FROM migrations_reversible_test")
        .await?
        .get(0);
    assert_eq!(res, 101);

    // roll back one version
    migrator.undo(&mut conn, 20220721124650).await?;

    // check outcome
    let res: i64 = conn
        .fetch_one("SELECT some_payload FROM migrations_reversible_test")
        .await?
        .get(0);
    assert_eq!(res, 100);

    Ok(())
}

#[tokio::test]
async fn skip() -> anyhow::Result<()> {
    let (mut conn, _db) = test_conn().await?;
    clean_up(&mut conn).await?;
    let migrator = Migrator::new(fixture_dir().join("migrations_reversible")).await?;

    // get to the state of after the first migration manually
    let sql = include_str!("migrations_reversible/20220721124650_add_table.up.sql");
    let statements: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
    for statement in statements {
        conn.execute(statement).await?;
    }

    // skip first migration
    migrator.skip(&mut conn, Some(20220721124650)).await?;

    // check outcome
    let res: i64 = conn
        .fetch_one("SELECT some_payload FROM migrations_reversible_test")
        .await?
        .get(0);
    assert_eq!(res, 100);

    // run remaining migration
    migrator.run(&mut conn).await?;

    // check outcome
    let res: i64 = conn
        .fetch_one("SELECT some_payload FROM migrations_reversible_test")
        .await?
        .get(0);
    assert_eq!(res, 101);

    // roll back one version
    migrator.undo(&mut conn, 20220721124650).await?;

    // check outcome
    let res: i64 = conn
        .fetch_one("SELECT some_payload FROM migrations_reversible_test")
        .await?
        .get(0);
    assert_eq!(res, 100);

    Ok(())
}

#[tokio::test]
#[ignore = "turso: VACUUM is experimental and disabled by default"]
async fn no_tx() -> anyhow::Result<()> {
    let (mut conn, _db) = test_conn().await?;
    clean_up(&mut conn).await?;
    let migrator = Migrator::new(fixture_dir().join("migrations_no_tx")).await?;

    // run migration
    migrator.run(&mut conn).await?;

    Ok(())
}

#[tokio::test]
#[ignore = "turso: journal_mode cannot leave WAL (PRAGMA journal_mode = DELETE is a no-op)"]
async fn no_tx_reversible() -> anyhow::Result<()> {
    let (mut conn, _db) = test_conn().await?;
    clean_up(&mut conn).await?;

    let migrator = Migrator::new(fixture_dir().join("migrations_no_tx_reversible")).await?;

    // run migration
    migrator.run(&mut conn).await?;

    // check outcome
    let res: String = conn.fetch_one("PRAGMA JOURNAL_MODE").await?.get(0);
    assert_eq!(res, "wal".to_string());

    // roll back
    migrator.undo(&mut conn, -1).await?;

    // check outcome
    let res: String = conn.fetch_one("PRAGMA JOURNAL_MODE").await?.get(0);
    assert_eq!(res, "delete".to_string());

    Ok(())
}

/// Ensure that we have a clean initial state.
async fn clean_up(conn: &mut TursoConnection) -> anyhow::Result<()> {
    conn.execute("DROP TABLE migrations_simple_test").await.ok();
    conn.execute("DROP TABLE migrations_reversible_test")
        .await
        .ok();
    conn.execute("DROP TABLE _sqlx_migrations").await.ok();

    Ok(())
}
