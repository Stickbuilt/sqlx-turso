//! Regression tests for transaction bookkeeping, statement metadata, migrations and
//! connect-time validation.

use std::time::Duration;

use sqlx::{ConnectOptions, Connection, Executor, Row};
use sqlx_core::migrate::{Migration, MigrationType, Migrator};
use sqlx_core::sql_str::SqlStr;
use sqlx_core::transaction::TransactionManager;
use sqlx_turso::{
    TursoConnectOptions, TursoConnection, TursoError, TursoErrorKind, TursoPoolOptions,
    TursoTransactionManager,
};

fn turso_error(err: &sqlx::Error) -> &TursoError {
    err.as_database_error()
        .unwrap_or_else(|| panic!("not a database error: {err:?}"))
        .try_downcast_ref::<TursoError>()
        .expect("TursoError")
}

fn assert_aborted(err: &sqlx::Error) {
    let e = turso_error(err);
    assert_eq!(e.turso_kind(), TursoErrorKind::Aborted, "{e}");
    assert_eq!(e.sqlite_code(), 516);
}

async fn xs(conn: &mut TursoConnection, table: &str) -> Vec<i64> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT x FROM {table} ORDER BY x"
    )))
    .fetch_all(conn)
    .await
    .unwrap()
}

fn file_options(dir: &tempfile::TempDir, name: &str) -> TursoConnectOptions {
    TursoConnectOptions::new()
        .filename(dir.path().join(name))
        .create_if_missing(true)
}

// --- Stale column metadata ----------------------------------------------------------

#[tokio::test]
async fn columns_follow_schema_changes_from_another_connection() {
    let dir = tempfile::tempdir().unwrap();
    let opts = file_options(&dir, "schema.db");
    let mut a = TursoConnection::connect_with(&opts).await.unwrap();
    let mut b = TursoConnection::connect_with(&opts).await.unwrap();

    b.execute(
        "CREATE TABLE users (first_name TEXT, last_name TEXT);
         INSERT INTO users VALUES ('Ada', 'Lovelace');",
    )
    .await
    .unwrap();

    let query = "SELECT * FROM users WHERE ? = 1";
    let row = sqlx::query(query).bind(1).fetch_one(&mut a).await.unwrap();
    assert_eq!(row.get::<String, _>("first_name"), "Ada");

    // Same column count, different order.
    b.execute(
        "DROP TABLE users;
         CREATE TABLE users (last_name TEXT, first_name TEXT);
         INSERT INTO users VALUES ('Lovelace', 'Ada');",
    )
    .await
    .unwrap();
    let row = sqlx::query(query).bind(1).fetch_one(&mut a).await.unwrap();
    assert_eq!(row.get::<String, _>("first_name"), "Ada");
    assert_eq!(row.get::<String, _>("last_name"), "Lovelace");
    assert_eq!(row.get::<String, _>(0), "Lovelace");

    // More columns.
    b.execute("ALTER TABLE users ADD COLUMN born INTEGER; UPDATE users SET born = 1815")
        .await
        .unwrap();
    let row = sqlx::query(query).bind(1).fetch_one(&mut a).await.unwrap();
    assert_eq!(row.len(), 3);
    assert_eq!(row.get::<i64, _>("born"), 1815);

    // Fewer columns.
    b.execute("ALTER TABLE users DROP COLUMN last_name")
        .await
        .unwrap();
    let row = sqlx::query(query).bind(1).fetch_one(&mut a).await.unwrap();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get::<String, _>("first_name"), "Ada");
    assert_eq!(row.get::<i64, _>("born"), 1815);
    assert!(row.try_get::<String, _>("last_name").is_err());
}

#[tokio::test]
async fn columns_follow_schema_changes_on_the_same_connection() {
    let mut conn = TursoConnection::connect("turso::memory:").await.unwrap();
    conn.execute("CREATE TABLE t (a INTEGER, b TEXT); INSERT INTO t VALUES (1, 'x')")
        .await
        .unwrap();

    let query = "SELECT * FROM t WHERE a = ?";
    for _ in 0..3 {
        let row = sqlx::query(query)
            .bind(1)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            (row.get::<i64, _>("a"), row.get::<String, _>("b")),
            (1, "x".into())
        );
    }

    conn.execute("ALTER TABLE t ADD COLUMN c REAL; UPDATE t SET c = 2.5")
        .await
        .unwrap();
    let row = sqlx::query(query)
        .bind(1)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(row.get::<f64, _>("c"), 2.5);

    conn.execute("ALTER TABLE t DROP COLUMN b").await.unwrap();
    let row = sqlx::query(query)
        .bind(1)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(row.len(), 2);
    assert_eq!(row.get::<f64, _>(1), 2.5);
}

#[tokio::test]
async fn statement_cache_is_bounded_and_clearable() {
    let opts = TursoConnectOptions::new()
        .in_memory()
        .statement_cache_capacity(3);
    let mut conn = TursoConnection::connect_with(&opts).await.unwrap();
    conn.execute("CREATE TABLE t (x INTEGER)").await.unwrap();

    for i in 0..10_i64 {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO t VALUES (? + {i})"
        )))
        .bind(0_i64)
        .execute(&mut conn)
        .await
        .unwrap();
        // Repeated cached queries keep returning correct results.
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM t WHERE x >= ?")
            .bind(0_i64)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(n, i + 1);
        assert!(conn.cached_statements_size() <= 3);
    }
    assert_eq!(conn.cached_statements_size(), 3);
    conn.clear_cached_statements().await.unwrap();
    assert_eq!(conn.cached_statements_size(), 0);

    let sum: i64 = sqlx::query_scalar("SELECT sum(x) FROM t WHERE x >= ?")
        .bind(0_i64)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(sum, 45);
}

// --- Failed and cancelled begins ----------------------------------------------------

#[tokio::test]
async fn failed_nested_begin_leaves_outer_transaction_alone() {
    let mut conn = TursoConnection::connect("turso::memory:").await.unwrap();
    conn.execute("CREATE TABLE t (x INTEGER)").await.unwrap();

    let mut tx = conn.begin().await.unwrap();
    tx.execute("INSERT INTO t VALUES (1)").await.unwrap();
    // A custom BEGIN is only valid at the top level.
    assert!((&mut *tx).begin_with("BEGIN IMMEDIATE").await.is_err());
    assert_eq!(TursoTransactionManager::get_transaction_depth(&tx), 1);
    tx.execute("INSERT INTO t VALUES (2)").await.unwrap();
    tx.rollback().await.unwrap();
    // Both inserts belonged to the rolled-back transaction.
    assert_eq!(xs(&mut conn, "t").await, Vec::<i64>::new());

    let mut tx = conn.begin().await.unwrap();
    tx.execute("INSERT INTO t VALUES (1)").await.unwrap();
    assert!((&mut *tx).begin_with("BEGIN IMMEDIATE").await.is_err());
    tx.execute("INSERT INTO t VALUES (2)").await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(xs(&mut conn, "t").await, [1, 2]);
    assert!(!conn.is_in_transaction());
}

#[tokio::test]
async fn failed_top_level_begin_is_harmless() {
    let mut conn = TursoConnection::connect("turso::memory:").await.unwrap();
    conn.execute("CREATE TABLE t (x INTEGER)").await.unwrap();

    // Runs, but doesn't open a transaction.
    let err = conn.begin_with("SELECT 1").await.unwrap_err();
    assert!(matches!(err, sqlx::Error::BeginFailed), "{err:?}");
    assert!(!conn.is_in_transaction());
    assert!(conn.begin_with("NOT SQL").await.is_err());
    assert!(!conn.is_in_transaction());

    let mut tx = conn.begin().await.unwrap();
    tx.execute("INSERT INTO t VALUES (1)").await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(xs(&mut conn, "t").await, [1]);
}

#[tokio::test]
async fn commit_and_rollback_without_transaction_fail() {
    let mut conn = TursoConnection::connect("turso::memory:").await.unwrap();
    let err = TursoTransactionManager::commit(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(turso_error(&err).turso_kind(), TursoErrorKind::Misuse);
    let err = TursoTransactionManager::rollback(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(turso_error(&err).turso_kind(), TursoErrorKind::Misuse);
    conn.ping().await.unwrap();
}

#[tokio::test]
async fn cancelled_begin_does_not_affect_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let opts = file_options(&dir, "busy.db").busy_timeout(Duration::from_secs(5));
    let mut a = TursoConnection::connect_with(&opts).await.unwrap();
    let mut b = TursoConnection::connect_with(&opts).await.unwrap();
    a.execute("CREATE TABLE t (x INTEGER)").await.unwrap();

    let mut a_tx = a.begin_with("BEGIN IMMEDIATE").await.unwrap();
    a_tx.execute("INSERT INTO t VALUES (1)").await.unwrap();

    // B waits on A's write lock; drop its BEGIN mid-flight.
    let cancelled =
        tokio::time::timeout(Duration::from_millis(200), b.begin_with("BEGIN IMMEDIATE"))
            .await
            .is_err();
    assert!(cancelled, "BEGIN IMMEDIATE should have been waiting");
    assert!(!b.is_in_transaction());

    a_tx.commit().await.unwrap();

    // B settles the cancelled BEGIN and works normally.
    assert_eq!(xs(&mut b, "t").await, [1]);
    assert!(!b.is_in_transaction());
    let mut b_tx = b.begin_with("BEGIN IMMEDIATE").await.unwrap();
    b_tx.execute("INSERT INTO t VALUES (2)").await.unwrap();
    b_tx.commit().await.unwrap();
    assert_eq!(xs(&mut a, "t").await, [1, 2]);
    b.ping().await.unwrap();
}

#[tokio::test]
async fn queued_rollbacks_run_in_order() {
    let mut conn = TursoConnection::connect("turso::memory:").await.unwrap();
    conn.execute("CREATE TABLE t (x INTEGER)").await.unwrap();

    // Inner level dropped; outer continues and commits.
    {
        let mut tx = conn.begin().await.unwrap();
        tx.execute("INSERT INTO t VALUES (1)").await.unwrap();
        {
            let mut inner = tx.begin().await.unwrap();
            inner.execute("INSERT INTO t VALUES (2)").await.unwrap();
            {
                let mut innermost = inner.begin().await.unwrap();
                innermost.execute("INSERT INTO t VALUES (3)").await.unwrap();
            }
            // Two levels dropped without an operation in between.
        }
        assert_eq!(TursoTransactionManager::get_transaction_depth(&tx), 1);
        tx.execute("INSERT INTO t VALUES (4)").await.unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(xs(&mut conn, "t").await, [1, 4]);

    // All levels dropped; flushed by the next operation.
    {
        let mut tx = conn.begin().await.unwrap();
        tx.execute("INSERT INTO t VALUES (5)").await.unwrap();
        let mut inner = tx.begin().await.unwrap();
        inner.execute("INSERT INTO t VALUES (6)").await.unwrap();
        drop(inner);
        drop(tx);
    }
    assert!(!conn.is_in_transaction());
    conn.ping().await.unwrap();
    assert_eq!(xs(&mut conn, "t").await, [1, 4]);
}

// --- Engine-aborted transactions ----------------------------------------------------

#[tokio::test]
async fn statements_after_engine_rollback_do_not_autocommit() {
    let mut conn = TursoConnection::connect("turso::memory:").await.unwrap();
    conn.execute("CREATE TABLE t (x INTEGER)").await.unwrap();

    // The engine leaves the transaction behind the driver's back.
    let mut tx = conn.begin().await.unwrap();
    tx.execute("INSERT INTO t VALUES (1)").await.unwrap();
    tx.execute("ROLLBACK").await.unwrap();
    for _ in 0..2 {
        let err = tx.execute("INSERT INTO t VALUES (2)").await.unwrap_err();
        assert_aborted(&err);
    }
    assert!(
        tx.begin().await.is_err(),
        "savepoint must not open a new transaction"
    );
    tx.execute("SELECT 1").await.unwrap_err();
    tx.rollback().await.unwrap();
    assert_eq!(xs(&mut conn, "t").await, Vec::<i64>::new());

    // Commit of an aborted transaction fails; the dropped transaction is cleaned up.
    let mut tx = conn.begin().await.unwrap();
    tx.execute("ROLLBACK").await.unwrap();
    let err = tx.commit().await.unwrap_err();
    assert_aborted(&err);
    assert!(!conn.is_in_transaction());

    // Nested: every level keeps failing until it is rolled back.
    let mut tx = conn.begin().await.unwrap();
    tx.execute("INSERT INTO t VALUES (1)").await.unwrap();
    {
        let mut inner = tx.begin().await.unwrap();
        inner.execute("ROLLBACK").await.unwrap();
        assert_aborted(&inner.execute("INSERT INTO t VALUES (2)").await.unwrap_err());
        inner.rollback().await.unwrap();
    }
    assert_aborted(&tx.execute("INSERT INTO t VALUES (3)").await.unwrap_err());
    {
        // A dropped inner level must not reset the outer one either.
        let _ = tx.begin().await;
    }
    assert_aborted(&tx.execute("INSERT INTO t VALUES (4)").await.unwrap_err());
    drop(tx);

    assert_eq!(xs(&mut conn, "t").await, Vec::<i64>::new());
    conn.execute("INSERT INTO t VALUES (9)").await.unwrap();
    assert_eq!(xs(&mut conn, "t").await, [9]);
    assert!(!conn.is_in_transaction());
}

#[tokio::test]
async fn write_conflict_aborts_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let opts = file_options(&dir, "conflict.db").concurrent_writes(true);
    let pool = TursoPoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await
        .unwrap();
    pool.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO t VALUES (1, 0);
         CREATE TABLE log (x INTEGER)",
    )
    .await
    .unwrap();

    let mut a = pool.begin().await.unwrap();
    let mut b = pool.begin().await.unwrap();
    a.execute("UPDATE t SET v = 1 WHERE id = 1").await.unwrap();
    let err = b
        .execute("UPDATE t SET v = 2 WHERE id = 1")
        .await
        .expect_err("conflicting write should fail");
    let e = turso_error(&err);
    assert!(e.is_conflict(), "{e}");
    assert_eq!(e.sqlite_code(), 517, "{e}");
    assert_eq!(
        err.as_database_error().unwrap().code().as_deref(),
        Some("517")
    );

    // B's transaction is gone; later statements must not autocommit.
    assert_aborted(&b.execute("INSERT INTO log VALUES (2)").await.unwrap_err());
    assert_aborted(&b.execute("INSERT INTO log VALUES (3)").await.unwrap_err());
    b.rollback().await.unwrap();
    a.execute("INSERT INTO log VALUES (1)").await.unwrap();
    a.commit().await.unwrap();

    let log: Vec<i64> = sqlx::query_scalar("SELECT x FROM log ORDER BY x")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(log, [1]);
    let v: i64 = sqlx::query_scalar("SELECT v FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 1);
}

#[tokio::test]
async fn begin_modes_and_savepoints_still_work() {
    let dir = tempfile::tempdir().unwrap();
    for concurrent in [false, true] {
        let name = format!("modes-{concurrent}.db");
        let opts = file_options(&dir, &name).concurrent_writes(concurrent);
        let mut conn = TursoConnection::connect_with(&opts).await.unwrap();
        conn.execute("CREATE TABLE t (x INTEGER)").await.unwrap();

        let mut modes = vec!["BEGIN", "BEGIN DEFERRED", "BEGIN IMMEDIATE"];
        if concurrent {
            modes.push("BEGIN CONCURRENT");
        } else {
            modes.push("BEGIN EXCLUSIVE");
        }
        for (i, mode) in modes.into_iter().enumerate() {
            let i = i as i64 * 10;
            let mut tx = conn.begin_with(mode).await.unwrap();
            sqlx::query("INSERT INTO t VALUES (?)")
                .bind(i)
                .execute(&mut *tx)
                .await
                .unwrap();
            {
                let mut sp = tx.begin().await.unwrap();
                sqlx::query("INSERT INTO t VALUES (?)")
                    .bind(i + 1)
                    .execute(&mut *sp)
                    .await
                    .unwrap();
                sp.rollback().await.unwrap();
            }
            {
                let mut sp = tx.begin().await.unwrap();
                sqlx::query("INSERT INTO t VALUES (?)")
                    .bind(i + 2)
                    .execute(&mut *sp)
                    .await
                    .unwrap();
                sp.commit().await.unwrap();
            }
            tx.commit().await.unwrap();
        }
        assert_eq!(xs(&mut conn, "t").await, [0, 2, 10, 12, 20, 22, 30, 32]);
    }
}

// --- Migrations under BEGIN CONCURRENT ----------------------------------------------

fn migrations() -> Migrator {
    Migrator::with_migrations(vec![
        Migration::new(
            1,
            "create".into(),
            MigrationType::ReversibleUp,
            SqlStr::from_static("CREATE TABLE m (id INTEGER PRIMARY KEY, v TEXT)"),
            false,
        ),
        Migration::new(
            1,
            "create".into(),
            MigrationType::ReversibleDown,
            SqlStr::from_static("DROP TABLE m"),
            false,
        ),
        Migration::new(
            2,
            "alter".into(),
            MigrationType::ReversibleUp,
            SqlStr::from_static(
                "ALTER TABLE m ADD COLUMN w INTEGER; INSERT INTO m (v) VALUES ('x')",
            ),
            false,
        ),
        Migration::new(
            2,
            "alter".into(),
            MigrationType::ReversibleDown,
            SqlStr::from_static("ALTER TABLE m DROP COLUMN w"),
            false,
        ),
    ])
}

#[tokio::test]
async fn migrations_run_with_concurrent_default_begin() {
    let dir = tempfile::tempdir().unwrap();
    let opts = file_options(&dir, "migrate.db").concurrent_writes(true);
    let mut conn = TursoConnection::connect_with(&opts).await.unwrap();

    // DDL is refused in the configured default transaction mode...
    let mut tx = conn.begin().await.unwrap();
    assert!(tx.execute("CREATE TABLE x (a INTEGER)").await.is_err());
    tx.rollback().await.unwrap();

    // ...but migrations use an exclusive transaction.
    let migrator = migrations();
    migrator.run(&mut conn).await.unwrap();
    let row = sqlx::query("SELECT * FROM m")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(row.len(), 3);
    migrator.undo(&mut conn, 0).await.unwrap();
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE name = 'm'")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert!(!conn.is_in_transaction());
}

// --- Statement parameters -----------------------------------------------------------

#[tokio::test]
async fn statement_parameters_count_distinct_arguments() {
    use sqlx::Statement;
    let mut conn = TursoConnection::connect("turso::memory:").await.unwrap();
    conn.execute("CREATE TABLE t (x INTEGER)").await.unwrap();

    for (sql, expected) in [
        ("INSERT INTO t VALUES ($1); SELECT x FROM t WHERE x = $1", 1),
        ("INSERT INTO t VALUES (?); SELECT x FROM t WHERE x = ?", 2),
        ("SELECT ?1, ?2, ?1", 2),
    ] {
        let stmt = conn.prepare(SqlStr::from_static(sql)).await.unwrap();
        assert_eq!(stmt.parameters().unwrap().right(), Some(expected), "{sql}");
    }

    // Binding exactly that many arguments works.
    let stmt = conn
        .prepare(SqlStr::from_static(
            "INSERT INTO t VALUES ($1); SELECT x FROM t WHERE x = $1",
        ))
        .await
        .unwrap();
    let x: i64 = stmt
        .query_scalar()
        .bind(7_i64)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(x, 7);
}

// --- REAL rendering -----------------------------------------------------------------

#[tokio::test]
async fn real_text_rendering_matches_engine_cast() {
    let mut conn = TursoConnection::connect("turso::memory:").await.unwrap();
    for v in [
        0.00001234_f64,
        0.0001234,
        0.0001,
        0.00009999,
        1.5e-7,
        -0.00001,
        1.0,
        0.1,
        1.0 / 3.0,
        123456789012345.0,
        1e15,
        1e20,
        -2.5e100,
    ] {
        let row = sqlx::query("SELECT ?1 AS r, CAST(?1 AS TEXT) AS t")
            .bind(v)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        let ours = row.get_unchecked::<&str, _>("r");
        let engine = row.get::<String, _>("t");
        assert_eq!(ours, engine, "{v:e}");
    }
}

// --- Remote configuration (no network needed) ---------------------------------------

#[cfg(feature = "serverless")]
#[tokio::test]
async fn remote_rejects_per_connection_settings_and_bad_urls() {
    use sqlx_core::connection::ConnectOptions;

    fn config_error(result: Result<TursoConnection, sqlx::Error>) -> String {
        match result {
            Err(sqlx::Error::Configuration(e)) => e.to_string(),
            other => panic!("expected configuration error, got {other:?}"),
        }
    }

    let base = TursoConnectOptions::new().remote("libsql://example.invalid");
    let msg = config_error(base.clone().pragma("cache_size", "100").connect().await);
    assert!(msg.contains("pragma"), "{msg}");
    let msg = config_error(base.clone().foreign_keys(true).connect().await);
    assert!(msg.contains("foreign_keys"), "{msg}");

    for url in [
        "not a url",
        "postgres://db.example.invalid",
        "turso:data.db",
        "libsql://db.example.invalid?authToken=secret",
    ] {
        let opts = TursoConnectOptions::new().remote(url);
        let _ = opts.to_url_lossy();
        let msg = config_error(opts.connect().await);
        assert!(msg.contains("Turso Cloud URL"), "{url}: {msg}");
        assert!(!msg.contains("secret"), "{msg}");
    }

    // Auth tokens never go over plain http:// except to a local development server.
    let msg = config_error(
        TursoConnectOptions::new()
            .remote("http://db.example.invalid")
            .auth_token("secret")
            .connect()
            .await,
    );
    assert!(msg.contains("unencrypted"), "{msg}");
    #[cfg(feature = "sync")]
    {
        let dir = tempfile::tempdir().unwrap();
        let msg = config_error(
            TursoConnectOptions::new()
                .filename(dir.path().join("replica.db"))
                .create_if_missing(true)
                .sync_url("http://db.example.invalid")
                .auth_token("secret")
                .connect()
                .await,
        );
        assert!(msg.contains("unencrypted"), "{msg}");
    }
    for url in ["http://localhost:1", "http://127.0.0.1:1", "http://[::1]:1"] {
        let result = TursoConnectOptions::new()
            .remote(url)
            .auth_token("secret")
            .connect()
            .await;
        assert!(
            !matches!(result, Err(sqlx::Error::Configuration(_))),
            "{url}: {result:?}"
        );
    }
}

#[cfg(feature = "serverless")]
#[tokio::test]
async fn remote_connect_contacts_the_server() {
    // Nothing listens here; establishing the connection must fail.
    let opts = TursoConnectOptions::new()
        .remote("http://127.0.0.1:9")
        .auth_token("x");
    assert!(TursoConnection::connect_with(&opts).await.is_err());
    let pool = TursoPoolOptions::new()
        .acquire_timeout(Duration::from_secs(2))
        .connect_with(opts)
        .await;
    assert!(pool.is_err());
}

/// A write that fails when it completes (here: a deferred foreign key checked at commit) must
/// not report success just because its first `RETURNING` row was already produced.
#[tokio::test]
async fn returning_write_failing_at_completion_is_an_error() {
    let mut conn = TursoConnection::connect("turso::memory:").await.unwrap();
    conn.execute(
        "CREATE TABLE p(id INTEGER PRIMARY KEY);
         CREATE TABLE c(id INTEGER PRIMARY KEY,
                        p INT REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED);",
    )
    .await
    .unwrap();

    let res = sqlx::query("INSERT INTO c(p) VALUES (99) RETURNING id")
        .fetch_one(&mut conn)
        .await;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM c")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let err = res.map(|r| r.get::<i64, _>(0)).expect_err("the insert was rolled back");
    assert_eq!(
        err.as_database_error().map(|e| e.kind()),
        Some(sqlx::error::ErrorKind::ForeignKeyViolation),
        "{err}"
    );
    assert_eq!(count, 0);

    // A successful RETURNING write read through fetch_one is committed.
    conn.execute("INSERT INTO p VALUES (99)").await.unwrap();
    let id: i64 = sqlx::query_scalar("INSERT INTO c(p) VALUES (99) RETURNING id")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let stored: i64 = sqlx::query_scalar("SELECT id FROM c")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(stored, id);
}

/// Dropping a database must remove the engine's MVCC log so a recreated database starts empty.
#[tokio::test]
async fn drop_database_removes_mvcc_log() {
    use sqlx_core::migrate::MigrateDatabase;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.sqlite");
    let opts = || {
        TursoConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .concurrent_writes(true)
    };
    {
        let mut conn = opts().connect().await.unwrap();
        conn.execute("CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1)")
            .await
            .unwrap();
        conn.close().await.unwrap();
    }
    let before: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    let url = format!("turso:{}", path.display());
    sqlx_turso::Turso::drop_database(&url).await.unwrap();
    let after: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(after.is_empty(), "before drop: {before:?}; left after drop: {after:?}");
}

/// `app.sqlite` and `app.db` share turso's MVCC log; dropping one must not delete it.
#[tokio::test]
async fn drop_database_refuses_shared_mvcc_log() {
    use sqlx_core::migrate::MigrateDatabase;
    let dir = tempfile::tempdir().unwrap();
    let sqlite = dir.path().join("app.sqlite");
    std::fs::write(&sqlite, b"").unwrap();
    std::fs::write(dir.path().join("app.db"), b"").unwrap();
    std::fs::write(dir.path().join("app.db-log"), b"x").unwrap();

    let err = sqlx_turso::Turso::drop_database(&format!("turso:{}", sqlite.display()))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("shared"), "{err}");
    assert!(sqlite.exists() && dir.path().join("app.db-log").exists());
}
