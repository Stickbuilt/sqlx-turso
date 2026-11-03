//! Port of `sqlx/tests/sqlite/sqlite.rs`.
//!
//! Not ported (SQLite C APIs with no Turso equivalent): `it_opens_with_extension`
//! (`load_extension`), `it_supports_collations`, the progress / update / commit / rollback /
//! preupdate hook tests, `test_get_last_error` (`LockedSqliteHandle`) and the
//! `serialize` / `deserialize` tests. `it_can_use_transaction_options` is adapted: the
//! transaction state read via `sqlite3_txn_state` is not available.

use futures_util::TryStreamExt;
use rand::{RngExt, SeedableRng, rngs::Xoshiro256PlusPlus};
use sqlx::SqlSafeStr;
use sqlx::{Column, ConnectOptions, Connection, Executor, Row, Statement, TypeInfo, query};
use sqlx_turso::{
    Turso, TursoConnectOptions, TursoConnection, TursoPool, TursoPoolOptions, TursoRow,
};
use std::future::Future;
use std::time::Duration;

use crate::common::{fixture_url, memory, new};

#[tokio::test]
async fn it_connects() -> anyhow::Result<()> {
    Ok(memory().await?.ping().await?)
}

#[tokio::test]
async fn it_fetches_and_inflates_row() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    // process rows, one-at-a-time
    // this reuses the memory of the row

    {
        let expected = [15, 39, 51];
        let mut i = 0;
        let mut s = conn.fetch("SELECT 15 UNION SELECT 51 UNION SELECT 39");

        while let Some(row) = s.try_next().await? {
            let v1 = row.get::<i32, _>(0);
            assert_eq!(expected[i], v1);
            i += 1;
        }
    }

    // same query, but fetch all rows at once
    // this triggers the internal inflation

    let rows = conn
        .fetch_all("SELECT 15 UNION SELECT 51 UNION SELECT 39")
        .await?;

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i32, _>(0), 15);
    assert_eq!(rows[1].get::<i32, _>(0), 39);
    assert_eq!(rows[2].get::<i32, _>(0), 51);

    // same query but fetch the first row a few times from a non-persistent query
    // these rows should be immediately inflated

    let row1 = conn
        .fetch_one("SELECT 15 UNION SELECT 51 UNION SELECT 39")
        .await?;

    assert_eq!(row1.get::<i32, _>(0), 15);

    let row2 = conn
        .fetch_one("SELECT 15 UNION SELECT 51 UNION SELECT 39")
        .await?;

    assert_eq!(row1.get::<i32, _>(0), 15);
    assert_eq!(row2.get::<i32, _>(0), 15);

    // same query (again) but make it persistent
    // and fetch the first row a few times

    let row1 = conn
        .fetch_one(query("SELECT 15 UNION SELECT 51 UNION SELECT 39"))
        .await?;

    assert_eq!(row1.get::<i32, _>(0), 15);

    let row2 = conn
        .fetch_one(query("SELECT 15 UNION SELECT 51 UNION SELECT 39"))
        .await?;

    assert_eq!(row1.get::<i32, _>(0), 15);
    assert_eq!(row2.get::<i32, _>(0), 15);

    Ok(())
}

#[tokio::test]
async fn it_maths() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    let value = sqlx::query("select 1 + ?1")
        .bind(5_i32)
        .try_map(|row: TursoRow| row.try_get::<i32, _>(0))
        .fetch_one(&mut conn)
        .await?;

    assert_eq!(6i32, value);

    Ok(())
}

#[tokio::test]
async fn test_bind_multiple_statements_multiple_values() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    let values: Vec<i32> = sqlx::query_scalar::<_, i32>("select ?; select ?")
        .bind(5_i32)
        .bind(15_i32)
        .fetch_all(&mut conn)
        .await?;

    assert_eq!(values.len(), 2);
    assert_eq!(values[0], 5);
    assert_eq!(values[1], 15);

    Ok(())
}

#[tokio::test]
async fn test_bind_multiple_statements_same_value() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    let values: Vec<i32> = sqlx::query_scalar::<_, i32>("select ?1; select ?1")
        .bind(25_i32)
        .fetch_all(&mut conn)
        .await?;

    assert_eq!(values.len(), 2);
    assert_eq!(values[0], 25);
    assert_eq!(values[1], 25);

    Ok(())
}

#[tokio::test]
async fn it_can_describe_with_pragma() -> anyhow::Result<()> {
    use sqlx::{Decode, TypeInfo, ValueRef};

    let (mut conn, _db) = new().await?;

    let defaults = sqlx::query("pragma table_info (tweet)")
        .try_map(|row: TursoRow| {
            let val = row.try_get_raw("dflt_value")?;
            let ty = val.type_info().clone().into_owned();

            let val: Option<i32> = Decode::<Turso>::decode(val).map_err(sqlx::Error::Decode)?;

            if val.is_some() {
                assert_eq!(ty.name(), "TEXT");
            }

            Ok(val)
        })
        .fetch_all(&mut conn)
        .await?;

    assert_eq!(defaults[0], None);
    assert_eq!(defaults[2], Some(0));

    Ok(())
}

#[tokio::test]
async fn it_binds_positional_parameters_issue_467() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    let row: (i32, i32, i32, i32) = sqlx::query_as("select ?1, ?1, ?3, ?2")
        .bind(5_i32)
        .bind(500_i32)
        .bind(1020_i32)
        .fetch_one(&mut conn)
        .await?;

    assert_eq!(row.0, 5);
    assert_eq!(row.1, 5);
    assert_eq!(row.2, 1020);
    assert_eq!(row.3, 500);

    Ok(())
}

#[tokio::test]
async fn it_fetches_in_loop() -> anyhow::Result<()> {
    // this is trying to check for any data races
    // there were a few that triggered *sometimes* while building out StatementWorker
    for _ in 0..1000_usize {
        let (mut conn, _db) = new().await?;
        let v: Vec<(i32,)> = sqlx::query_as("SELECT 1").fetch_all(&mut conn).await?;

        assert_eq!(v[0].0, 1);
    }

    Ok(())
}

#[tokio::test]
async fn it_executes_with_pool() -> anyhow::Result<()> {
    let (url, _db) = fixture_url()?;
    let pool: TursoPool = TursoPoolOptions::new()
        .min_connections(2)
        .max_connections(2)
        .test_before_acquire(false)
        .connect(&url)
        .await?;

    let rows = pool.fetch_all("SELECT 1; SElECT 2").await?;

    assert_eq!(rows.len(), 2);

    Ok(())
}

#[tokio::test]
async fn it_opens_in_memory() -> anyhow::Result<()> {
    // If the filename is ":memory:", then a private, temporary in-memory database
    // is created for the connection.
    let conn = TursoConnection::connect("turso::memory:").await?;
    conn.close().await?;

    Ok(())
}

#[tokio::test]
async fn it_opens_temp_on_disk() -> anyhow::Result<()> {
    // sqlx-sqlite opens "" as a private, temporary on-disk database. The Turso URL with
    // an empty path opens a private in-memory database.
    let conn = TursoConnection::connect("turso:").await?;
    conn.close().await?;

    Ok(())
}

#[tokio::test]
async fn it_fails_to_parse() -> anyhow::Result<()> {
    let mut conn = memory().await?;
    let res = sqlx::raw_sql("SEELCT 1").execute(&mut conn).await;

    assert!(res.is_err());

    let err = res.unwrap_err().to_string();

    assert_eq!(
        "error returned from database: (code: 1) near \"SEELCT\": syntax error",
        err
    );

    Ok(())
}

#[tokio::test]
async fn it_handles_empty_queries() -> anyhow::Result<()> {
    let mut conn = memory().await?;
    let done = conn.execute("").await?;

    assert_eq!(done.rows_affected(), 0);

    Ok(())
}

#[tokio::test]
async fn it_binds_parameters() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    let v: i32 = sqlx::query_scalar("SELECT ?")
        .bind(10_i32)
        .fetch_one(&mut conn)
        .await?;

    assert_eq!(v, 10);

    let v: (i32, i32) = sqlx::query_as("SELECT ?1, ?")
        .bind(10_i32)
        .fetch_one(&mut conn)
        .await?;

    assert_eq!(v.0, 10);
    assert_eq!(v.1, 10);

    Ok(())
}

#[tokio::test]
async fn it_binds_dollar_parameters() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    let v: (i32, i32) = sqlx::query_as("SELECT $1, $2")
        .bind(10_i32)
        .bind(11_i32)
        .fetch_one(&mut conn)
        .await?;

    assert_eq!(v.0, 10);
    assert_eq!(v.1, 11);

    Ok(())
}

#[tokio::test]
async fn it_executes_queries() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    let _ = conn
        .execute(
            r#"
CREATE TEMPORARY TABLE users (id INTEGER PRIMARY KEY)
            "#,
        )
        .await?;

    for index in 1..=10_i32 {
        let done = sqlx::query("INSERT INTO users (id) VALUES (?)")
            .bind(index * 2)
            .execute(&mut conn)
            .await?;

        assert_eq!(done.rows_affected(), 1);
    }

    let sum: i32 = sqlx::query_as("SELECT id FROM users")
        .fetch(&mut conn)
        .try_fold(0_i32, |acc, (x,): (i32,)| async move { Ok(acc + x) })
        .await?;

    assert_eq!(sum, 110);

    Ok(())
}

#[tokio::test]
async fn it_can_execute_multiple_statements() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    let done = conn
        .execute(
            r#"
CREATE TEMPORARY TABLE users (id INTEGER PRIMARY KEY, other INTEGER);
INSERT INTO users DEFAULT VALUES;
            "#,
        )
        .await?;

    assert_eq!(done.rows_affected(), 1);

    for index in 2..5_i32 {
        let (id, other): (i32, i32) = sqlx::query_as(
            r#"
INSERT INTO users (other) VALUES (?);
SELECT id, other FROM users WHERE id = last_insert_rowid();
            "#,
        )
        .bind(index)
        .fetch_one(&mut conn)
        .await?;

        assert_eq!(id, index);
        assert_eq!(other, index);
    }

    Ok(())
}

#[tokio::test]
async fn it_interleaves_reads_and_writes() -> anyhow::Result<()> {
    let (mut conn, _db) = new().await?;

    let mut cursor = conn.fetch(
        "
CREATE TABLE IF NOT EXISTS _sqlx_test (
    id INT PRIMARY KEY,
    text TEXT NOT NULL
);

SELECT 'Hello World' as _1;

INSERT INTO _sqlx_test (text) VALUES ('this is a test');

SELECT id, text FROM _sqlx_test;
    ",
    );

    let row = cursor.try_next().await?.unwrap();

    assert!("Hello World" == row.try_get::<&str, _>("_1")?);

    let row = cursor.try_next().await?.unwrap();

    let id: i64 = row.try_get("id")?;
    let text: &str = row.try_get("text")?;

    assert_eq!(0, id);
    assert_eq!("this is a test", text);

    Ok(())
}

#[tokio::test]
async fn it_caches_statements() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    // Initial PRAGMAs are not cached as we are not going to execute
    // them more than once.
    assert_eq!(0, conn.cached_statements_size());

    // `&str` queries are not persistent.
    let row = conn.fetch_one("SELECT 100 AS val").await?;
    let val: i32 = row.get("val");
    assert_eq!(val, 100);
    assert_eq!(0, conn.cached_statements_size());

    // `Query` is persistent by default.
    let mut conn = memory().await?;
    for i in 0..2 {
        let row = sqlx::query("SELECT ? AS val")
            .bind(i)
            .fetch_one(&mut conn)
            .await?;

        let val: i32 = row.get("val");

        assert_eq!(i, val);
    }
    assert_eq!(1, conn.cached_statements_size());

    // Cache can be cleared.
    conn.clear_cached_statements().await?;
    assert_eq!(0, conn.cached_statements_size());

    // `Query` is not persistent if `.persistent(false)` is used
    // explicitly.
    let mut conn = memory().await?;
    for i in 0..2 {
        let row = sqlx::query("SELECT ? AS val")
            .bind(i)
            .persistent(false)
            .fetch_one(&mut conn)
            .await?;

        let val: i32 = row.get("val");

        assert_eq!(i, val);
    }
    assert_eq!(0, conn.cached_statements_size());

    Ok(())
}

#[tokio::test]
async fn it_can_prepare_then_execute() -> anyhow::Result<()> {
    let (mut conn, _db) = new().await?;
    let mut tx = conn.begin().await?;

    let _ = sqlx::query("INSERT INTO tweet ( id, text ) VALUES ( 2, 'Hello, World' )")
        .execute(&mut *tx)
        .await?;

    let tweet_id: i32 = 2;

    let statement = tx
        .prepare("SELECT * FROM tweet WHERE id = ?1".into_sql_str())
        .await?;

    assert_eq!(statement.column(0).name(), "id");
    assert_eq!(statement.column(1).name(), "text");
    assert_eq!(statement.column(2).name(), "is_sent");
    assert_eq!(statement.column(3).name(), "owner_id");

    assert_eq!(statement.column(0).type_info().name(), "INTEGER");
    assert_eq!(statement.column(1).type_info().name(), "TEXT");
    assert_eq!(statement.column(2).type_info().name(), "BOOLEAN");
    assert_eq!(statement.column(3).type_info().name(), "INTEGER");

    let row = statement.query().bind(tweet_id).fetch_one(&mut *tx).await?;
    let tweet_text: &str = row.try_get("text")?;

    assert_eq!(tweet_text, "Hello, World");

    Ok(())
}

#[tokio::test]
async fn it_resets_prepared_statement_after_fetch_one() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    conn.execute("CREATE TEMPORARY TABLE foobar (id INTEGER)")
        .await?;
    conn.execute("INSERT INTO foobar VALUES (42)").await?;

    let r = sqlx::query("SELECT id FROM foobar")
        .fetch_one(&mut conn)
        .await?;
    let x: i32 = r.try_get("id")?;
    assert_eq!(x, 42);

    conn.execute("DROP TABLE foobar").await?;

    Ok(())
}

#[tokio::test]
async fn it_resets_prepared_statement_after_fetch_many() -> anyhow::Result<()> {
    let mut conn = memory().await?;

    conn.execute("CREATE TEMPORARY TABLE foobar (id INTEGER)")
        .await?;
    conn.execute("INSERT INTO foobar VALUES (42)").await?;
    conn.execute("INSERT INTO foobar VALUES (43)").await?;

    let mut rows = sqlx::query("SELECT id FROM foobar").fetch(&mut conn);
    let row = rows.try_next().await?.unwrap();
    let x: i32 = row.try_get("id")?;
    assert_eq!(x, 42);
    drop(rows);

    conn.execute("DROP TABLE foobar").await?;

    Ok(())
}

// https://github.com/launchbadge/sqlx/issues/1300
#[tokio::test]
async fn concurrent_resets_dont_segfault() {
    use std::str::FromStr;

    let mut conn = TursoConnectOptions::from_str("turso::memory:")
        .unwrap()
        .connect()
        .await
        .unwrap();

    sqlx::query("CREATE TABLE stuff (name INTEGER, value INTEGER)")
        .execute(&mut conn)
        .await
        .unwrap();

    tokio::spawn(async move {
        for i in 0..1000 {
            sqlx::query("INSERT INTO stuff (name, value) VALUES (?, ?)")
                .bind(i)
                .bind(0)
                .execute(&mut conn)
                .await
                .unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(1)).await;
}

// https://github.com/launchbadge/sqlx/issues/1419
// note: this passes before and after the fix; you need to run it with `--nocapture`
// to see the panic from the worker thread, which doesn't happen after the fix
#[tokio::test]
async fn row_dropped_after_connection_doesnt_panic() {
    let mut conn = TursoConnection::connect("turso::memory:").await.unwrap();

    let books = sqlx::query("SELECT 'hello' AS title")
        .fetch_all(&mut conn)
        .await
        .unwrap();

    for book in &books {
        // force the row to be inflated
        let _title: String = book.get("title");
    }

    // hold `books` past the lifetime of `conn`
    drop(conn);
    tokio::time::sleep(Duration::from_secs(1)).await;
    drop(books);
}

// note: to repro issue #1467 this should be run in release mode
// May spuriously fail with UNIQUE constraint failures (which aren't relevant to the original issue)
// which I have tried to reproduce using the same seed as printed from CI but to no avail.
// It may be due to some nondeterminism in SQLite itself for all I know.
#[tokio::test]
#[ignore]
async fn issue_1467() -> anyhow::Result<()> {
    let mut conn = TursoConnectOptions::new().in_memory().connect().await?;

    sqlx::query(
        r#"
    CREATE TABLE kv (k PRIMARY KEY, v);
    CREATE INDEX idx_kv ON kv (v);
    "#,
    )
    .execute(&mut conn)
    .await?;

    // Random seed:
    let seed: [u8; 32] = rand::random();
    println!("RNG seed: {}", hex::encode(seed));

    // Pre-determined seed:
    // let mut seed: [u8; 32] = [0u8; 32];
    // hex::decode_to_slice(
    //     "135234871d03fc0479e22f2f06395b6074761bac5fe7dcf205dbe01eef9f7794",
    //     &mut seed,
    // )?;

    // reproducible RNG for testing
    let mut rng = Xoshiro256PlusPlus::from_seed(seed);

    for i in 0..1_000_000 {
        if i % 1_000 == 0 {
            println!("{i}");
        }
        let key = rng.random_range(0..1_000);
        let value = rng.random_range(0..1_000);
        let mut tx = conn.begin().await?;

        let exists = sqlx::query("SELECT 1 FROM kv WHERE k = ?")
            .bind(key)
            .fetch_optional(&mut *tx)
            .await?;
        if exists.is_some() {
            sqlx::query("UPDATE kv SET v = ? WHERE k = ?")
                .bind(value)
                .bind(key)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query("INSERT INTO kv(k, v) VALUES (?, ?)")
                .bind(key)
                .bind(value)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_read_and_write() {
    let pool: TursoPool = TursoPoolOptions::new()
        .min_connections(2)
        .connect("turso::memory:")
        .await
        .unwrap();

    sqlx::query("CREATE TABLE kv (k PRIMARY KEY, v)")
        .execute(&pool)
        .await
        .unwrap();

    let n = 100;

    let read = tokio::spawn({
        let mut conn = pool.acquire().await.unwrap();

        async move {
            for i in 0u32..n {
                sqlx::query("SELECT v FROM kv")
                    .bind(i)
                    .fetch_all(&mut *conn)
                    .await
                    .unwrap();
            }
        }
    });

    let write = tokio::spawn({
        let mut conn = pool.acquire().await.unwrap();

        async move {
            for i in 0u32..n {
                sqlx::query("INSERT INTO kv (k, v) VALUES (?, ?)")
                    .bind(i)
                    .bind(i * i)
                    .execute(&mut *conn)
                    .await
                    .unwrap();
            }
        }
    });

    read.await.unwrap();
    write.await.unwrap();
}

#[tokio::test]
async fn issue_3150() {
    // Same bounds as `tokio::spawn()`
    async fn fake_spawn<F>(future: F) -> F::Output
    where
        F: Future + Send + 'static,
    {
        future.await
    }

    fake_spawn(async {
        let mut db = TursoConnection::connect("turso::memory:").await.unwrap();
        sqlx::raw_sql("").execute(&mut db).await.unwrap();
        db.close().await.unwrap();
    })
    .await;
}

// Adapted: upstream also asserts the engine's transaction state (none / write) through
// `sqlite3_txn_state`, a C API that Turso does not expose.
#[tokio::test]
async fn it_can_use_transaction_options() -> anyhow::Result<()> {
    let mut conn = TursoConnectOptions::new()
        .in_memory()
        .connect()
        .await
        .unwrap();

    assert!(!conn.is_in_transaction());

    let tx = conn.begin_with("BEGIN DEFERRED").await?;
    drop(tx);

    let tx = conn.begin_with("BEGIN IMMEDIATE").await?;
    drop(tx);

    let tx = conn.begin_with("BEGIN EXCLUSIVE").await?;
    drop(tx);

    Ok(())
}

#[tokio::test]
async fn it_can_recover_from_bad_transaction_begin() -> anyhow::Result<()> {
    let mut conn = TursoConnectOptions::new()
        .in_memory()
        .connect()
        .await
        .unwrap();

    // This statement doesn't actually start a transaction.
    assert!(conn.begin_with("SELECT 1").await.is_err());

    // Transaction state bookkeeping should be correctly reset.

    let mut tx = conn.begin_with("BEGIN IMMEDIATE").await?;
    let value = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&mut *tx)
        .await?;
    assert_eq!(value, 1);

    Ok(())
}

#[tokio::test]
async fn issue_3982() -> anyhow::Result<()> {
    let (mut conn, _db) = new().await?;

    let r = sqlx::raw_sql("insert into products(product_no) values(1)")
        .execute(&mut conn)
        .await?;
    assert_eq!(r.rows_affected(), 1);

    let (name,) = sqlx::query_as::<_, (Option<String>,)>(
        r#"
        select name from products where name IS NULL
        "#,
    )
    .fetch_one(&mut conn)
    .await?;

    assert_eq!(name, None,);

    Ok(())
}

#[cfg(feature = "migrate")]
#[tokio::test]
async fn issue_4300() -> anyhow::Result<()> {
    use crate::common::fixture_dir;
    use sqlx_core::migrate::Migrator;

    let pool = TursoPoolOptions::new().connect("turso::memory:").await?;

    let migrator = Migrator::new(fixture_dir().join("migrations_issue_4300")).await?;
    migrator.run(&pool).await?;

    sqlx::query("CREATE TABLE my_table ( qqq TEXT )")
        .execute(&pool)
        .await?;

    sqlx::query("CREATE TABLE other_table ( www TEXT )")
        .execute(&pool)
        .await?;

    sqlx::query("INSERT INTO my_table (qqq) VALUES ('temporary')")
        .execute(&pool)
        .await?;

    let result = sqlx::query("BEGIN TRANSACTION; COMMIT;")
        .execute(&pool)
        .await?;

    assert_eq!(result.rows_affected(), 0);

    Ok(())
}
