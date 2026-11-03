use sqlx::{Connection, Executor, Row};
use sqlx_core::error::ErrorKind;
use sqlx_turso::{Turso, TursoConnectOptions, TursoConnection, TursoPoolOptions};

async fn memory() -> TursoConnection {
    TursoConnection::connect("turso::memory:").await.unwrap()
}

#[tokio::test]
async fn select_bind_and_decode() {
    let mut conn = memory().await;

    let row = sqlx::query("SELECT ?1 + 1 AS n, ?2 AS s, ?3 AS b, ?4 AS f, NULL AS z")
        .bind(41_i64)
        .bind("hello")
        .bind(vec![1_u8, 2, 3])
        .bind(1.5_f64)
        .fetch_one(&mut conn)
        .await
        .unwrap();

    assert_eq!(row.get::<i64, _>("n"), 42);
    assert_eq!(row.get::<String, _>("s"), "hello");
    assert_eq!(row.get::<Vec<u8>, _>("b"), vec![1, 2, 3]);
    assert_eq!(row.get::<f64, _>("f"), 1.5);
    assert_eq!(row.get::<Option<i64>, _>("z"), None);
    // Unchecked decodes coerce like sqlite3_value_text() / sqlite3_value_int64().
    assert_eq!(row.get_unchecked::<&str, _>("n"), "42");
    assert_eq!(row.get_unchecked::<&str, _>("f"), "1.5");
    assert_eq!(row.get_unchecked::<i64, _>("s"), 0);
}

#[tokio::test]
async fn crud_with_returning_and_outcomes() {
    let mut conn = memory().await;

    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, ok BOOLEAN)")
        .await
        .unwrap();

    let done = sqlx::query("INSERT INTO t (name, ok) VALUES (?, ?), (?, ?)")
        .bind("a")
        .bind(true)
        .bind("b")
        .bind(false)
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(done.rows_affected(), 2);
    assert_eq!(done.last_insert_rowid(), 2);

    let (id, name): (i64, String) =
        sqlx::query_as("INSERT INTO t (name, ok) VALUES ('c', 1) RETURNING id, name")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!((id, name.as_str()), (3, "c"));

    let done = sqlx::query("UPDATE t SET ok = 1")
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(done.rows_affected(), 3);

    let oks: Vec<bool> = sqlx::query_scalar("SELECT ok FROM t ORDER BY id")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(oks, [true, true, true]);

    let selected = sqlx::query("SELECT * FROM t")
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(selected.rows_affected(), 0);
}

#[tokio::test]
async fn multi_statement_queries() {
    let mut conn = memory().await;

    conn.execute("CREATE TABLE a (x INTEGER); INSERT INTO a VALUES (1); INSERT INTO a VALUES (2);")
        .await
        .unwrap();

    // Anonymous parameters continue numbering across statements.
    let rows = sqlx::query("INSERT INTO a VALUES (?); SELECT x FROM a WHERE x >= ? ORDER BY x")
        .bind(3_i64)
        .bind(2_i64)
        .fetch_all(&mut conn)
        .await
        .unwrap();
    let xs: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
    assert_eq!(xs, [2, 3]);

    let row = sqlx::query("SELECT ?2, ?1")
        .bind(1_i64)
        .bind(2_i64)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!((row.get::<i64, _>(0), row.get::<i64, _>(1)), (2, 1));
}

#[tokio::test]
async fn constraint_errors_have_kinds() {
    let mut conn = memory().await;
    conn.execute(
        "CREATE TABLE p (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, v INTEGER CHECK (v >= 0));
         CREATE TABLE c (id INTEGER PRIMARY KEY, p INTEGER NOT NULL REFERENCES p(id));
         INSERT INTO p (id, name, v) VALUES (1, 'a', 1);",
    )
    .await
    .unwrap();

    async fn kind(conn: &mut TursoConnection, sql: &'static str) -> Option<(ErrorKind, String)> {
        let err = conn.execute(sql).await.unwrap_err();
        err.as_database_error()
            .map(|e| (e.kind(), e.code().unwrap().into_owned()))
    }

    assert_eq!(
        kind(&mut conn, "INSERT INTO p (name) VALUES ('a')").await,
        Some((ErrorKind::UniqueViolation, "2067".into()))
    );
    assert_eq!(
        kind(&mut conn, "INSERT INTO p (id, name) VALUES (1, 'z')")
            .await
            .unwrap()
            .0,
        ErrorKind::UniqueViolation
    );
    assert_eq!(
        kind(&mut conn, "INSERT INTO p (name) VALUES (NULL)").await,
        Some((ErrorKind::NotNullViolation, "1299".into()))
    );
    assert_eq!(
        kind(&mut conn, "INSERT INTO p (name, v) VALUES ('x', -1)").await,
        Some((ErrorKind::CheckViolation, "275".into()))
    );
    assert_eq!(
        kind(&mut conn, "INSERT INTO c (p) VALUES (99)").await,
        Some((ErrorKind::ForeignKeyViolation, "787".into()))
    );
}

#[tokio::test]
async fn nested_transactions_and_drop_rollback() {
    let mut conn = memory().await;
    conn.execute("CREATE TABLE t (x INTEGER)").await.unwrap();

    {
        let mut tx = conn.begin().await.unwrap();
        tx.execute("INSERT INTO t VALUES (1)").await.unwrap();
        {
            let mut inner = tx.begin().await.unwrap();
            inner.execute("INSERT INTO t VALUES (2)").await.unwrap();
            inner.rollback().await.unwrap();
        }
        {
            let mut inner = tx.begin().await.unwrap();
            inner.execute("INSERT INTO t VALUES (3)").await.unwrap();
            inner.commit().await.unwrap();
        }
        {
            let mut inner = tx.begin().await.unwrap();
            inner.execute("INSERT INTO t VALUES (4)").await.unwrap();
            // dropped: rolled back before the next operation
        }
        tx.commit().await.unwrap();
    }

    {
        let mut tx = conn.begin().await.unwrap();
        tx.execute("INSERT INTO t VALUES (5)").await.unwrap();
        // dropped without commit
    }

    let xs: Vec<i64> = sqlx::query_scalar("SELECT x FROM t ORDER BY x")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(xs, [1, 3]);
    assert!(!conn.is_in_transaction());

    // A custom BEGIN is only allowed at the top level.
    let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.unwrap();
    assert!(tx.begin_with("BEGIN").await.is_err());
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn pool_shares_in_memory_database() {
    let pool = TursoPoolOptions::new()
        .max_connections(4)
        .connect("turso::memory:")
        .await
        .unwrap();

    pool.execute("CREATE TABLE shared (x INTEGER)")
        .await
        .unwrap();
    let mut conns = Vec::new();
    for i in 0..4_i64 {
        let mut conn = pool.acquire().await.unwrap();
        sqlx::query("INSERT INTO shared VALUES (?)")
            .bind(i)
            .execute(&mut *conn)
            .await
            .unwrap();
        conns.push(conn);
    }
    drop(conns);

    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM shared")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 4);
}

#[tokio::test]
async fn file_database_and_create_if_missing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.db");

    let missing = TursoConnectOptions::new().filename(&path);
    assert!(TursoConnection::connect_with(&missing).await.is_err());

    let opts = TursoConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let mut conn = TursoConnection::connect_with(&opts).await.unwrap();
    conn.execute("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (7)")
        .await
        .unwrap();
    conn.close().await.unwrap();

    let url = format!("turso:{}", path.display());
    let mut conn = TursoConnection::connect(&url).await.unwrap();
    let x: i64 = sqlx::query_scalar("SELECT x FROM t")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(x, 7);
}

#[tokio::test]
async fn concurrent_writes_use_mvcc() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TursoConnectOptions::new()
        .filename(dir.path().join("mvcc.db"))
        .create_if_missing(true)
        .concurrent_writes(true);
    let pool = TursoPoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await
        .unwrap();

    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(mode, "mvcc");

    pool.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .await
        .unwrap();

    // Two open transactions writing different rows both commit.
    let mut a = pool.begin().await.unwrap();
    let mut b = pool.begin().await.unwrap();
    a.execute("INSERT INTO t (id, v) VALUES (1, 1)")
        .await
        .unwrap();
    b.execute("INSERT INTO t (id, v) VALUES (2, 2)")
        .await
        .unwrap();
    a.commit().await.unwrap();
    b.commit().await.unwrap();

    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM t")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 2);
}

#[tokio::test]
async fn statement_describes_columns() {
    let mut conn = memory().await;
    conn.execute("CREATE TABLE t (id INTEGER, name TEXT, at DATETIME)")
        .await
        .unwrap();

    let stmt = conn
        .prepare(sqlx::SqlStr::from_static(
            "SELECT id, name, at, 1 + 1 AS e FROM t WHERE id = ?",
        ))
        .await
        .unwrap();
    use sqlx::{Column, Statement, TypeInfo};
    let cols: Vec<(String, String)> = stmt
        .columns()
        .iter()
        .map(|c| (c.name().to_string(), c.type_info().name().to_string()))
        .collect();
    assert_eq!(
        cols,
        [
            ("id".into(), "INTEGER".into()),
            ("name".into(), "TEXT".into()),
            ("at".into(), "DATETIME".into()),
            ("e".into(), "NULL".into())
        ]
    );
    assert_eq!(stmt.parameters().unwrap().right(), Some(1));
}

#[allow(dead_code)]
fn assert_database(_: Turso) {}

#[tokio::test]
async fn concurrent_write_conflict_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let opts = TursoConnectOptions::new()
        .filename(dir.path().join("conflict.db"))
        .create_if_missing(true)
        .concurrent_writes(true);
    let pool = TursoPoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await
        .unwrap();
    pool.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER); INSERT INTO t VALUES (1, 0)")
        .await
        .unwrap();

    let mut a = pool.begin().await.unwrap();
    let mut b = pool.begin().await.unwrap();
    a.execute("UPDATE t SET v = 1 WHERE id = 1").await.unwrap();
    let b_write = b.execute("UPDATE t SET v = 2 WHERE id = 1").await;
    a.commit().await.unwrap();

    // The losing transaction fails either at its write or at commit.
    let err = match b_write {
        Err(err) => err,
        Ok(_) => b.commit().await.unwrap_err(),
    };
    let db_err = err.as_database_error().expect("database error");
    let turso_err = db_err
        .try_downcast_ref::<sqlx_turso::TursoError>()
        .expect("TursoError");
    assert!(
        turso_err.is_conflict(),
        "not classified as conflict: {turso_err}"
    );

    let v: i64 = sqlx::query_scalar("SELECT v FROM t WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 1);
}
