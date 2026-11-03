//! Port of `sqlx/tests/sqlite/error.rs`.

use sqlx::{Connection, Error, error::ErrorKind};

use crate::common::new;

#[tokio::test]
async fn it_fails_with_unique_violation() -> anyhow::Result<()> {
    let (mut conn, _db) = new().await?;
    let mut tx = conn.begin().await?;

    let res: Result<_, sqlx::Error> = sqlx::query("INSERT INTO tweet VALUES (1, 'Foo', true, 1);")
        .execute(&mut *tx)
        .await;
    let err = res.unwrap_err();

    let err = err.into_database_error().unwrap();

    assert_eq!(err.kind(), ErrorKind::UniqueViolation);

    Ok(())
}

#[tokio::test]
async fn it_fails_with_foreign_key_violation() -> anyhow::Result<()> {
    let (mut conn, _db) = new().await?;
    let mut tx = conn.begin().await?;

    let res: Result<_, sqlx::Error> =
        sqlx::query("INSERT INTO tweet_reply (id, tweet_id, text) VALUES (2, 2, 'Reply!');")
            .execute(&mut *tx)
            .await;
    let err = res.unwrap_err();

    let err = err.into_database_error().unwrap();

    assert_eq!(err.kind(), ErrorKind::ForeignKeyViolation);

    Ok(())
}

#[tokio::test]
async fn it_fails_with_not_null_violation() -> anyhow::Result<()> {
    let (mut conn, _db) = new().await?;
    let mut tx = conn.begin().await?;

    let res: Result<_, sqlx::Error> = sqlx::query("INSERT INTO tweet (text) VALUES (null);")
        .execute(&mut *tx)
        .await;
    let err = res.unwrap_err();

    let err = err.into_database_error().unwrap();

    assert_eq!(err.kind(), ErrorKind::NotNullViolation);

    Ok(())
}

#[tokio::test]
async fn it_fails_with_check_violation() -> anyhow::Result<()> {
    let (mut conn, _db) = new().await?;
    let mut tx = conn.begin().await?;

    let res: Result<_, sqlx::Error> =
        sqlx::query("INSERT INTO products VALUES (1, 'Product 1', 0);")
            .execute(&mut *tx)
            .await;
    let err = res.unwrap_err();

    let err = err.into_database_error().unwrap();

    assert_eq!(err.kind(), ErrorKind::CheckViolation);

    Ok(())
}

#[tokio::test]
async fn it_fails_with_begin_failed() -> anyhow::Result<()> {
    let (mut conn, _db) = new().await?;
    let res = conn.begin_with("SELECT * FROM tweet").await;

    let err = res.unwrap_err();

    assert!(matches!(err, Error::BeginFailed), "{err:?}");

    Ok(())
}

#[tokio::test]
async fn it_fails_with_invalid_save_point_statement() -> anyhow::Result<()> {
    let (mut conn, _db) = new().await?;
    let mut txn = conn.begin().await?;
    let txn_conn = sqlx::Acquire::acquire(&mut txn).await?;
    let res = txn_conn.begin_with("BEGIN").await;

    let err = res.unwrap_err();

    assert!(matches!(err, Error::InvalidSavePointStatement), "{err}");

    Ok(())
}
