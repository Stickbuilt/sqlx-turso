//! Local replacements for the `sqlx-test` helpers. Every test gets its own database:
//! a private copy of the `sqlite.db` fixture, or an in-memory database.

use std::path::{Path, PathBuf};

use sqlx::Connection;
use sqlx_turso::{TursoConnectOptions, TursoConnection};
use tempfile::TempDir;

/// Directory holding the fixtures (`sqlite.db`, `setup.sql`, `migrations_*`).
pub fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sqlite")
}

/// A private copy of the `sqlite.db` fixture. The database lives as long as the
/// returned [`TempDir`].
pub fn fixture_db() -> anyhow::Result<(PathBuf, TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("sqlite.db");
    std::fs::copy(fixture_dir().join("sqlite.db"), &path)?;
    Ok((path, dir))
}

/// `turso:` URL of a private copy of the `sqlite.db` fixture.
pub fn fixture_url() -> anyhow::Result<(String, TempDir)> {
    let (path, dir) = fixture_db()?;
    Ok((format!("turso:{}", path.display()), dir))
}

/// Equivalent of `sqlx_test::new::<Sqlite>()`: a connection to a private copy of the
/// `sqlite.db` fixture. Keep the returned [`TempDir`] alive for the whole test.
pub async fn new() -> anyhow::Result<(TursoConnection, TempDir)> {
    let (path, dir) = fixture_db()?;
    let conn = TursoConnection::connect_with(&TursoConnectOptions::new().filename(path)).await?;
    Ok((conn, dir))
}

/// A connection to a fresh in-memory database.
pub async fn memory() -> sqlx::Result<TursoConnection> {
    TursoConnection::connect("turso::memory:").await
}

/// Equivalent of `sqlx_test::test_type!` for Turso: an encode/decode round trip through a
/// prepared query and a decode through an unprepared query.
#[macro_export]
macro_rules! test_type {
    ($name:ident<$ty:ty>($db:ident, $sql:literal, $($text:literal == $value:expr),+ $(,)?)) => {
        $crate::__test_prepared_type!($name<$ty>($db, $sql, $($text == $value),+));
        $crate::test_unprepared_type!($name<$ty>($db, $($text == $value),+));
    };

    ($name:ident<$ty:ty>($db:ident, $($text:literal == $value:expr),+ $(,)?)) => {
        $crate::__test_prepared_type!($name<$ty>($db, "SELECT {0} is ?, {0}, ?", $($text == $value),+));
        $crate::test_unprepared_type!($name<$ty>($db, $($text == $value),+));
    };

    ($name:ident($db:ident, $($text:literal == $value:expr),+ $(,)?)) => {
        $crate::test_type!($name<$name>($db, $($text == $value),+));
    };
}

/// Type decoding through the simple (unprepared) query API.
#[macro_export]
macro_rules! test_unprepared_type {
    ($name:ident<$ty:ty>($db:ident, $($text:literal == $value:expr),+ $(,)?)) => {
        paste::item! {
            #[tokio::test]
            async fn [< test_unprepared_type_ $name >] () -> anyhow::Result<()> {
                use sqlx::prelude::*;
                use sqlx::AssertSqlSafe;
                use futures_util::TryStreamExt;

                let mut conn = $crate::common::memory().await?;

                $(
                    let query = format!("SELECT {}", $text);
                    let mut s = conn.fetch(AssertSqlSafe(query));
                    let row = s.try_next().await?.unwrap();
                    let rec = row.try_get::<$ty, _>(0)?;

                    assert_eq!($value, rec);

                    drop(s);
                )+

                Ok(())
            }
        }
    }
}

/// Type encoding and decoding through the prepared query API.
#[macro_export]
macro_rules! __test_prepared_type {
    ($name:ident<$ty:ty>($db:ident, $sql:expr, $($text:literal == $value:expr),+ $(,)?)) => {
        paste::item! {
            #[tokio::test]
            async fn [< test_prepared_type_ $name >] () -> anyhow::Result<()> {
                use sqlx::Row;
                use sqlx::AssertSqlSafe;

                let mut conn = $crate::common::memory().await?;

                $(
                    let query = format!($sql, $text);
                    println!("{query}");

                    let row = sqlx::query::<$db>(AssertSqlSafe(query))
                        .bind($value)
                        .bind($value)
                        .fetch_one(&mut conn)
                        .await?;

                    let matches: i32 = row.try_get(0)?;
                    let returned: $ty = row.try_get(1)?;
                    let round_trip: $ty = row.try_get(2)?;

                    assert!(matches != 0,
                            "[1] DB value mismatch; given value: {:?}\n\
                             as returned: {:?}\n\
                             round-trip: {:?}",
                            $value, returned, round_trip);

                    assert_eq!($value, returned,
                            "[2] DB value mismatch; given value: {:?}\n\
                                     as returned: {:?}\n\
                                     round-trip: {:?}",
                                    $value, returned, round_trip);

                    assert_eq!($value, round_trip,
                            "[3] DB value mismatch; given value: {:?}\n\
                                     as returned: {:?}\n\
                                     round-trip: {:?}",
                                    $value, returned, round_trip);
                )+

                Ok(())
            }
        }
    };
}
