use crate::error::Error;
use crate::executor::Executor;
use crate::fs;
use crate::migrate::MigrateError;
use crate::migrate::{AppliedMigration, Migration};
use crate::migrate::{Migrate, MigrateDatabase};
use crate::options::Target;
use crate::query::query;
use crate::query_as::query_as;
use crate::{Turso, TursoConnectOptions, TursoConnection};
use futures_core::future::BoxFuture;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_core::sql_str::AssertSqlSafe;
use std::str::FromStr;
use std::time::Duration;
use std::time::Instant;

pub(crate) use sqlx_core::migrate::*;
use sqlx_core::query_scalar::query_scalar;

impl MigrateDatabase for Turso {
    async fn create_database(url: &str) -> Result<(), Error> {
        let opts = TursoConnectOptions::from_str(url)?.create_if_missing(true);

        // Opening a connection creates an embedded database file; remote databases are
        // created through Turso Cloud and only need to be reachable.
        opts.connect().await?.close().await?;

        Ok(())
    }

    async fn database_exists(url: &str) -> Result<bool, Error> {
        let options = TursoConnectOptions::from_str(url)?;

        match &options.target {
            Target::Memory => Ok(true),
            Target::File(path) => Ok(path.exists()),
            Target::Remote { .. } => match options.connect().await {
                Ok(conn) => {
                    conn.close().await?;
                    Ok(true)
                }
                Err(error) => Err(error),
            },
        }
    }

    async fn drop_database(url: &str) -> Result<(), Error> {
        let options = TursoConnectOptions::from_str(url)?;

        match &options.target {
            Target::Memory => Ok(()),
            Target::File(path) => {
                fs::remove_file(path).await?;
                // Remove the engine's side files if present.
                for suffix in ["-wal", "-shm", "-log"] {
                    let mut side = path.clone().into_os_string();
                    side.push(suffix);
                    let side = std::path::PathBuf::from(side);
                    if side.exists() {
                        fs::remove_file(&side).await?;
                    }
                }
                Ok(())
            }
            Target::Remote { .. } => Err(Error::Configuration(
                "dropping a Turso Cloud database is not supported through the driver; use the \
                 Turso CLI or Platform API"
                    .into(),
            )),
        }
    }
}

impl Migrate for TursoConnection {
    fn create_schema_if_not_exists<'e>(
        &'e mut self,
        schema_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            // Check if the schema already exists; if so, don't error.
            let schema_version: Option<i64> = query_scalar(AssertSqlSafe(format!(
                "PRAGMA {schema_name}.schema_version"
            )))
            .fetch_optional(&mut *self)
            .await?;

            if schema_version.is_some() {
                return Ok(());
            }

            Err(MigrateError::CreateSchemasNotSupported(format!(
                "cannot create new schema {schema_name}; creation of additional schemas in Turso requires attaching extra database files"
            )))
        })
    }

    fn ensure_migrations_table<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            // language=Turso
            self.execute(AssertSqlSafe(format!(
                r#"
CREATE TABLE IF NOT EXISTS {table_name} (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    checksum BLOB NOT NULL,
    execution_time BIGINT NOT NULL
);
                "#
            )))
            .await?;

            Ok(())
        })
    }

    fn dirty_version<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Option<i64>, MigrateError>> {
        Box::pin(async move {
            // language=Turso
            let row: Option<(i64,)> = query_as(AssertSqlSafe(format!(
                "SELECT version FROM {table_name} WHERE success = false ORDER BY version LIMIT 1"
            )))
            .fetch_optional(self)
            .await?;

            Ok(row.map(|r| r.0))
        })
    }

    fn list_applied_migrations<'e>(
        &'e mut self,
        table_name: &'e str,
    ) -> BoxFuture<'e, Result<Vec<AppliedMigration>, MigrateError>> {
        Box::pin(async move {
            // language=Turso
            let rows: Vec<(i64, Vec<u8>)> = query_as(AssertSqlSafe(format!(
                "SELECT version, checksum FROM {table_name} ORDER BY version"
            )))
            .fetch_all(self)
            .await?;

            let migrations = rows
                .into_iter()
                .map(|(version, checksum)| AppliedMigration {
                    version,
                    checksum: checksum.into(),
                })
                .collect();

            Ok(migrations)
        })
    }

    fn lock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move { Ok(()) })
    }

    fn unlock(&mut self) -> BoxFuture<'_, Result<(), MigrateError>> {
        Box::pin(async move { Ok(()) })
    }

    fn apply<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let start = Instant::now();

            if migration.no_tx {
                execute_migration(self, table_name, migration).await?;
            } else {
                // Use a single transaction for the actual migration script and the essential bookkeeping so we never
                // execute migrations twice. See https://github.com/launchbadge/sqlx/issues/1966.
                // The `execution_time` however can only be measured for the whole transaction. This value _only_ exists for
                // data lineage and debugging reasons, so it is not super important if it is lost. So we initialize it to -1
                // and update it once the actual transaction completed.
                let mut tx = begin_migration(self).await?;
                execute_migration(&mut tx, table_name, migration).await?;
                tx.commit().await?;
            }

            // Update `elapsed_time`.
            // NOTE: The process may disconnect/die at this point, so the elapsed time value might be lost. We accept
            //       this small risk since this value is not super important.
            let elapsed = start.elapsed();

            // language=Turso
            #[allow(clippy::cast_possible_truncation)]
            let _ = query(AssertSqlSafe(format!(
                r#"
    UPDATE {table_name}
    SET execution_time = ?1
    WHERE version = ?2
                "#
            )))
            .bind(elapsed.as_nanos() as i64)
            .bind(migration.version)
            .execute(self)
            .await?;

            Ok(elapsed)
        })
    }

    fn revert<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<Duration, MigrateError>> {
        Box::pin(async move {
            let start = Instant::now();

            if migration.no_tx {
                revert_migration(self, table_name, migration).await?;
            } else {
                // Use a single transaction for the actual migration script and the essential bookkeeping so we never
                // execute migrations twice. See https://github.com/launchbadge/sqlx/issues/1966.
                let mut tx = begin_migration(self).await?;
                revert_migration(&mut tx, table_name, migration).await?;
                tx.commit().await?;
            }

            let elapsed = start.elapsed();

            Ok(elapsed)
        })
    }

    fn skip<'e>(
        &'e mut self,
        table_name: &'e str,
        migration: &'e Migration,
    ) -> BoxFuture<'e, Result<(), MigrateError>> {
        Box::pin(async move {
            // language=Turso
            let _ = query(AssertSqlSafe(format!(
                r#"
    INSERT INTO {table_name} ( version, description, success, checksum, execution_time )
    VALUES ( ?1, ?2, TRUE, ?3, -1 )
        "#
            )))
            .bind(migration.version)
            .bind(&*migration.description)
            .bind(&*migration.checksum)
            .execute(self)
            .await?;

            Ok(())
        })
    }
}

/// Open the transaction a migration runs in. DDL is rejected inside `BEGIN CONCURRENT`
/// (the default with `concurrent_writes(true)` and for `turso://` / `https://` URLs), so a
/// top-level migration transaction always uses `BEGIN IMMEDIATE`. Inside a caller's
/// transaction a savepoint is used, as with `begin()`.
async fn begin_migration(
    conn: &mut TursoConnection,
) -> Result<sqlx_core::transaction::Transaction<'_, Turso>, Error> {
    if conn.state.transaction_depth == 0 {
        sqlx_core::transaction::Transaction::begin(
            conn,
            Some(sqlx_core::sql_str::SqlStr::from_static("BEGIN IMMEDIATE")),
        )
        .await
    } else {
        conn.begin().await
    }
}

async fn execute_migration(
    conn: &mut TursoConnection,
    table_name: &str,
    migration: &Migration,
) -> Result<(), MigrateError> {
    let _ = conn
        .execute(migration.sql.clone())
        .await
        .map_err(|e| MigrateError::ExecuteMigration(e, migration.version))?;

    // language=Turso
    let _ = query(AssertSqlSafe(format!(
        r#"
    INSERT INTO {table_name} ( version, description, success, checksum, execution_time )
    VALUES ( ?1, ?2, TRUE, ?3, -1 )
        "#
    )))
    .bind(migration.version)
    .bind(&*migration.description)
    .bind(&*migration.checksum)
    .execute(conn)
    .await?;

    Ok(())
}

async fn revert_migration(
    conn: &mut TursoConnection,
    table_name: &str,
    migration: &Migration,
) -> Result<(), MigrateError> {
    let _ = conn
        .execute(migration.sql.clone())
        .await
        .map_err(|e| MigrateError::ExecuteMigration(e, migration.version))?;

    // language=Turso
    let _ = query(AssertSqlSafe(format!(
        r#"
    DELETE FROM {table_name}
    WHERE version = ?1
        "#
    )))
    .bind(migration.version)
    .execute(conn)
    .await?;

    Ok(())
}
