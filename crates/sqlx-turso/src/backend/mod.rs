//! The engine behind a connection: embedded `turso`, `turso::sync`, or Turso Cloud over
//! HTTP (`turso_serverless`). Everything above this module is transport-agnostic; values
//! are normalised to [`turso::Value`] and errors to [`sqlx_core::Error`] here.

use std::sync::Arc;
use std::vec;

use sqlx_core::error::Error;

use crate::error::SqlxResultExt;
use turso::Value;

/// A shared database handle. Every connection created from one
/// [`TursoConnectOptions`](crate::TursoConnectOptions) goes through the same handle, so a
/// pool over `:memory:` sees one database and the sync engine sees every connection.
#[derive(Clone)]
pub(crate) enum AnyDatabase {
    Local(turso::Database),
    #[cfg(feature = "sync")]
    Sync(turso::sync::Database),
    #[cfg(feature = "serverless")]
    Serverless(turso_serverless::Database),
}

impl AnyDatabase {
    pub(crate) async fn connect(&self) -> Result<AnyConn, Error> {
        Ok(match self {
            Self::Local(db) => AnyConn::Embedded(db.connect().sqlx()?),
            #[cfg(feature = "sync")]
            Self::Sync(db) => AnyConn::Embedded(db.connect().await.sqlx()?),
            #[cfg(feature = "serverless")]
            Self::Serverless(db) => AnyConn::Serverless(db.connect().sqlx()?),
        })
    }
}

/// Column metadata reported by the engine for a statement.
#[derive(Debug, Clone)]
pub(crate) struct RawColumn {
    pub(crate) name: String,
    pub(crate) decl_type: Option<String>,
}

pub(crate) enum AnyConn {
    Embedded(turso::Connection),
    #[cfg(feature = "serverless")]
    Serverless(turso_serverless::Connection),
}

impl AnyConn {
    pub(crate) fn is_remote(&self) -> bool {
        match self {
            Self::Embedded(_) => false,
            #[cfg(feature = "serverless")]
            Self::Serverless(_) => true,
        }
    }

    /// Run one statement.
    ///
    /// Embedded statements are always freshly prepared: turso's own statement cache
    /// (`Connection::prepare_cached`) is unbounded and never invalidated, and a cached
    /// program recompiled after a schema change reports stale column metadata. Preparing
    /// adopts the connection's current schema, and [`Cursor::columns`] is re-read after
    /// the first step in case the engine still recompiled the statement.
    pub(crate) async fn run(&self, sql: &str, params: Vec<Value>) -> Result<Cursor, Error> {
        match self {
            Self::Embedded(conn) => {
                let mut stmt = conn.prepare(sql).await.sqlx()?;
                let columns = raw_columns(&stmt);
                let rows = stmt.query(params).await.sqlx()?;
                Ok(Cursor::Embedded {
                    conn: conn.clone(),
                    stmt,
                    rows: Some(rows),
                    columns,
                    stepped: false,
                })
            }
            #[cfg(feature = "serverless")]
            Self::Serverless(conn) => {
                let params = params
                    .into_iter()
                    .map(serverless::to_value)
                    .collect::<Vec<_>>();
                let stmt = turso_serverless::BatchStatement::new(sql, params).sqlx()?;
                let mut results = conn.batch(vec![stmt]).await.sqlx()?;
                let result = results
                    .pop()
                    .ok_or_else(|| {
                        Error::Protocol("Turso Cloud returned no result for a statement".into())
                    })
                    .sqlx()?;
                let columns: Vec<RawColumn> = result
                    .columns()
                    .iter()
                    .map(|c| RawColumn {
                        name: c.name().to_string(),
                        decl_type: c.decl_type().map(str::to_string),
                    })
                    .collect();
                let rows = result
                    .rows()
                    .iter()
                    .map(|row| {
                        (0..row.column_count())
                            .map(|i| row.get_value(i).map(serverless::from_value))
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .collect::<Result<Vec<Vec<Value>>, _>>()
                    .sqlx()?;
                if rows.iter().any(|row| row.len() != columns.len()) {
                    return Err(schema_changed_error());
                }
                Ok(Cursor::Buffered {
                    columns,
                    rows: rows.into_iter(),
                    changes: result.rows_affected(),
                    last_insert_rowid: result
                        .last_insert_rowid()
                        .unwrap_or_else(|| conn.last_insert_rowid()),
                })
            }
        }
    }

    /// Column metadata of a statement, without executing it.
    pub(crate) async fn describe(&self, sql: &str) -> Result<Vec<RawColumn>, Error> {
        let columns = match self {
            Self::Embedded(conn) => raw_columns(&conn.prepare(sql).await.sqlx()?),
            #[cfg(feature = "serverless")]
            Self::Serverless(conn) => conn
                .prepare(sql)
                .await
                .sqlx()?
                .columns()
                .into_iter()
                .map(|c| RawColumn {
                    name: c.name().to_string(),
                    decl_type: c.decl_type().map(str::to_string),
                })
                .collect(),
        };
        Ok(columns)
    }

    pub(crate) fn is_autocommit(&self) -> Result<bool, Error> {
        Ok(match self {
            Self::Embedded(conn) => conn.is_autocommit().sqlx()?,
            #[cfg(feature = "serverless")]
            Self::Serverless(conn) => conn.is_autocommit().sqlx()?,
        })
    }

    pub(crate) fn busy_timeout(&self, duration: std::time::Duration) -> Result<(), Error> {
        match self {
            Self::Embedded(conn) => conn.busy_timeout(duration).sqlx()?,
            #[cfg(feature = "serverless")]
            Self::Serverless(_) => {}
        }
        Ok(())
    }

    pub(crate) async fn close(self) -> Result<(), Error> {
        match self {
            Self::Embedded(conn) => drop(conn),
            #[cfg(feature = "serverless")]
            Self::Serverless(conn) => conn.close().await.sqlx()?,
        }
        Ok(())
    }
}

fn raw_columns(stmt: &turso::Statement) -> Vec<RawColumn> {
    stmt.columns()
        .into_iter()
        .map(|c| RawColumn {
            name: c.name().to_string(),
            decl_type: c.decl_type().map(str::to_string),
        })
        .collect()
}

fn schema_changed_error() -> Error {
    Error::Database(Box::new(crate::TursoError::new(
        crate::TursoErrorKind::Schema,
        "the database schema changed while the statement was starting and its result \
         columns no longer match; run the statement again",
    )))
}

/// The rows and outcome of one statement.
pub(crate) enum Cursor {
    Embedded {
        conn: turso::Connection,
        stmt: turso::Statement,
        rows: Option<turso::Rows>,
        columns: Vec<RawColumn>,
        /// Whether the statement has been stepped; the engine recompiles a statement
        /// whose schema is stale on its first step, so `columns` is refreshed then.
        stepped: bool,
    },
    #[cfg_attr(not(feature = "serverless"), allow(dead_code))]
    Buffered {
        columns: Vec<RawColumn>,
        rows: vec::IntoIter<Vec<Value>>,
        changes: u64,
        last_insert_rowid: i64,
    },
}

impl Cursor {
    /// Result column metadata. For embedded statements this is only final after the
    /// first call to [`Cursor::next`].
    pub(crate) fn columns(&self) -> &[RawColumn] {
        match self {
            Self::Embedded { columns, .. } | Self::Buffered { columns, .. } => columns,
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<Vec<Value>>, Error> {
        match self {
            Self::Embedded {
                stmt,
                rows,
                columns,
                stepped,
                ..
            } => {
                let Some(cursor) = rows.as_mut() else {
                    return Ok(None);
                };
                let result = cursor.next().await;
                if !*stepped {
                    *stepped = true;
                    // The first step may have recompiled the statement (schema change
                    // since it was prepared); the row was then built from the new
                    // program, so take the column metadata from it too.
                    let previous = columns.len();
                    *columns = raw_columns(stmt);
                    match &result {
                        // turso sizes the first row by the column count read before the
                        // step; if the recompiled statement has a different number of
                        // columns, the row is truncated or failed to build.
                        Ok(Some(row)) if row.column_count() != columns.len() => {
                            rows.take();
                            return Err(schema_changed_error());
                        }
                        Err(_) if previous != columns.len() => {
                            rows.take();
                            return Err(schema_changed_error());
                        }
                        _ => {}
                    }
                }
                match result.sqlx()? {
                    Some(row) => {
                        let values = (0..row.column_count())
                            .map(|i| row.get_value(i))
                            .collect::<Result<Vec<_>, _>>()
                            .sqlx()?;
                        Ok(Some(values))
                    }
                    None => {
                        // Release the statement's row cursor once exhausted.
                        rows.take();
                        Ok(None)
                    }
                }
            }
            Self::Buffered { rows, .. } => Ok(rows.next()),
        }
    }

    /// Rows changed by the statement and the connection's last inserted rowid.
    /// Only meaningful once [`Cursor::next`] has returned `None`.
    pub(crate) fn outcome(&self) -> (u64, i64) {
        match self {
            Self::Embedded { conn, stmt, .. } => (stmt.n_change(), conn.last_insert_rowid()),
            Self::Buffered {
                changes,
                last_insert_rowid,
                ..
            } => (*changes, *last_insert_rowid),
        }
    }
}

#[cfg(feature = "serverless")]
mod serverless {
    use turso::Value;
    use turso_serverless::Value as RemoteValue;

    pub(super) fn to_value(value: Value) -> RemoteValue {
        match value {
            Value::Null => RemoteValue::Null,
            Value::Integer(i) => RemoteValue::Integer(i),
            Value::Real(f) => RemoteValue::Real(f),
            Value::Text(s) => RemoteValue::Text(s),
            Value::Blob(b) => RemoteValue::Blob(b),
        }
    }

    pub(super) fn from_value(value: RemoteValue) -> Value {
        match value {
            RemoteValue::Null => Value::Null,
            RemoteValue::Integer(i) => Value::Integer(i),
            RemoteValue::Real(f) => Value::Real(f),
            RemoteValue::Text(s) => Value::Text(s),
            RemoteValue::Blob(b) => Value::Blob(b),
        }
    }
}

/// Keeps the [`AnyDatabase`] type nameable in public signatures without exposing it.
pub(crate) type SharedDatabase = Arc<futures_util::lock::Mutex<Option<AnyDatabase>>>;
