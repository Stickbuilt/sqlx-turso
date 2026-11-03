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

    /// Execute a statement that returns no rows of interest.
    pub(crate) async fn execute(&self, sql: &str) -> Result<(), Error> {
        let mut cursor = self.run(sql, Vec::new(), false).await.sqlx()?;
        while cursor.next().await.sqlx()?.is_some() {}
        Ok(())
    }

    /// Run one statement. With `cached`, the embedded engine reuses a prepared statement
    /// for the same SQL text.
    pub(crate) async fn run(
        &self,
        sql: &str,
        params: Vec<Value>,
        cached: bool,
    ) -> Result<Cursor, Error> {
        match self {
            Self::Embedded(conn) => {
                let mut stmt = if cached {
                    conn.prepare_cached(sql).await.sqlx()?
                } else {
                    conn.prepare(sql).await.sqlx()?
                };
                let columns = stmt
                    .columns()
                    .into_iter()
                    .map(|c| RawColumn {
                        name: c.name().to_string(),
                        decl_type: c.decl_type().map(str::to_string),
                    })
                    .collect();
                let rows = stmt.query(params).await.sqlx()?;
                Ok(Cursor::Embedded {
                    conn: conn.clone(),
                    stmt,
                    rows: Some(rows),
                    columns,
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
                let columns = result
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
                    .collect::<Result<Vec<_>, _>>()
                    .sqlx()?;
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
            Self::Embedded(conn) => conn
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

/// The rows and outcome of one statement.
pub(crate) enum Cursor {
    Embedded {
        conn: turso::Connection,
        stmt: turso::Statement,
        rows: Option<turso::Rows>,
        columns: Vec<RawColumn>,
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
    pub(crate) fn columns(&self) -> &[RawColumn] {
        match self {
            Self::Embedded { columns, .. } | Self::Buffered { columns, .. } => columns,
        }
    }

    pub(crate) async fn next(&mut self) -> Result<Option<Vec<Value>>, Error> {
        match self {
            Self::Embedded { rows, .. } => {
                let Some(cursor) = rows.as_mut() else {
                    return Ok(None);
                };
                match cursor.next().await.sqlx()? {
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
