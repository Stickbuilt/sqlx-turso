use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use sqlx_core::common::StatementCache;
use sqlx_core::connection::{Connection, LogSettings};
use sqlx_core::error::Error;
use sqlx_core::sql_str::SqlStr;
use sqlx_core::transaction::{
    begin_ansi_transaction_sql, commit_ansi_transaction_sql, rollback_ansi_transaction_sql,
};

use crate::backend::AnyConn;
use crate::{Turso, TursoConnectOptions};

mod executor;
pub(crate) mod parse;

use parse::ParsedQuery;

/// A transaction-control statement whose future may have been dropped before it
/// completed, leaving the engine's state unknown until the next operation.
#[derive(Debug, Clone, Copy)]
enum InFlight {
    /// A top-level `BEGIN` was sent.
    Begin,
    /// A top-level `COMMIT` / `ROLLBACK` was sent.
    End,
    /// A savepoint statement was sent; its outcome can't be observed.
    Savepoint,
}

/// A connection to a Turso database.
pub struct TursoConnection {
    pub(crate) raw: AnyConn,
    pub(crate) options: TursoConnectOptions,
    pub(crate) default_begin: &'static str,
    pub(crate) transaction_depth: usize,
    /// Rollbacks requested from `Drop` (which can't await), executed before the next
    /// operation on the connection.
    pending_rollbacks: Vec<SqlStr>,
    in_flight: Option<InFlight>,
    /// Set when the connection's transaction state can no longer be trusted; `ping()`
    /// then fails so a pool discards the connection.
    broken: bool,
    pub(crate) statements: StatementCache<Arc<ParsedQuery>>,
    pub(crate) log_settings: LogSettings,
}

impl TursoConnection {
    pub(crate) async fn establish(options: &TursoConnectOptions) -> Result<Self, Error> {
        let db = options.database().await?;
        let raw = db.connect().await?;
        raw.busy_timeout(options.busy_timeout)?;

        Ok(Self {
            raw,
            options: options.clone(),
            default_begin: options.default_begin_sql(),
            transaction_depth: 0,
            pending_rollbacks: Vec::new(),
            in_flight: None,
            broken: false,
            statements: StatementCache::new(options.statement_cache_capacity),
            log_settings: options.log_settings.clone(),
        })
    }

    /// The options this connection was created from. Connections created from the same
    /// options share one database handle (and, with sync, one replica).
    pub fn connect_options(&self) -> &TursoConnectOptions {
        &self.options
    }

    /// Whether this connection talks to Turso Cloud over HTTP.
    pub fn is_remote(&self) -> bool {
        self.raw.is_remote()
    }

    /// Resolve deferred work left by dropped transactions or cancelled futures.
    pub(crate) async fn settle(&mut self) -> Result<(), Error> {
        if self.broken {
            return Err(Error::Protocol(
                "connection is in an unknown transaction state after a cancelled \
                 savepoint operation"
                    .into(),
            ));
        }

        if let Some(in_flight) = self.in_flight.take() {
            let in_transaction = !self.raw.is_autocommit()?;
            match in_flight {
                InFlight::Begin => {
                    // BEGIN may have run although its future was dropped; nothing owns that
                    // transaction, so undo it.
                    if in_transaction && self.transaction_depth == 0 {
                        self.raw.execute("ROLLBACK").await?;
                    }
                }
                InFlight::End => {
                    if !in_transaction {
                        self.transaction_depth = 0;
                    }
                }
                InFlight::Savepoint => {
                    self.broken = true;
                    return Box::pin(self.settle()).await;
                }
            }
        }

        while !self.pending_rollbacks.is_empty() {
            let sql = self.pending_rollbacks.remove(0);
            if let Err(error) = self.raw.execute(sql.as_str()).await {
                // A failed top-level ROLLBACK usually means the transaction is already gone
                // (e.g. rolled back by the engine on error); anything else is unknown state.
                if !self.raw.is_autocommit().unwrap_or(false) {
                    self.broken = true;
                    return Err(error);
                }
                self.pending_rollbacks.clear();
                self.transaction_depth = 0;
            }
        }

        Ok(())
    }

    pub(crate) async fn begin_tx(&mut self, statement: Option<SqlStr>) -> Result<(), Error> {
        self.settle().await?;

        let depth = self.transaction_depth;
        let is_custom = statement.is_some();
        let sql = match statement {
            Some(_) if depth > 0 => return Err(Error::InvalidSavePointStatement),
            Some(statement) => statement,
            None if depth == 0 => SqlStr::from_static(self.default_begin),
            None => begin_ansi_transaction_sql(depth),
        };

        self.in_flight = Some(if depth == 0 {
            InFlight::Begin
        } else {
            InFlight::Savepoint
        });
        let result = self.raw.execute(sql.as_str()).await;
        self.in_flight = None;
        result?;

        if is_custom && self.raw.is_autocommit()? {
            return Err(Error::BeginFailed);
        }

        self.transaction_depth += 1;
        Ok(())
    }

    pub(crate) async fn commit_tx(&mut self) -> Result<(), Error> {
        self.settle().await?;
        let depth = self.transaction_depth;
        if depth == 0 {
            return Ok(());
        }

        self.in_flight = Some(if depth == 1 {
            InFlight::End
        } else {
            InFlight::Savepoint
        });
        let result = self
            .raw
            .execute(commit_ansi_transaction_sql(depth).as_str())
            .await;
        self.in_flight = None;

        match result {
            Ok(()) => {
                self.transaction_depth -= 1;
                Ok(())
            }
            Err(error) => {
                // A failed top-level COMMIT may leave the transaction open (e.g. busy) or
                // closed (e.g. MVCC conflict); keep depth in step with the engine.
                if depth == 1 && self.raw.is_autocommit().unwrap_or(false) {
                    self.transaction_depth = 0;
                }
                Err(error)
            }
        }
    }

    pub(crate) async fn rollback_tx(&mut self) -> Result<(), Error> {
        self.settle().await?;
        let depth = self.transaction_depth;
        if depth == 0 {
            return Ok(());
        }

        self.in_flight = Some(if depth == 1 {
            InFlight::End
        } else {
            InFlight::Savepoint
        });
        let result = self
            .raw
            .execute(rollback_ansi_transaction_sql(depth).as_str())
            .await;
        self.in_flight = None;

        match result {
            Ok(()) => {
                self.transaction_depth -= 1;
                Ok(())
            }
            Err(error) => {
                if depth == 1 && self.raw.is_autocommit().unwrap_or(false) {
                    self.transaction_depth = 0;
                    return Ok(());
                }
                Err(error)
            }
        }
    }

    pub(crate) fn start_rollback_tx(&mut self) {
        let depth = self.transaction_depth;
        if depth > 0 {
            self.pending_rollbacks
                .push(rollback_ansi_transaction_sql(depth));
            self.transaction_depth -= 1;
        }
    }
}

impl Debug for TursoConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TursoConnection")
            .field("options", &self.options)
            .field("transaction_depth", &self.transaction_depth)
            .field("cached_statements_size", &self.statements.len())
            .finish_non_exhaustive()
    }
}

impl Connection for TursoConnection {
    type Database = Turso;

    type Options = TursoConnectOptions;

    async fn close(mut self) -> Result<(), Error> {
        // Best effort: don't leave a dangling transaction behind on a shared remote stream.
        let _ = self.settle().await;
        if self.transaction_depth > 0 {
            let _ = self.raw.execute("ROLLBACK").await;
        }
        self.raw.close().await
    }

    async fn close_hard(self) -> Result<(), Error> {
        self.raw.close().await
    }

    fn begin(
        &mut self,
    ) -> impl std::future::Future<
        Output = Result<sqlx_core::transaction::Transaction<'_, Turso>, Error>,
    > + Send
    + '_ {
        sqlx_core::transaction::Transaction::begin(self, None)
    }

    async fn ping(&mut self) -> Result<(), Error> {
        self.settle().await?;
        self.raw.execute("SELECT 1").await
    }

    fn cached_statements_size(&self) -> usize {
        self.statements.len()
    }

    async fn clear_cached_statements(&mut self) -> Result<(), Error> {
        self.statements.clear();
        Ok(())
    }

    fn shrink_buffers(&mut self) {}

    async fn flush(&mut self) -> Result<(), Error> {
        self.settle().await
    }

    fn should_flush(&self) -> bool {
        !self.pending_rollbacks.is_empty() || self.in_flight.is_some()
    }
}
