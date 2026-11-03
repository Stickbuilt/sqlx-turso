use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx_core::common::StatementCache;
use sqlx_core::connection::{Connection, LogSettings};
use sqlx_core::error::Error;
use sqlx_core::sql_str::SqlStr;
use sqlx_core::transaction::{
    begin_ansi_transaction_sql, commit_ansi_transaction_sql, rollback_ansi_transaction_sql,
};

use crate::backend::{AnyConn, Cursor};
use crate::{Turso, TursoConnectOptions, TursoError, TursoErrorKind};

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

/// How long a remote connection may sit idle before `ping()` checks it with a round
/// trip. Turso Cloud expires idle HTTP streams after a server-defined timeout; a ping
/// right after use (as on every pool release) needs no request.
const REMOTE_PING_IDLE: Duration = Duration::from_secs(5);

/// A connection to a Turso database.
pub struct TursoConnection {
    // Boxed so the connection stays pointer-sized inside pools and enums.
    pub(crate) state: Box<ConnectionState>,
}

pub(crate) struct ConnectionState {
    pub(crate) raw: AnyConn,
    pub(crate) options: TursoConnectOptions,
    pub(crate) default_begin: &'static str,
    pub(crate) transaction_depth: usize,
    /// A `begin` is running or failed without opening a level. sqlx-core drops the
    /// `Transaction` of a failed or cancelled `begin` (calling `start_rollback`); that
    /// rollback belongs to the level that never opened and must not touch outer ones.
    begin_pending: bool,
    /// Rollbacks requested from `Drop` (which can't await), executed before the next
    /// operation on the connection.
    pending_rollbacks: Vec<SqlStr>,
    in_flight: Option<InFlight>,
    /// Set when the connection's transaction state can no longer be trusted; `ping()`
    /// then fails so a pool discards the connection.
    broken: bool,
    /// Remote only: a request was started and has not been answered by the server
    /// (cancelled, or failed in transport), so the stream's state is unknown.
    remote_unverified: bool,
    /// Remote only: when the server last answered a request.
    remote_answered_at: Option<Instant>,
    pub(crate) statements: StatementCache<Arc<ParsedQuery>>,
    pub(crate) log_settings: LogSettings,
}

pub(crate) fn no_transaction_error() -> Error {
    TursoError::new(
        TursoErrorKind::Misuse,
        "no transaction is open on this connection",
    )
    .into()
}

pub(crate) fn aborted_error() -> Error {
    TursoError::new(
        TursoErrorKind::Aborted,
        "the transaction was rolled back by the database (write conflict, failed commit or \
         lost Turso Cloud stream); roll back or drop it before running more statements",
    )
    .into()
}

impl TursoConnection {
    pub(crate) async fn establish(options: &TursoConnectOptions) -> Result<Self, Error> {
        let db = options.database().await?;
        let raw = db.connect().await?;
        raw.busy_timeout(options.busy_timeout)?;

        Ok(Self {
            state: Box::new(ConnectionState {
                raw,
                options: options.clone(),
                default_begin: options.default_begin_sql(),
                transaction_depth: 0,
                begin_pending: false,
                pending_rollbacks: Vec::new(),
                in_flight: None,
                broken: false,
                remote_unverified: false,
                remote_answered_at: None,
                statements: StatementCache::new(options.statement_cache_capacity),
                log_settings: options.log_settings.clone(),
            }),
        })
    }

    /// The options this connection was created from. Connections created from the same
    /// options share one database handle (and, with sync, one replica).
    pub fn connect_options(&self) -> &TursoConnectOptions {
        &self.state.options
    }

    /// Whether this connection talks to Turso Cloud over HTTP.
    pub fn is_remote(&self) -> bool {
        self.state.raw.is_remote()
    }

    /// Run one statement on the engine, tracking whether a remote server answered.
    pub(crate) async fn raw_run(
        &mut self,
        sql: &str,
        params: Vec<turso::Value>,
    ) -> Result<Cursor, Error> {
        let remote = self.state.raw.is_remote();
        if remote {
            self.state.remote_unverified = true;
        }
        let result = self.state.raw.run(sql, params).await;
        if remote {
            // A database error is an answer from the server; transport and protocol
            // errors leave the stream in an unknown state.
            if matches!(&result, Ok(_) | Err(Error::Database(_))) {
                self.state.remote_unverified = false;
                self.state.remote_answered_at = Some(Instant::now());
            }
        }
        result
    }

    /// Execute a statement that returns no rows of interest.
    pub(crate) async fn raw_execute(&mut self, sql: &str) -> Result<(), Error> {
        let mut cursor = self.raw_run(sql, Vec::new()).await?;
        while cursor.next().await?.is_some() {}
        Ok(())
    }

    /// Fail if the engine rolled back the transaction this connection still considers
    /// open (MVCC write conflict, failed commit, reset Turso Cloud stream). Running the
    /// statement anyway would autocommit it outside the caller's transaction.
    pub(crate) fn check_not_aborted(&self) -> Result<(), Error> {
        if self.state.transaction_depth > 0 && self.state.raw.is_autocommit()? {
            return Err(aborted_error());
        }
        Ok(())
    }

    /// Resolve deferred work left by dropped transactions or cancelled futures.
    pub(crate) async fn settle(&mut self) -> Result<(), Error> {
        // A pending begin is resolved by the `start_rollback` that follows a failed
        // `begin` immediately; one still set here has no owner.
        self.state.begin_pending = false;
        self.settle_inner().await
    }

    async fn settle_inner(&mut self) -> Result<(), Error> {
        if self.state.broken {
            return Err(Error::Protocol(
                "connection is in an unknown transaction state after a cancelled \
                 savepoint operation"
                    .into(),
            ));
        }

        if let Some(in_flight) = self.state.in_flight.take() {
            let in_transaction = !self.state.raw.is_autocommit()?;
            match in_flight {
                InFlight::Begin => {
                    // BEGIN may have run although its future was dropped; nothing owns that
                    // transaction, so undo it.
                    if in_transaction && self.state.transaction_depth == 0 {
                        self.raw_execute("ROLLBACK").await?;
                    }
                }
                InFlight::End => {
                    if !in_transaction {
                        self.state.transaction_depth = 0;
                    }
                }
                InFlight::Savepoint => {
                    self.state.broken = true;
                    return Box::pin(self.settle_inner()).await;
                }
            }
        }

        while !self.state.pending_rollbacks.is_empty() {
            let sql = self.state.pending_rollbacks.remove(0);
            // With the engine in autocommit the transaction is already gone (rolled back
            // by the engine, or never opened); there is nothing to undo. Outer levels
            // stay open in the driver so their statements keep failing.
            if self.state.raw.is_autocommit()? {
                continue;
            }
            if let Err(error) = self.raw_execute(sql.as_str()).await {
                if self.state.raw.is_autocommit().unwrap_or(false) {
                    continue;
                }
                self.state.broken = true;
                return Err(error);
            }
        }

        Ok(())
    }

    pub(crate) async fn begin_tx(&mut self, statement: Option<SqlStr>) -> Result<(), Error> {
        // Set before the first await: if this future fails or is dropped, sqlx-core's
        // `start_rollback` consumes the pending level instead of an outer one.
        self.state.begin_pending = true;
        self.settle_inner().await?;

        let depth = self.state.transaction_depth;
        let is_custom = statement.is_some();
        let sql = match statement {
            Some(_) if depth > 0 => return Err(Error::InvalidSavePointStatement),
            Some(statement) => statement,
            None if depth == 0 => SqlStr::from_static(self.state.default_begin),
            None => begin_ansi_transaction_sql(depth),
        };
        // A SAVEPOINT outside a transaction would silently open a new one.
        self.check_not_aborted()?;

        self.state.in_flight = Some(if depth == 0 {
            InFlight::Begin
        } else {
            InFlight::Savepoint
        });
        let result = self.raw_execute(sql.as_str()).await;
        self.state.in_flight = None;
        result?;

        if is_custom && self.state.raw.is_autocommit()? {
            return Err(Error::BeginFailed);
        }

        self.state.begin_pending = false;
        self.state.transaction_depth += 1;
        Ok(())
    }

    pub(crate) async fn commit_tx(&mut self) -> Result<(), Error> {
        self.settle().await?;
        let depth = self.state.transaction_depth;
        if depth == 0 {
            return Err(no_transaction_error());
        }
        // Committing a transaction the engine already rolled back must not look like
        // success; the caller's `Transaction` rolls the level back on drop.
        self.check_not_aborted()?;

        self.state.in_flight = Some(if depth == 1 {
            InFlight::End
        } else {
            InFlight::Savepoint
        });
        let result = self
            .raw_execute(commit_ansi_transaction_sql(depth).as_str())
            .await;
        self.state.in_flight = None;

        match result {
            Ok(()) => {
                self.state.transaction_depth -= 1;
                Ok(())
            }
            Err(error) => {
                // A failed top-level COMMIT may leave the transaction open (e.g. busy) or
                // closed (e.g. MVCC conflict); keep depth in step with the engine.
                if depth == 1 && self.state.raw.is_autocommit().unwrap_or(false) {
                    self.state.transaction_depth = 0;
                }
                Err(error)
            }
        }
    }

    pub(crate) async fn rollback_tx(&mut self) -> Result<(), Error> {
        self.settle().await?;
        let depth = self.state.transaction_depth;
        if depth == 0 {
            return Err(no_transaction_error());
        }
        if self.state.raw.is_autocommit()? {
            // The engine already rolled the transaction back; nothing to undo. Outer
            // levels stay open in the driver until they are rolled back too.
            self.state.transaction_depth -= 1;
            return Ok(());
        }

        self.state.in_flight = Some(if depth == 1 {
            InFlight::End
        } else {
            InFlight::Savepoint
        });
        let result = self
            .raw_execute(rollback_ansi_transaction_sql(depth).as_str())
            .await;
        self.state.in_flight = None;

        match result {
            Ok(()) => {
                self.state.transaction_depth -= 1;
                Ok(())
            }
            Err(error) => {
                if self.state.raw.is_autocommit().unwrap_or(false) {
                    self.state.transaction_depth -= 1;
                    return Ok(());
                }
                Err(error)
            }
        }
    }

    pub(crate) fn start_rollback_tx(&mut self) {
        if std::mem::take(&mut self.state.begin_pending) {
            // The `begin` this rollback belongs to never opened a level.
            return;
        }
        let depth = self.state.transaction_depth;
        if depth > 0 {
            self.state
                .pending_rollbacks
                .push(rollback_ansi_transaction_sql(depth));
            self.state.transaction_depth -= 1;
        }
    }
}

impl Debug for TursoConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TursoConnection")
            .field("options", &self.state.options)
            .field("transaction_depth", &self.state.transaction_depth)
            .field("cached_statements_size", &self.state.statements.len())
            .finish_non_exhaustive()
    }
}

impl Connection for TursoConnection {
    type Database = Turso;

    type Options = TursoConnectOptions;

    async fn close(mut self) -> Result<(), Error> {
        // Best effort: don't leave a dangling transaction behind on a shared remote stream.
        let _ = self.settle().await;
        if !self.state.raw.is_autocommit().unwrap_or(true) {
            let _ = self.raw_execute("ROLLBACK").await;
        }
        self.state.raw.close().await
    }

    async fn close_hard(self) -> Result<(), Error> {
        self.state.raw.close().await
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
        if self.state.raw.is_remote() {
            // sqlx-core pings on every pool release; only go to the network when the
            // stream's state is unknown or it may have expired while idle.
            let fresh = self
                .state
                .remote_answered_at
                .is_some_and(|at| at.elapsed() < REMOTE_PING_IDLE);
            if fresh && !self.state.remote_unverified {
                return Ok(());
            }
        }
        self.raw_execute("SELECT 1").await
    }

    fn cached_statements_size(&self) -> usize {
        self.state.statements.len()
    }

    async fn clear_cached_statements(&mut self) -> Result<(), Error> {
        self.state.statements.clear();
        Ok(())
    }

    fn shrink_buffers(&mut self) {}

    async fn flush(&mut self) -> Result<(), Error> {
        self.settle().await
    }

    fn should_flush(&self) -> bool {
        !self.state.pending_rollbacks.is_empty() || self.state.in_flight.is_some()
    }
}
