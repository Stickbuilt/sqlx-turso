use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use sqlx_core::IndexMap;
use sqlx_core::connection::{ConnectOptions, LogSettings};
use sqlx_core::error::{BoxDynError, Error};
use url::Url;

use crate::TursoConnection;
use crate::backend::{AnyDatabase, SharedDatabase};
use crate::error::SqlxResultExt;

mod parse;

pub use turso::EncryptionOpts;
#[cfg(feature = "sync")]
pub use turso::sync::{PartialBootstrapStrategy, PartialSyncOpts, RemoteEncryptionCipher};

/// Future returned by an [auth token callback](TursoConnectOptions::auth_token_fn).
pub type AuthTokenFuture = Pin<Box<dyn Future<Output = Result<String, BoxDynError>> + Send>>;

type AuthTokenFn = Arc<dyn Fn() -> AuthTokenFuture + Send + Sync>;

/// Which database a connection opens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    /// An embedded database file.
    File(PathBuf),
    /// An embedded in-memory database, shared by all connections of one options value.
    Memory,
    /// A Turso Cloud database over HTTP. The URL has any `authToken` parameter removed.
    Remote { url: String },
}

/// The statement used to open a transaction when none is given explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TursoTransactionMode {
    /// `BEGIN DEFERRED`
    Deferred,
    /// `BEGIN IMMEDIATE`
    Immediate,
    /// `BEGIN EXCLUSIVE`
    Exclusive,
    /// `BEGIN CONCURRENT` (MVCC; conflicting transactions fail at commit and must be retried)
    Concurrent,
}

impl TursoTransactionMode {
    pub(crate) fn begin_sql(self) -> &'static str {
        match self {
            Self::Deferred => "BEGIN DEFERRED",
            Self::Immediate => "BEGIN IMMEDIATE",
            Self::Exclusive => "BEGIN EXCLUSIVE",
            Self::Concurrent => "BEGIN CONCURRENT",
        }
    }
}

/// Opt-in flags for experimental engine features. Each mirrors a
/// `turso::Builder::experimental_*` method.
#[derive(Debug, Clone, Default)]
struct Experimental {
    encryption: Option<EncryptionOpts>,
    attach: bool,
    custom_types: bool,
    generated_columns: bool,
    index_method: bool,
    materialized_views: bool,
    vacuum: bool,
    multiprocess_wal: bool,
    without_rowid: bool,
}

#[cfg(feature = "sync")]
#[derive(Clone)]
struct SyncOptions {
    enabled: bool,
    remote_url: Option<String>,
    client_name: Option<String>,
    long_poll_timeout: Option<Duration>,
    bootstrap_if_empty: bool,
    partial_sync: Option<PartialSyncOpts>,
    remote_encryption_cipher: Option<RemoteEncryptionCipher>,
}

#[cfg(feature = "sync")]
impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            remote_url: None,
            client_name: None,
            long_poll_timeout: None,
            // Matches `turso::sync::Builder::new_remote`.
            bootstrap_if_empty: true,
            partial_sync: None,
            remote_encryption_cipher: None,
        }
    }
}

/// Options and flags which can be used to configure a Turso connection.
///
/// A connection can open one of three engines:
///
/// * an embedded database file or in-memory database
///   ([`filename`](Self::filename), [`in_memory`](Self::in_memory), `turso:path`,
///   `turso::memory:`);
/// * an embedded database file replicated with Turso Cloud (requires the `sync` feature;
///   [`sync_url`](Self::sync_url));
/// * a Turso Cloud database over HTTP (requires the `serverless` feature;
///   `libsql://`, `turso://`, `https://` or `http://` URLs with a host).
///
/// All connections created from one `TursoConnectOptions` value (including its clones)
/// share the same underlying database handle. Calling any builder method produces a value
/// with a fresh handle.
///
/// # URL format
///
/// | URL                                   | Database                                   |
/// |---------------------------------------|--------------------------------------------|
/// | `turso::memory:`                      | in-memory                                  |
/// | `turso:data.db`, `turso:///abs/data.db` | embedded file                            |
/// | `libsql://db-org.turso.io`, `turso://…`, `https://…`, `http://…` | Turso Cloud |
///
/// Query parameters: `authToken=<token>` (remote), `mode=ro|rw|rwc|memory` (embedded).
#[derive(Clone)]
pub struct TursoConnectOptions {
    pub(crate) target: Target,
    pub(crate) create_if_missing: bool,
    pub(crate) read_only: bool,
    pub(crate) foreign_keys: Option<bool>,
    pub(crate) busy_timeout: Duration,
    pub(crate) concurrent_writes: bool,
    pub(crate) transaction_mode: Option<TursoTransactionMode>,
    pub(crate) pragmas: IndexMap<Cow<'static, str>, Cow<'static, str>>,
    pub(crate) statement_cache_capacity: usize,
    pub(crate) log_settings: LogSettings,
    auth_token: Option<AuthTokenFn>,
    remote_encryption_key: Option<String>,
    experimental: Experimental,
    #[cfg(feature = "sync")]
    sync: SyncOptions,
    pub(crate) shared: SharedDatabase,
}

impl Default for TursoConnectOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl TursoConnectOptions {
    /// Options for an in-memory database.
    pub fn new() -> Self {
        Self {
            target: Target::Memory,
            create_if_missing: false,
            read_only: false,
            foreign_keys: None,
            busy_timeout: Duration::from_secs(5),
            concurrent_writes: false,
            transaction_mode: None,
            pragmas: IndexMap::new(),
            statement_cache_capacity: 100,
            log_settings: LogSettings::default(),
            auth_token: None,
            remote_encryption_key: None,
            experimental: Experimental::default(),
            #[cfg(feature = "sync")]
            sync: SyncOptions::default(),
            shared: SharedDatabase::default(),
        }
    }

    /// Detach from the database handle shared with the value this was cloned from, so
    /// the changed configuration takes effect for new connections.
    fn changed(mut self) -> Self {
        self.shared = SharedDatabase::default();
        self
    }

    /// Open an embedded database file.
    pub fn filename(mut self, path: impl AsRef<Path>) -> Self {
        self.target = Target::File(path.as_ref().to_path_buf());
        self.changed()
    }

    /// Open an embedded in-memory database.
    pub fn in_memory(mut self) -> Self {
        self.target = Target::Memory;
        self.changed()
    }

    /// Connect to a Turso Cloud database over HTTP (requires the `serverless` feature at
    /// connect time). Transactions use `BEGIN` unless
    /// [`concurrent_writes`](Self::concurrent_writes) or a
    /// [transaction mode](Self::transaction_mode) is set.
    ///
    /// The URL is validated when connecting: it must be a `libsql://`, `turso://`,
    /// `https://` or `http://` URL with a host and no query string (pass the token with
    /// [`auth_token`](Self::auth_token)).
    pub fn remote(mut self, url: impl Into<String>) -> Self {
        self.target = Target::Remote { url: url.into() };
        self.changed()
    }

    /// Create the database file if it does not exist (default: `false`).
    pub fn create_if_missing(mut self, create: bool) -> Self {
        self.create_if_missing = create;
        self.changed()
    }

    /// Open the embedded database read-only (default: `false`).
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self.changed()
    }

    /// Enforce foreign key constraints (`PRAGMA foreign_keys`).
    ///
    /// Embedded databases enable this by default, as sqlx-sqlite does.
    ///
    /// Not supported for remote (Turso Cloud) databases: connecting fails with a
    /// configuration error if this is set. Per-connection `PRAGMA` state lives on the
    /// server-side HTTP stream, which expires when idle, so the setting would silently
    /// revert to the server default. Turso Cloud enforces foreign keys by default.
    pub fn foreign_keys(mut self, on: bool) -> Self {
        self.foreign_keys = Some(on);
        self.changed()
    }

    /// How long an embedded connection waits for a lock before failing with a busy
    /// error (default: 5 seconds).
    ///
    /// Has no effect on remote (Turso Cloud) connections, where the server decides how
    /// long to wait.
    pub fn busy_timeout(mut self, timeout: Duration) -> Self {
        self.busy_timeout = timeout;
        self.changed()
    }

    /// Let transactions run concurrently instead of serializing on a single writer.
    ///
    /// Embedded connections switch to Turso's MVCC journal
    /// (`PRAGMA journal_mode = 'mvcc'`) and transactions open with `BEGIN CONCURRENT`
    /// unless another [transaction mode](Self::transaction_mode) is set. Conflicting
    /// transactions fail at commit and must be retried; see
    /// [`TursoError::is_conflict`](crate::TursoError::is_conflict).
    ///
    /// On Turso Cloud this only switches transactions to `BEGIN CONCURRENT`: the server
    /// manages the journal. Concurrent writes on Turso Cloud must be enabled for the database
    /// (an opt-in at the time of writing); servers without it reject `BEGIN CONCURRENT` with a
    /// syntax error.
    ///
    /// DDL (`CREATE`, `ALTER`, `DROP`, ...) is rejected inside a `BEGIN CONCURRENT`
    /// transaction ("DDL statements require an exclusive transaction"); open such
    /// transactions with `Connection::begin_with("BEGIN")` (or `BEGIN IMMEDIATE`).
    /// Migrations do this automatically.
    pub fn concurrent_writes(mut self, on: bool) -> Self {
        self.concurrent_writes = on;
        self.changed()
    }

    /// Statement used to open transactions that don't specify one, overriding the
    /// default (`BEGIN`, or `BEGIN CONCURRENT` with [concurrent writes](Self::concurrent_writes)).
    /// A single transaction can still choose its
    /// own statement with `Connection::begin_with`.
    ///
    /// [`Concurrent`](TursoTransactionMode::Concurrent) transactions cannot run DDL;
    /// see [`concurrent_writes`](Self::concurrent_writes).
    pub fn transaction_mode(mut self, mode: TursoTransactionMode) -> Self {
        self.transaction_mode = Some(mode);
        self.changed()
    }

    /// Set a `PRAGMA` executed on every new connection, after the built-in ones.
    ///
    /// Embedded databases only: connecting to a remote (Turso Cloud) database fails with
    /// a configuration error if any pragma is set, because per-connection `PRAGMA` state
    /// lives on the server-side HTTP stream and silently reverts when the stream expires.
    pub fn pragma(
        mut self,
        key: impl Into<Cow<'static, str>>,
        value: impl Into<Cow<'static, str>>,
    ) -> Self {
        self.pragmas.insert(key.into(), value.into());
        self.changed()
    }

    /// Number of parsed query strings cached per connection (default: 100).
    ///
    /// The cache holds the driver's statement splitting and parameter numbering. Engine
    /// statements are prepared on every execution so they always reflect the current
    /// schema (turso's own statement cache is unbounded and isn't invalidated by schema
    /// changes).
    pub fn statement_cache_capacity(mut self, capacity: usize) -> Self {
        self.statement_cache_capacity = capacity;
        self.changed()
    }

    /// Static auth token for Turso Cloud (remote and sync). Sent as a `Bearer` token.
    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        let token = token.into();
        self.auth_token = Some(Arc::new(move || {
            let token = token.clone();
            Box::pin(async move { Ok(token) })
        }));
        self.changed()
    }

    /// Callback producing an auth token before every HTTP request, for rotating tokens.
    /// Replaces any static token.
    pub fn auth_token_fn<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, BoxDynError>> + Send + 'static,
    {
        self.auth_token = Some(Arc::new(move || Box::pin(f())));
        self.changed()
    }

    /// Base64 encryption key for an encrypted Turso Cloud database (remote and sync),
    /// sent as the `x-turso-encryption-key` header.
    pub fn remote_encryption_key(mut self, base64_key: impl Into<String>) -> Self {
        self.remote_encryption_key = Some(base64_key.into());
        self.changed()
    }

    /// Enable experimental local encryption with the given cipher and key.
    pub fn experimental_encryption(mut self, opts: EncryptionOpts) -> Self {
        self.experimental.encryption = Some(opts);
        self.changed()
    }

    /// Enable experimental `ATTACH DATABASE` support.
    pub fn experimental_attach(mut self, on: bool) -> Self {
        self.experimental.attach = on;
        self.changed()
    }

    /// Enable experimental custom types.
    pub fn experimental_custom_types(mut self, on: bool) -> Self {
        self.experimental.custom_types = on;
        self.changed()
    }

    /// Enable experimental generated columns.
    pub fn experimental_generated_columns(mut self, on: bool) -> Self {
        self.experimental.generated_columns = on;
        self.changed()
    }

    /// Enable experimental index methods.
    pub fn experimental_index_method(mut self, on: bool) -> Self {
        self.experimental.index_method = on;
        self.changed()
    }

    /// Enable experimental materialized views.
    pub fn experimental_materialized_views(mut self, on: bool) -> Self {
        self.experimental.materialized_views = on;
        self.changed()
    }

    /// Enable experimental `VACUUM`.
    pub fn experimental_vacuum(mut self, on: bool) -> Self {
        self.experimental.vacuum = on;
        self.changed()
    }

    /// Enable experimental multi-process WAL.
    pub fn experimental_multiprocess_wal(mut self, on: bool) -> Self {
        self.experimental.multiprocess_wal = on;
        self.changed()
    }

    /// Enable experimental `WITHOUT ROWID` tables.
    pub fn experimental_without_rowid(mut self, on: bool) -> Self {
        self.experimental.without_rowid = on;
        self.changed()
    }

    /// Replicate the embedded database file with a Turso Cloud database. Accepts
    /// `https://`, `http://` and `libsql://` URLs.
    #[cfg(feature = "sync")]
    pub fn sync_url(mut self, url: impl Into<String>) -> Self {
        self.sync.enabled = true;
        self.sync.remote_url = Some(url.into());
        self.changed()
    }

    /// Open the embedded file with the sync engine even without a [`sync_url`](Self::sync_url):
    /// a file that was synced before reads its remote URL from on-disk metadata.
    #[cfg(feature = "sync")]
    pub fn sync(mut self, on: bool) -> Self {
        self.sync.enabled = on;
        self.changed()
    }

    /// Client name reported to the sync server (default: `turso-sync-rust`).
    #[cfg(feature = "sync")]
    pub fn sync_client_name(mut self, name: impl Into<String>) -> Self {
        self.sync.enabled = true;
        self.sync.client_name = Some(name.into());
        self.changed()
    }

    /// How long a pull waits on the server for changes.
    #[cfg(feature = "sync")]
    pub fn sync_long_poll_timeout(mut self, timeout: Duration) -> Self {
        self.sync.enabled = true;
        self.sync.long_poll_timeout = Some(timeout);
        self.changed()
    }

    /// Download the remote database when the local file is empty (default: `true`).
    #[cfg(feature = "sync")]
    pub fn sync_bootstrap_if_empty(mut self, on: bool) -> Self {
        self.sync.enabled = true;
        self.sync.bootstrap_if_empty = on;
        self.changed()
    }

    /// Experimental partial sync configuration.
    #[cfg(feature = "sync")]
    pub fn sync_partial_experimental(mut self, opts: PartialSyncOpts) -> Self {
        self.sync.enabled = true;
        self.sync.partial_sync = Some(opts);
        self.changed()
    }

    /// Cipher of an encrypted Turso Cloud database, used together with
    /// [`remote_encryption_key`](Self::remote_encryption_key) to lay out pages when
    /// syncing without an initial bootstrap.
    #[cfg(feature = "sync")]
    pub fn sync_remote_encryption_cipher(mut self, cipher: RemoteEncryptionCipher) -> Self {
        self.sync.remote_encryption_cipher = Some(cipher);
        self.changed()
    }

    /// The SQL used to open a transaction when none is given.
    pub(crate) fn default_begin_sql(&self) -> &'static str {
        if let Some(mode) = self.transaction_mode {
            return mode.begin_sql();
        }
        if self.concurrent_writes {
            "BEGIN CONCURRENT"
        } else {
            "BEGIN"
        }
    }

    pub(crate) fn is_remote(&self) -> bool {
        matches!(self.target, Target::Remote { .. })
    }

    pub(crate) fn is_sync(&self) -> bool {
        #[cfg(feature = "sync")]
        {
            self.sync.enabled
        }
        #[cfg(not(feature = "sync"))]
        {
            false
        }
    }

    /// The shared database handle, opened on first use.
    pub(crate) async fn database(&self) -> Result<AnyDatabase, Error> {
        let mut slot = self.shared.lock().await;
        if let Some(db) = slot.as_ref() {
            return Ok(db.clone());
        }
        let db = self.open().await.sqlx()?;
        *slot = Some(db.clone());
        Ok(db)
    }

    #[cfg(feature = "sync")]
    async fn sync_database(&self) -> Result<turso::sync::Database, Error> {
        match self.database().await? {
            AnyDatabase::Sync(db) => Ok(db),
            _ => Err(Error::Configuration(
                "the connection is not configured for sync; set sync_url() or sync(true)".into(),
            )),
        }
    }

    /// Push local changes to Turso Cloud.
    ///
    /// Works on the database handle shared by every connection created from these
    /// options (e.g. `pool.connect_options().push()`).
    #[cfg(feature = "sync")]
    pub async fn push(&self) -> Result<(), Error> {
        self.sync_database().await?.push().await.sqlx()
    }

    /// Pull remote changes and apply them locally. Returns `true` when changes were
    /// applied, `false` when the remote had nothing new.
    #[cfg(feature = "sync")]
    pub async fn pull(&self) -> Result<bool, Error> {
        self.sync_database().await?.pull().await.sqlx()
    }

    /// Force a WAL checkpoint of the synced database.
    #[cfg(feature = "sync")]
    pub async fn checkpoint(&self) -> Result<(), Error> {
        self.sync_database().await?.checkpoint().await.sqlx()
    }

    /// Sync statistics of the synced database.
    #[cfg(feature = "sync")]
    pub async fn sync_stats(&self) -> Result<turso::sync::DatabaseSyncStats, Error> {
        self.sync_database().await?.stats().await.sqlx()
    }

    async fn open(&self) -> Result<AnyDatabase, Error> {
        let path = match &self.target {
            Target::Remote { url, .. } => return self.open_remote(url).await,
            Target::Memory => ":memory:".to_string(),
            Target::File(path) => {
                if !self.create_if_missing && !self.is_sync() && !path.exists() {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("database file {} does not exist", path.display()),
                    )));
                }
                path.to_str()
                    .ok_or_else(|| Error::Configuration("database path is not UTF-8".into()))
                    .sqlx()?
                    .to_string()
            }
        };

        if self.is_sync() {
            return self.open_sync(&path).await;
        }

        if self.auth_token.is_some() || self.remote_encryption_key.is_some() {
            return Err(Error::Configuration(
                "an auth token or remote encryption key is set, but the connection opens a \
                 plain embedded database; use a remote URL or a sync URL"
                    .into(),
            ));
        }

        let e = &self.experimental;
        let mut builder = turso::Builder::new_local(&path)
            .read_only(self.read_only)
            .experimental_attach(e.attach)
            .experimental_custom_types(e.custom_types)
            .experimental_generated_columns(e.generated_columns)
            .experimental_index_method(e.index_method)
            .experimental_materialized_views(e.materialized_views)
            .experimental_vacuum(e.vacuum)
            .experimental_multiprocess_wal(e.multiprocess_wal)
            .experimental_without_rowid(e.without_rowid);
        if let Some(opts) = &e.encryption {
            builder = builder
                .experimental_encryption(true)
                .with_encryption(opts.clone());
        }

        Ok(AnyDatabase::Local(builder.build().await.sqlx()?))
    }

    #[cfg(feature = "sync")]
    async fn open_sync(&self, path: &str) -> Result<AnyDatabase, Error> {
        let e = &self.experimental;
        if e.encryption.is_some() || self.read_only {
            return Err(Error::Configuration(
                "local encryption and read-only mode are not supported with sync".into(),
            ));
        }

        let s = &self.sync;
        let mut builder = turso::sync::Builder::new_remote(path)
            .experimental_attach(e.attach)
            .experimental_custom_types(e.custom_types)
            .experimental_generated_columns(e.generated_columns)
            .experimental_index_method(e.index_method)
            .experimental_materialized_views(e.materialized_views)
            .experimental_vacuum(e.vacuum)
            .experimental_multiprocess_wal(e.multiprocess_wal)
            .experimental_without_rowid(e.without_rowid)
            // Bootstrapping needs a remote to bootstrap from.
            .bootstrap_if_empty(s.remote_url.is_some() && s.bootstrap_if_empty);

        if let Some(url) = &s.remote_url {
            builder = builder.with_remote_url(url);
        }
        if let Some(token_fn) = self.auth_token.clone() {
            builder = builder.with_auth_token_fn(move || {
                let fut = token_fn();
                async move { fut.await.map_err(|e| turso::Error::Error(e.to_string())) }
            });
        }
        if let Some(name) = &s.client_name {
            builder = builder.with_client_name(name);
        }
        if let Some(timeout) = s.long_poll_timeout {
            builder = builder.with_long_poll_timeout(timeout);
        }
        if let Some(opts) = &s.partial_sync {
            builder = builder.with_partial_sync_opts_experimental(opts.clone());
        }
        match (&self.remote_encryption_key, s.remote_encryption_cipher) {
            (Some(key), Some(cipher)) => builder = builder.with_remote_encryption(key, cipher),
            (Some(key), None) => builder = builder.with_remote_encryption_key(key),
            (None, Some(_)) => {
                return Err(Error::Configuration(
                    "a remote encryption cipher requires a remote encryption key".into(),
                ));
            }
            (None, None) => {}
        }

        Ok(AnyDatabase::Sync(builder.build().await.sqlx()?))
    }

    #[cfg(not(feature = "sync"))]
    async fn open_sync(&self, _path: &str) -> Result<AnyDatabase, Error> {
        unreachable!("sync is never enabled without the `sync` feature")
    }

    #[cfg(feature = "serverless")]
    async fn open_remote(&self, url: &str) -> Result<AnyDatabase, Error> {
        if self.is_sync() {
            return Err(Error::Configuration(
                "sync replicates an embedded file; use a file path with sync_url() instead of \
                 a remote URL"
                    .into(),
            ));
        }
        let e = &self.experimental;
        if e.encryption.is_some()
            || e.attach
            || e.custom_types
            || e.generated_columns
            || e.index_method
            || e.materialized_views
            || e.vacuum
            || e.multiprocess_wal
            || e.without_rowid
            || self.read_only
        {
            return Err(Error::Configuration(
                "experimental engine options and read-only mode apply to embedded databases \
                 only"
                    .into(),
            ));
        }

        let mut builder = turso_serverless::Builder::new_remote(url);
        if let Some(token_fn) = self.auth_token.clone() {
            builder = builder.with_auth_token_fn(move || {
                let fut = token_fn();
                async move {
                    fut.await
                        .map_err(|e| turso_serverless::Error::Error(e.to_string()))
                }
            });
        }
        if let Some(key) = &self.remote_encryption_key {
            builder = builder.with_remote_encryption_key(key.clone());
        }

        Ok(AnyDatabase::Serverless(builder.build().await.sqlx()?))
    }

    #[cfg(not(feature = "serverless"))]
    async fn open_remote(&self, url: &str) -> Result<AnyDatabase, Error> {
        Err(Error::Configuration(
            format!("connecting to remote database {url} requires the `serverless` feature").into(),
        ))
    }
}

impl fmt::Debug for TursoConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("TursoConnectOptions");
        s.field("target", &self.target)
            .field("create_if_missing", &self.create_if_missing)
            .field("read_only", &self.read_only)
            .field("foreign_keys", &self.foreign_keys)
            .field("busy_timeout", &self.busy_timeout)
            .field("concurrent_writes", &self.concurrent_writes)
            .field("transaction_mode", &self.transaction_mode)
            .field("pragmas", &self.pragmas)
            .field("statement_cache_capacity", &self.statement_cache_capacity)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "remote_encryption_key",
                &self.remote_encryption_key.as_ref().map(|_| "<redacted>"),
            )
            .field("experimental", &self.experimental);
        #[cfg(feature = "sync")]
        s.field("sync_enabled", &self.sync.enabled)
            .field("sync_url", &self.sync.remote_url);
        s.finish_non_exhaustive()
    }
}

impl ConnectOptions for TursoConnectOptions {
    type Connection = TursoConnection;

    fn from_url(url: &Url) -> Result<Self, Error> {
        Self::from_db_url(url)
    }

    fn to_url_lossy(&self) -> Url {
        self.build_url()
    }

    async fn connect(&self) -> Result<Self::Connection, Error> {
        if let Target::Remote { url, .. } = &self.target {
            self.validate_remote(url)?;
        }

        let mut conn = TursoConnection::establish(self).await.sqlx()?;

        if conn.is_remote() {
            // Opening a remote connection does no I/O; make a round trip so a bad URL or
            // token fails here rather than on first use.
            conn.raw_execute("SELECT 1").await?;
        } else {
            // Built-in and user pragmas.
            for statement in self.connect_statements() {
                conn.raw_execute(&statement).await?;
            }
        }

        Ok(conn)
    }

    fn log_statements(mut self, level: log::LevelFilter) -> Self {
        self.log_settings.log_statements(level);
        self
    }

    fn log_slow_statements(mut self, level: log::LevelFilter, duration: Duration) -> Self {
        self.log_settings.log_slow_statements(level, duration);
        self
    }
}

impl TursoConnectOptions {
    /// Pragmas run on every new embedded connection.
    fn connect_statements(&self) -> Vec<String> {
        let mut statements = Vec::new();

        if self.concurrent_writes {
            statements.push("PRAGMA journal_mode = 'mvcc'".to_string());
        }
        let foreign_keys = self.foreign_keys.unwrap_or(true);
        statements.push(format!(
            "PRAGMA foreign_keys = {}",
            if foreign_keys { "ON" } else { "OFF" }
        ));
        for (key, value) in &self.pragmas {
            statements.push(format!("PRAGMA {key} = {value}"));
        }

        statements
    }

    /// Reject remote configurations that can't work: malformed URLs and per-connection
    /// settings that would silently revert when the HTTP stream expires.
    fn validate_remote(&self, url: &str) -> Result<(), Error> {
        let parsed = Url::parse(url).map_err(|e| {
            Error::Configuration(format!("invalid Turso Cloud URL {url:?}: {e}").into())
        })?;
        if !matches!(parsed.scheme(), "libsql" | "turso" | "https" | "http") {
            return Err(Error::Configuration(
                format!(
                    "invalid Turso Cloud URL {url:?}: the scheme must be libsql, turso, https \
                     or http"
                )
                .into(),
            ));
        }
        if parsed.host_str().is_none_or(str::is_empty) {
            return Err(Error::Configuration(
                format!("invalid Turso Cloud URL {url:?}: no host").into(),
            ));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(Error::Configuration(
                "invalid Turso Cloud URL: query strings and fragments are not supported by \
                 TursoConnectOptions::remote(); pass the token with auth_token()"
                    .into(),
            ));
        }

        if self.foreign_keys.is_some() || !self.pragmas.is_empty() {
            return Err(Error::Configuration(
                "foreign_keys() and pragma() are not supported for Turso Cloud connections: \
                 per-connection PRAGMA state lives on the server's HTTP stream, which expires \
                 when idle, so the setting would silently revert to the server default \
                 (Turso Cloud enforces foreign keys by default)"
                    .into(),
            ));
        }
        Ok(())
    }
}
