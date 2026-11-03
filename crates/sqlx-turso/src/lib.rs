//! **Turso** database driver for SQLx.
//!
//! Supports embedded database files and in-memory databases, embedded files replicated
//! with Turso Cloud (`sync` feature) and Turso Cloud over HTTP (`serverless` feature).
//! The SQL dialect, type mappings and behaviour follow `sqlx-sqlite`.

#[macro_use]
extern crate sqlx_core;

pub use arguments::{TursoArgumentValue, TursoArguments, TursoArgumentsBuffer};
pub use column::TursoColumn;
pub use connection::TursoConnection;
pub use database::Turso;
pub use error::{TursoError, TursoErrorKind};
pub use options::{AuthTokenFuture, EncryptionOpts, TursoConnectOptions, TursoTransactionMode};
#[cfg(feature = "sync")]
pub use options::{PartialBootstrapStrategy, PartialSyncOpts, RemoteEncryptionCipher};
pub use query_result::TursoQueryResult;
pub use row::TursoRow;
pub use statement::TursoStatement;
pub use transaction::TursoTransactionManager;
#[cfg(feature = "sync")]
pub use turso::sync::DatabaseSyncStats;
pub use type_info::TursoTypeInfo;
pub use value::{TursoValue, TursoValueRef};

pub(crate) use sqlx_core::driver_prelude::*;

use sqlx_core::executor::Executor;

mod arguments;
mod backend;
mod column;
mod connection;
mod database;
mod error;
mod options;
mod query_result;
mod row;
mod statement;
mod transaction;
mod type_checking;
mod type_info;
pub mod types;
mod value;

#[cfg(feature = "any")]
pub mod any;

#[cfg(feature = "migrate")]
mod migrate;

/// An alias for [`Pool`][crate::pool::Pool], specialized for Turso.
pub type TursoPool = crate::pool::Pool<Turso>;

/// An alias for [`PoolOptions`][crate::pool::PoolOptions], specialized for Turso.
pub type TursoPoolOptions = crate::pool::PoolOptions<Turso>;

/// An alias for [`Executor<'_, Database = Turso>`][Executor].
pub trait TursoExecutor<'c>: Executor<'c, Database = Turso> {}
impl<'c, T: Executor<'c, Database = Turso>> TursoExecutor<'c> for T {}

/// An alias for [`Transaction`][sqlx_core::transaction::Transaction], specialized for Turso.
pub type TursoTransaction<'c> = sqlx_core::transaction::Transaction<'c, Turso>;

impl_into_arguments_for_arguments!(TursoArguments);
impl_column_index_for_row!(TursoRow);
impl_column_index_for_statement!(TursoStatement);
impl_acquire!(Turso, TursoConnection);

// required because some databases have a different handling of NULL
impl_encode_for_option!(Turso);
