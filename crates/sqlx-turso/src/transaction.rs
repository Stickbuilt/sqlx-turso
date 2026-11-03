use std::future::Future;

use sqlx_core::transaction::TransactionManager;
use sqlx_core::{error::Error, sql_str::SqlStr};

use crate::{Turso, TursoConnection};

/// Implementation of [`TransactionManager`] for Turso.
///
/// Top-level transactions open with the connection's default `BEGIN` statement
/// (see [`TursoConnectOptions::transaction_mode`](crate::TursoConnectOptions::transaction_mode));
/// nested transactions use savepoints.
pub struct TursoTransactionManager;

impl TransactionManager for TursoTransactionManager {
    type Database = Turso;

    fn begin(
        conn: &mut TursoConnection,
        statement: Option<SqlStr>,
    ) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        conn.begin_tx(statement)
    }

    fn commit(conn: &mut TursoConnection) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        conn.commit_tx()
    }

    fn rollback(conn: &mut TursoConnection) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        conn.rollback_tx()
    }

    fn start_rollback(conn: &mut TursoConnection) {
        conn.start_rollback_tx();
    }

    fn get_transaction_depth(conn: &TursoConnection) -> usize {
        conn.transaction_depth
    }
}
