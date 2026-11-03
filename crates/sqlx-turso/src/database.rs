pub(crate) use sqlx_core::database::{Database, HasStatementCache};

use crate::arguments::TursoArgumentsBuffer;
use crate::{
    TursoArguments, TursoColumn, TursoConnection, TursoQueryResult, TursoRow, TursoStatement,
    TursoTransactionManager, TursoTypeInfo, TursoValue, TursoValueRef,
};

/// Turso database driver.
#[derive(Debug)]
pub struct Turso;

impl Database for Turso {
    type Connection = TursoConnection;

    type TransactionManager = TursoTransactionManager;

    type Row = TursoRow;

    type QueryResult = TursoQueryResult;

    type Column = TursoColumn;

    type TypeInfo = TursoTypeInfo;

    type Value = TursoValue;
    type ValueRef<'r> = TursoValueRef<'r>;

    type Arguments = TursoArguments;
    type ArgumentBuffer = TursoArgumentsBuffer;

    type Statement = TursoStatement;

    const NAME: &'static str = "Turso";

    const URL_SCHEMES: &'static [&'static str] = &["turso", "libsql", "https", "http"];
}

impl HasStatementCache for Turso {}
