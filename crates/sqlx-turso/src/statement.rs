use std::sync::Arc;

use sqlx_core::column::ColumnIndex;
use sqlx_core::error::Error;
use sqlx_core::ext::ustr::UStr;
use sqlx_core::sql_str::SqlStr;
use sqlx_core::{Either, HashMap};

pub(crate) use sqlx_core::statement::*;

use crate::{Turso, TursoArguments, TursoColumn, TursoTypeInfo};

/// A prepared (described) Turso statement.
#[derive(Debug, Clone)]
#[allow(clippy::rc_buffer)]
pub struct TursoStatement {
    pub(crate) sql: SqlStr,
    pub(crate) parameters: usize,
    pub(crate) columns: Arc<Vec<TursoColumn>>,
    pub(crate) column_names: Arc<HashMap<UStr, usize>>,
}

impl Statement for TursoStatement {
    type Database = Turso;

    fn into_sql(self) -> SqlStr {
        self.sql
    }

    fn sql(&self) -> &SqlStr {
        &self.sql
    }

    fn parameters(&self) -> Option<Either<&[TursoTypeInfo], usize>> {
        Some(Either::Right(self.parameters))
    }

    fn columns(&self) -> &[TursoColumn] {
        &self.columns
    }

    impl_statement_query!(TursoArguments);
}

impl ColumnIndex<TursoStatement> for &'_ str {
    fn index(&self, statement: &TursoStatement) -> Result<usize, Error> {
        statement
            .column_names
            .get(*self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
            .copied()
    }
}
