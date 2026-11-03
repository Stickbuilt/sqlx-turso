#![allow(clippy::rc_buffer)]

use std::sync::Arc;

use sqlx_core::HashMap;
use sqlx_core::column::ColumnIndex;
use sqlx_core::error::Error;
use sqlx_core::ext::ustr::UStr;
use sqlx_core::row::{Row, debug_row};

use crate::{Turso, TursoColumn, TursoValue, TursoValueRef};

/// A row returned from a Turso query.
pub struct TursoRow {
    pub(crate) values: Box<[TursoValue]>,
    pub(crate) columns: Arc<Vec<TursoColumn>>,
    pub(crate) column_names: Arc<HashMap<UStr, usize>>,
}

impl TursoRow {
    pub(crate) fn new(
        values: Vec<turso::Value>,
        columns: &Arc<Vec<TursoColumn>>,
        column_names: &Arc<HashMap<UStr, usize>>,
    ) -> Self {
        let values = values
            .into_iter()
            .enumerate()
            .map(|(i, value)| {
                let column_type = columns
                    .get(i)
                    .map(|c| c.type_info.clone())
                    .filter(|t| !sqlx_core::type_info::TypeInfo::is_null(t));
                TursoValue::new(value, column_type)
            })
            .collect();

        Self {
            values,
            columns: Arc::clone(columns),
            column_names: Arc::clone(column_names),
        }
    }

    /// The raw values of this row, in column order.
    pub fn values(&self) -> &[TursoValue] {
        &self.values
    }
}

impl Row for TursoRow {
    type Database = Turso;

    fn columns(&self) -> &[TursoColumn] {
        &self.columns
    }

    fn try_get_raw<I>(&self, index: I) -> Result<TursoValueRef<'_>, Error>
    where
        I: ColumnIndex<Self>,
    {
        let index = index.index(self)?;
        Ok(TursoValueRef::value(&self.values[index]))
    }
}

impl ColumnIndex<TursoRow> for &'_ str {
    fn index(&self, row: &TursoRow) -> Result<usize, Error> {
        row.column_names
            .get(*self)
            .ok_or_else(|| Error::ColumnNotFound((*self).into()))
            .copied()
    }
}

impl std::fmt::Debug for TursoRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        debug_row(self, f)
    }
}
