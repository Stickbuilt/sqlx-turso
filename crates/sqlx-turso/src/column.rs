use crate::ext::ustr::UStr;
use crate::{Turso, TursoTypeInfo};

pub(crate) use sqlx_core::column::*;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "offline", derive(serde::Serialize, serde::Deserialize))]
pub struct TursoColumn {
    pub(crate) name: UStr,
    pub(crate) ordinal: usize,
    pub(crate) type_info: TursoTypeInfo,

    #[cfg_attr(feature = "offline", serde(default))]
    pub(crate) origin: ColumnOrigin,
}

impl Column for TursoColumn {
    type Database = Turso;

    fn ordinal(&self) -> usize {
        self.ordinal
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn type_info(&self) -> &TursoTypeInfo {
        &self.type_info
    }

    fn origin(&self) -> ColumnOrigin {
        self.origin.clone()
    }
}
