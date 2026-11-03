use std::iter::{Extend, IntoIterator};

#[derive(Debug, Default)]
pub struct TursoQueryResult {
    pub(crate) changes: u64,
    pub(crate) last_insert_rowid: i64,
}

impl TursoQueryResult {
    pub fn rows_affected(&self) -> u64 {
        self.changes
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid
    }
}

impl Extend<TursoQueryResult> for TursoQueryResult {
    fn extend<T: IntoIterator<Item = TursoQueryResult>>(&mut self, iter: T) {
        for elem in iter {
            self.changes += elem.changes;
            self.last_insert_rowid = elem.last_insert_rowid;
        }
    }
}

#[cfg(feature = "any")]
impl From<TursoQueryResult> for sqlx_core::any::AnyQueryResult {
    fn from(done: TursoQueryResult) -> Self {
        let last_insert_id = match done.last_insert_rowid() {
            0 => None,
            n => Some(n),
        };
        sqlx_core::any::AnyQueryResult {
            rows_affected: done.rows_affected(),
            last_insert_id,
        }
    }
}
