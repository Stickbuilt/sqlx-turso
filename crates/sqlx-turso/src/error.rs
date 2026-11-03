use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt::{self, Display, Formatter};

pub(crate) use sqlx_core::error::*;

/// Class of a database error reported by Turso.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TursoErrorKind {
    /// Generic SQL error (syntax error, missing table, ...).
    Error,
    /// The database is locked by another writer.
    Busy,
    /// A snapshot conflict; the transaction must be retried.
    BusySnapshot,
    /// The operation was interrupted.
    Interrupt,
    /// The API was used incorrectly.
    Misuse,
    /// A constraint (UNIQUE, FOREIGN KEY, NOT NULL, CHECK, ...) was violated.
    Constraint,
    /// A write was attempted on a read-only database.
    Readonly,
    /// The database is full.
    DatabaseFull,
    /// The file is not a database.
    NotADatabase,
    /// The database file is corrupt.
    Corrupt,
}

/// An error returned by the Turso engine or the Turso Cloud server.
#[derive(Debug, Clone)]
pub struct TursoError {
    kind: TursoErrorKind,
    message: String,
}

// SQLite result codes, reported from `DatabaseError::code()` so that callers matching
// on sqlx-sqlite's codes keep working.
const SQLITE_ERROR: i32 = 1;
const SQLITE_BUSY: i32 = 5;
const SQLITE_READONLY: i32 = 8;
const SQLITE_INTERRUPT: i32 = 9;
const SQLITE_CORRUPT: i32 = 11;
const SQLITE_FULL: i32 = 13;
const SQLITE_CONSTRAINT: i32 = 19;
const SQLITE_MISUSE: i32 = 21;
const SQLITE_NOTADB: i32 = 26;
const SQLITE_CONSTRAINT_CHECK: i32 = 275;
const SQLITE_BUSY_SNAPSHOT: i32 = 517;
const SQLITE_CONSTRAINT_FOREIGNKEY: i32 = 787;
const SQLITE_CONSTRAINT_NOTNULL: i32 = 1299;
const SQLITE_CONSTRAINT_PRIMARYKEY: i32 = 1555;
const SQLITE_CONSTRAINT_UNIQUE: i32 = 2067;

impl TursoError {
    pub(crate) fn new(kind: TursoErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// The class of this error.
    pub fn turso_kind(&self) -> TursoErrorKind {
        self.kind
    }

    /// Whether the operation failed because of a conflict with a concurrent
    /// transaction and may succeed if retried (busy database, MVCC write conflict,
    /// concurrent schema change on Turso Cloud).
    pub fn is_conflict(&self) -> bool {
        match self.kind {
            TursoErrorKind::Busy | TursoErrorKind::BusySnapshot => true,
            TursoErrorKind::Error => {
                self.message.contains("conflict") || self.message.contains("schema changed")
            }
            _ => false,
        }
    }

    /// The SQLite (extended) result code equivalent to this error.
    pub fn sqlite_code(&self) -> i32 {
        match self.kind {
            TursoErrorKind::Error => SQLITE_ERROR,
            TursoErrorKind::Busy => SQLITE_BUSY,
            TursoErrorKind::BusySnapshot => SQLITE_BUSY_SNAPSHOT,
            TursoErrorKind::Interrupt => SQLITE_INTERRUPT,
            TursoErrorKind::Misuse => SQLITE_MISUSE,
            TursoErrorKind::Readonly => SQLITE_READONLY,
            TursoErrorKind::DatabaseFull => SQLITE_FULL,
            TursoErrorKind::NotADatabase => SQLITE_NOTADB,
            TursoErrorKind::Corrupt => SQLITE_CORRUPT,
            TursoErrorKind::Constraint => {
                // The engine reports constraint failures with SQLite's message text; Turso
                // Cloud prefixes it with "SQLite error: ".
                let m = self.message.as_str();
                let m = m.strip_prefix("SQLite error: ").unwrap_or(m);
                if m.starts_with("UNIQUE constraint failed") {
                    SQLITE_CONSTRAINT_UNIQUE
                } else if m.starts_with("PRIMARY KEY constraint failed") {
                    SQLITE_CONSTRAINT_PRIMARYKEY
                } else if m.starts_with("FOREIGN KEY constraint failed") {
                    SQLITE_CONSTRAINT_FOREIGNKEY
                } else if m.starts_with("NOT NULL constraint failed") {
                    SQLITE_CONSTRAINT_NOTNULL
                } else if m.starts_with("CHECK constraint failed") {
                    SQLITE_CONSTRAINT_CHECK
                } else {
                    SQLITE_CONSTRAINT
                }
            }
        }
    }
}

impl Display for TursoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(code: {}) {}", self.sqlite_code(), self.message)
    }
}

impl StdError for TursoError {}

impl DatabaseError for TursoError {
    fn message(&self) -> &str {
        &self.message
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(self.sqlite_code().to_string().into())
    }

    fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
        self
    }

    fn kind(&self) -> ErrorKind {
        match self.sqlite_code() {
            SQLITE_CONSTRAINT_UNIQUE | SQLITE_CONSTRAINT_PRIMARYKEY => ErrorKind::UniqueViolation,
            SQLITE_CONSTRAINT_FOREIGNKEY => ErrorKind::ForeignKeyViolation,
            SQLITE_CONSTRAINT_NOTNULL => ErrorKind::NotNullViolation,
            SQLITE_CONSTRAINT_CHECK => ErrorKind::CheckViolation,
            _ => ErrorKind::Other,
        }
    }
}

/// Conversion of engine errors into [`sqlx_core::Error`].
pub(crate) trait IntoSqlxError {
    fn into_sqlx(self) -> Error;
}

/// `result.sqlx()?` converts an engine error into [`sqlx_core::Error`].
pub(crate) trait SqlxResultExt<T> {
    fn sqlx(self) -> Result<T, Error>;
}

impl<T, E: IntoSqlxError> SqlxResultExt<T> for Result<T, E> {
    fn sqlx(self) -> Result<T, Error> {
        self.map_err(IntoSqlxError::into_sqlx)
    }
}

impl IntoSqlxError for Error {
    fn into_sqlx(self) -> Error {
        self
    }
}

impl IntoSqlxError for turso::Error {
    fn into_sqlx(self) -> Error {
        let error = self;
        use turso::Error as E;

        let db = |kind, message: String| Error::Database(Box::new(TursoError::new(kind, message)));

        match error {
            E::Busy(m) => db(TursoErrorKind::Busy, m),
            E::BusySnapshot(m) => db(TursoErrorKind::BusySnapshot, m),
            E::Interrupt(m) => db(TursoErrorKind::Interrupt, m),
            E::Error(m) => db(TursoErrorKind::Error, m),
            E::Misuse(m) => db(TursoErrorKind::Misuse, m),
            E::Constraint(m) => db(TursoErrorKind::Constraint, m),
            E::Readonly(m) => db(TursoErrorKind::Readonly, m),
            E::DatabaseFull(m) => db(TursoErrorKind::DatabaseFull, m),
            E::NotAdb(m) => db(TursoErrorKind::NotADatabase, m),
            E::Corrupt(m) => db(TursoErrorKind::Corrupt, m),
            E::IoError(kind, op) => Error::Io(std::io::Error::new(kind, op)),
            E::QueryReturnedNoRows => Error::RowNotFound,
            E::ConversionFailure(m) => Error::Decode(m.into()),
            E::ToSqlConversionFailure(e) => Error::Encode(e),
            E::BatchStatementFailed { error, .. } => error.into_sqlx(),
            e @ E::BatchRollbackFailed { .. } => Error::Protocol(e.to_string()),
        }
    }
}

#[cfg(feature = "serverless")]
impl IntoSqlxError for turso_serverless::Error {
    fn into_sqlx(self) -> Error {
        let error = self;
        use turso_serverless::Error as E;

        let db = |kind, message: String| Error::Database(Box::new(TursoError::new(kind, message)));

        match error {
            E::Busy(m) => db(TursoErrorKind::Busy, m),
            E::BusySnapshot(m) => db(TursoErrorKind::BusySnapshot, m),
            E::Interrupt(m) => db(TursoErrorKind::Interrupt, m),
            E::Error(m) => db(TursoErrorKind::Error, m),
            E::Misuse(m) => db(TursoErrorKind::Misuse, m),
            E::Constraint(m) => db(TursoErrorKind::Constraint, m),
            E::Readonly(m) => db(TursoErrorKind::Readonly, m),
            E::DatabaseFull(m) => db(TursoErrorKind::DatabaseFull, m),
            E::NotAdb(m) => db(TursoErrorKind::NotADatabase, m),
            E::Corrupt(m) => db(TursoErrorKind::Corrupt, m),
            // Transport failures: the pool should treat the connection as broken.
            E::Http(m) => Error::Io(std::io::Error::other(m)),
            E::QueryReturnedNoRows => Error::RowNotFound,
            E::ConversionFailure(m) => Error::Decode(m.into()),
            E::ToSqlConversionFailure(e) => Error::Encode(e),
            E::BatchStatementFailed { error, .. } => error.into_sqlx(),
            other => Error::Protocol(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_embedded_and_cloud_constraint_messages() {
        for prefix in ["", "SQLite error: "] {
            let kind = |m: &str| {
                TursoError::new(TursoErrorKind::Constraint, format!("{prefix}{m}")).kind()
            };
            assert_eq!(
                kind("UNIQUE constraint failed: t.x"),
                ErrorKind::UniqueViolation
            );
            assert_eq!(
                kind("PRIMARY KEY constraint failed: t.id"),
                ErrorKind::UniqueViolation
            );
            assert_eq!(
                kind("FOREIGN KEY constraint failed"),
                ErrorKind::ForeignKeyViolation
            );
            assert_eq!(
                kind("NOT NULL constraint failed: t.y"),
                ErrorKind::NotNullViolation
            );
            assert_eq!(
                kind("CHECK constraint failed: z > 0"),
                ErrorKind::CheckViolation
            );
        }
    }
}
