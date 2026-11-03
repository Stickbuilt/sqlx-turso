use std::sync::Arc;

pub(crate) use sqlx_core::arguments::*;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::{BoxDynError, Error};

use crate::Turso;
use crate::connection::parse::Parameter;

/// A value bound to a query parameter.
///
/// Mirrors `sqlx_sqlite::SqliteArgumentValue` so the type mappings stay identical.
#[derive(Debug, Clone)]
pub enum TursoArgumentValue {
    Null,
    Text(Arc<String>),
    TextSlice(Arc<str>),
    Blob(Arc<Vec<u8>>),
    Double(f64),
    Int(i32),
    Int64(i64),
}

/// Arguments for a Turso query.
#[derive(Default, Debug, Clone)]
pub struct TursoArguments {
    pub(crate) values: TursoArgumentsBuffer,
}

/// Buffer that [`Encode`] implementations push values into.
#[derive(Default, Debug, Clone)]
pub struct TursoArgumentsBuffer(Vec<TursoArgumentValue>);

impl TursoArguments {
    pub(crate) fn add<'t, T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: Encode<'t, Turso>,
    {
        let value_length_before_encoding = self.values.0.len();

        match value.encode(&mut self.values) {
            Ok(IsNull::Yes) => self.values.0.push(TursoArgumentValue::Null),
            Ok(IsNull::No) => {}
            Err(error) => {
                // Don't leave a half-encoded value behind.
                self.values.0.truncate(value_length_before_encoding);
                return Err(error);
            }
        };

        Ok(())
    }

    /// The encoded values, in bind order.
    pub fn values(&self) -> &[TursoArgumentValue] {
        &self.values.0
    }

    /// Build the positional parameter list for one statement.
    ///
    /// `offset` is the number of anonymous (`?`) arguments consumed by previous
    /// statements of the same query string; the number consumed by this statement
    /// is returned alongside the values. Numbering follows sqlx-sqlite:
    /// `?NNN` and `$NNN` address argument `NNN` directly, `?` takes the next
    /// argument in sequence, and parameters past the end of the argument list
    /// are bound as NULL.
    pub(crate) fn bind(
        &self,
        parameters: &[Parameter],
        offset: usize,
    ) -> Result<(Vec<turso::Value>, usize), Error> {
        let mut arg_i = offset;
        let mut values = Vec::with_capacity(parameters.len());

        for parameter in parameters {
            let n = match parameter {
                Parameter::Anonymous => {
                    arg_i += 1;
                    arg_i
                }
                Parameter::Numbered(n) => *n,
                Parameter::Named(name) => {
                    return Err(sqlx_core::err_protocol!(
                        "parameters with non-integer names are not currently supported: {}",
                        name
                    ));
                }
            };

            values.push(match self.values.0.get(n.wrapping_sub(1)) {
                Some(value) => value.to_turso(),
                // SQLite treats unbound parameters as NULL; reproduce that.
                None => turso::Value::Null,
            });
        }

        Ok((values, arg_i - offset))
    }
}

impl Arguments for TursoArguments {
    type Database = Turso;

    fn reserve(&mut self, len: usize, _size_hint: usize) {
        self.values.0.reserve(len);
    }

    fn add<'t, T>(&mut self, value: T) -> Result<(), BoxDynError>
    where
        T: Encode<'t, Self::Database>,
    {
        self.add(value)
    }

    fn len(&self) -> usize {
        self.values.0.len()
    }
}

impl TursoArgumentsBuffer {
    #[allow(dead_code)]
    pub(crate) fn new(values: Vec<TursoArgumentValue>) -> TursoArgumentsBuffer {
        Self(values)
    }

    /// Append an encoded value.
    pub fn push(&mut self, value: TursoArgumentValue) {
        self.0.push(value);
    }
}

impl TursoArgumentValue {
    pub(crate) fn to_turso(&self) -> turso::Value {
        use TursoArgumentValue::*;

        match self {
            Null => turso::Value::Null,
            Text(v) => turso::Value::Text(String::clone(v)),
            TextSlice(v) => turso::Value::Text(v.to_string()),
            Blob(v) => turso::Value::Blob(Vec::clone(v)),
            Double(v) => turso::Value::Real(*v),
            Int(v) => turso::Value::Integer(i64::from(*v)),
            Int64(v) => turso::Value::Integer(*v),
        }
    }
}
