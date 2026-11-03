use std::sync::Arc;

use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use sqlx_core::column::ColumnOrigin;
use sqlx_core::describe::Describe;
use sqlx_core::error::Error;
use sqlx_core::executor::{Execute, Executor};
use sqlx_core::ext::ustr::UStr;
use sqlx_core::logger::QueryLogger;
use sqlx_core::sql_str::SqlStr;
use sqlx_core::{Either, HashMap};

use super::parse::{Parameter, ParsedQuery};
use crate::arguments::argument_indexes;
use crate::backend::RawColumn;
use crate::type_info::DataType;
use crate::{
    Turso, TursoArguments, TursoColumn, TursoConnection, TursoQueryResult, TursoRow,
    TursoStatement, TursoTypeInfo,
};

pub(crate) fn columns_of(raw: &[RawColumn]) -> (Arc<Vec<TursoColumn>>, Arc<HashMap<UStr, usize>>) {
    let mut columns = Vec::with_capacity(raw.len());
    let mut names = HashMap::with_capacity(raw.len());

    for (ordinal, column) in raw.iter().enumerate() {
        let name = UStr::new(&column.name);
        let type_info = column
            .decl_type
            .as_deref()
            .and_then(|decl| decl.parse::<DataType>().ok())
            .map(TursoTypeInfo)
            // Expressions have no declared type; the value's storage class decides.
            .unwrap_or(TursoTypeInfo(DataType::Null));

        names.insert(name.clone(), ordinal);
        columns.push(TursoColumn {
            name,
            ordinal,
            type_info,
            origin: ColumnOrigin::Unknown,
        });
    }

    (Arc::new(columns), Arc::new(names))
}

/// Number of distinct arguments a query consumes, assigned across its statements the
/// way [`TursoArguments::bind`] assigns them (e.g. `SELECT $1; SELECT $1` takes one).
/// Named parameters, which can't be bound, count once per distinct name.
fn distinct_arguments(parsed: &ParsedQuery) -> usize {
    let mut indexes = std::collections::BTreeSet::new();
    let mut names = std::collections::BTreeSet::new();
    let mut offset = 0;
    for statement in &parsed.statements {
        let mut unnamed = Vec::with_capacity(statement.parameters.len());
        for parameter in &statement.parameters {
            match parameter {
                Parameter::Named(name) => {
                    names.insert(name.clone());
                }
                other => unnamed.push(other.clone()),
            }
        }
        if let Ok((slots, used)) = argument_indexes(&unnamed, offset) {
            indexes.extend(slots);
            offset += used;
        }
    }
    indexes.len() + names.len()
}

impl TursoConnection {
    fn parsed(&mut self, sql: &str, persistent: bool) -> Result<Arc<ParsedQuery>, Error> {
        if persistent && self.state.statements.is_enabled() {
            if let Some(parsed) = self.state.statements.get_mut(sql) {
                return Ok(Arc::clone(parsed));
            }
            let parsed = ParsedQuery::parse(sql)?;
            self.state.statements.insert(sql, Arc::clone(&parsed));
            return Ok(parsed);
        }
        ParsedQuery::parse(sql)
    }

    pub(crate) fn run<'e>(
        &'e mut self,
        sql: SqlStr,
        arguments: Option<TursoArguments>,
        persistent: bool,
        limit: Option<usize>,
    ) -> BoxStream<'e, Result<Either<TursoQueryResult, TursoRow>, Error>> {
        Box::pin(sqlx_core::try_stream! {
            self.settle().await?;

            let parsed = self.parsed(sql.as_str(), persistent)?;
            let mut logger = QueryLogger::new(sql, self.state.log_settings.clone());
            let mut args_used = 0;
            let mut rows_returned = 0;

            for statement in &parsed.statements {
                let params = match &arguments {
                    Some(arguments) => {
                        let (params, used) = arguments.bind(&statement.parameters, args_used)?;
                        args_used += used;
                        params
                    }
                    None => vec![turso::Value::Null; statement.parameters.len()],
                };

                self.check_not_aborted()?;
                let mut cursor = self.raw_run(&statement.sql, params).await?;
                // Column metadata is final only once the statement has been stepped.
                let mut next = cursor.next().await?;
                let (columns, column_names) = columns_of(cursor.columns());

                while let Some(values) = next {
                    logger.increment_rows_returned();
                    rows_returned += 1;
                    r#yield!(Either::Right(TursoRow::new(values, &columns, &column_names)));

                    if limit.is_some_and(|limit| rows_returned >= limit) {
                        return Ok(());
                    }
                    next = cursor.next().await?;
                }

                let (changes, last_insert_rowid) = cursor.outcome();
                logger.increase_rows_affected(changes);
                r#yield!(Either::Left(TursoQueryResult {
                    changes,
                    last_insert_rowid,
                }));
            }

            Ok(())
        })
    }

    pub(crate) async fn prepare_statement(&mut self, sql: SqlStr) -> Result<TursoStatement, Error> {
        self.settle().await?;

        let parsed = ParsedQuery::parse(sql.as_str())?;
        // Like sqlx-sqlite, a multi-statement query describes its last statement.
        let (raw_columns, parameters) = match parsed.statements.last() {
            Some(last) => (
                self.state.raw.describe(&last.sql).await?,
                distinct_arguments(&parsed),
            ),
            None => (Vec::new(), 0),
        };
        let (columns, column_names) = columns_of(&raw_columns);

        Ok(TursoStatement {
            sql,
            parameters,
            columns,
            column_names,
        })
    }
}

impl<'c> Executor<'c> for &'c mut TursoConnection {
    type Database = Turso;

    fn fetch_many<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxStream<'e, Result<Either<TursoQueryResult, TursoRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let arguments = match query.take_arguments().map_err(Error::Encode) {
            Ok(arguments) => arguments,
            Err(error) => return futures_util::stream::once(async move { Err(error) }).boxed(),
        };
        let persistent = query.persistent() && arguments.is_some();
        let sql = query.sql();

        self.run(sql, arguments, persistent, None)
    }

    fn fetch_optional<'e, 'q, E>(
        self,
        mut query: E,
    ) -> BoxFuture<'e, Result<Option<TursoRow>, Error>>
    where
        'c: 'e,
        E: Execute<'q, Self::Database>,
        'q: 'e,
        E: 'q,
    {
        let arguments = match query.take_arguments().map_err(Error::Encode) {
            Ok(arguments) => arguments,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let persistent = query.persistent() && arguments.is_some();
        let sql = query.sql();

        Box::pin(async move {
            let mut stream = self.run(sql, arguments, persistent, Some(1));
            while let Some(result) = stream.try_next().await? {
                if let Either::Right(row) = result {
                    return Ok(Some(row));
                }
            }
            Ok(None)
        })
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        _parameters: &'e [TursoTypeInfo],
    ) -> BoxFuture<'e, Result<TursoStatement, Error>>
    where
        'c: 'e,
    {
        Box::pin(self.prepare_statement(sql))
    }

    #[doc(hidden)]
    #[cfg(feature = "offline")]
    fn describe<'e>(self, sql: SqlStr) -> BoxFuture<'e, Result<Describe<Turso>, Error>>
    where
        'c: 'e,
    {
        Box::pin(async move {
            let statement = self.prepare_statement(sql).await?;
            let columns = statement.columns.as_ref().clone();
            // Nullability isn't reported by the engine.
            let nullable = vec![None; columns.len()];
            Ok(Describe {
                parameters: Some(Either::Right(statement.parameters)),
                columns,
                nullable,
            })
        })
    }
}

#[allow(unused_imports)]
use Describe as _;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_distinct_arguments() {
        let n = |sql| distinct_arguments(&ParsedQuery::parse(sql).unwrap());
        assert_eq!(n("SELECT 1"), 0);
        assert_eq!(n("SELECT ?, ?"), 2);
        assert_eq!(n("SELECT $1; SELECT $1"), 1);
        assert_eq!(n("SELECT ?1, ?1; SELECT ?1 + ?1"), 1);
        assert_eq!(n("INSERT INTO t VALUES (?); SELECT ?"), 2);
        assert_eq!(n("SELECT ?2; SELECT ?1"), 2);
        assert_eq!(n("SELECT ?3"), 3);
        assert_eq!(n("SELECT :a, :a; SELECT :a"), 1);
    }
}
