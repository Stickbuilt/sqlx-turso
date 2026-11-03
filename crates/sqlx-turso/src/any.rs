use crate::{
    Either, Turso, TursoArgumentValue, TursoArguments, TursoColumn, TursoConnectOptions,
    TursoConnection, TursoQueryResult, TursoRow, TursoTransactionManager, TursoTypeInfo,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{FutureExt, StreamExt, TryStreamExt};

use sqlx_core::any::{
    AnyArguments, AnyColumn, AnyConnectOptions, AnyConnectionBackend, AnyQueryResult, AnyRow,
    AnyStatement, AnyTypeInfo, AnyTypeInfoKind, AnyValueKind,
};
use sqlx_core::sql_str::SqlStr;

use crate::arguments::TursoArgumentsBuffer;
use crate::type_info::DataType;
use sqlx_core::connection::{ConnectOptions, Connection};
use sqlx_core::database::Database;
use sqlx_core::executor::Executor;
use sqlx_core::transaction::TransactionManager;
use std::pin::pin;
use std::sync::Arc;

sqlx_core::declare_driver_with_optional_migrate!(DRIVER = Turso);

impl AnyConnectionBackend for TursoConnection {
    fn name(&self) -> &str {
        <Turso as Database>::NAME
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close(*self).boxed()
    }

    fn close_hard(self: Box<Self>) -> BoxFuture<'static, sqlx_core::Result<()>> {
        Connection::close_hard(*self).boxed()
    }

    fn ping(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::ping(self).boxed()
    }

    fn begin(&mut self, statement: Option<SqlStr>) -> BoxFuture<'_, sqlx_core::Result<()>> {
        TursoTransactionManager::begin(self, statement).boxed()
    }

    fn commit(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        TursoTransactionManager::commit(self).boxed()
    }

    fn rollback(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        TursoTransactionManager::rollback(self).boxed()
    }

    fn start_rollback(&mut self) {
        TursoTransactionManager::start_rollback(self)
    }

    fn get_transaction_depth(&self) -> usize {
        TursoTransactionManager::get_transaction_depth(self)
    }

    fn shrink_buffers(&mut self) {
        // NO-OP.
    }

    fn flush(&mut self) -> BoxFuture<'_, sqlx_core::Result<()>> {
        Connection::flush(self).boxed()
    }

    fn should_flush(&self) -> bool {
        Connection::should_flush(self)
    }

    #[cfg(feature = "migrate")]
    fn as_migrate(
        &mut self,
    ) -> sqlx_core::Result<&mut (dyn sqlx_core::migrate::Migrate + Send + 'static)> {
        Ok(self)
    }

    fn fetch_many(
        &mut self,
        query: SqlStr,
        persistent: bool,
        arguments: Option<AnyArguments>,
    ) -> BoxStream<'_, sqlx_core::Result<Either<AnyQueryResult, AnyRow>>> {
        let persistent = persistent && arguments.is_some();
        let args = arguments.map(map_arguments);

        Box::pin(self.run(query, args, persistent, None).map(
            move |res: sqlx_core::Result<Either<TursoQueryResult, TursoRow>>| match res? {
                Either::Left(result) => Ok(Either::Left(result.into())),
                Either::Right(row) => Ok(Either::Right(AnyRow::try_from(&row)?)),
            },
        ))
    }

    fn fetch_optional(
        &mut self,
        query: SqlStr,
        persistent: bool,
        arguments: Option<AnyArguments>,
    ) -> BoxFuture<'_, sqlx_core::Result<Option<AnyRow>>> {
        let persistent = persistent && arguments.is_some();
        let args = arguments.map(map_arguments);

        Box::pin(async move {
            let mut stream = pin!(self.run(query, args, persistent, Some(1)));

            while let Some(result) = stream.try_next().await? {
                if let Either::Right(row) = result {
                    return Ok(Some(AnyRow::try_from(&row)?));
                }
            }

            Ok(None)
        })
    }

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: SqlStr,
        _parameters: &[AnyTypeInfo],
    ) -> BoxFuture<'c, sqlx_core::Result<AnyStatement>> {
        Box::pin(async move {
            let statement = Executor::prepare_with(self, sql, &[]).await?;
            let column_names = statement.column_names.clone();
            AnyStatement::try_from_statement(statement, column_names)
        })
    }

    #[cfg(feature = "offline")]
    fn describe(
        &mut self,
        sql: SqlStr,
    ) -> BoxFuture<'_, sqlx_core::Result<sqlx_core::describe::Describe<sqlx_core::any::Any>>> {
        Box::pin(async move { Executor::describe(self, sql).await?.try_into_any() })
    }
}

impl<'a> TryFrom<&'a TursoTypeInfo> for AnyTypeInfo {
    type Error = sqlx_core::Error;

    fn try_from(sqlite_type: &'a TursoTypeInfo) -> Result<Self, Self::Error> {
        Ok(AnyTypeInfo {
            kind: match &sqlite_type.0 {
                DataType::Null => AnyTypeInfoKind::Null,
                DataType::Int4 => AnyTypeInfoKind::Integer,
                DataType::Integer => AnyTypeInfoKind::BigInt,
                DataType::Float => AnyTypeInfoKind::Double,
                DataType::Blob => AnyTypeInfoKind::Blob,
                DataType::Text => AnyTypeInfoKind::Text,
                _ => {
                    return Err(sqlx_core::Error::AnyDriverError(
                        format!("Any driver does not support the Turso type {sqlite_type:?}")
                            .into(),
                    ));
                }
            },
        })
    }
}

impl<'a> TryFrom<&'a TursoColumn> for AnyColumn {
    type Error = sqlx_core::Error;

    fn try_from(col: &'a TursoColumn) -> Result<Self, Self::Error> {
        let type_info =
            AnyTypeInfo::try_from(&col.type_info).map_err(|e| sqlx_core::Error::ColumnDecode {
                index: col.name.to_string(),
                source: e.into(),
            })?;

        Ok(AnyColumn {
            ordinal: col.ordinal,
            name: col.name.clone(),
            type_info,
        })
    }
}

impl<'a> TryFrom<&'a TursoRow> for AnyRow {
    type Error = sqlx_core::Error;

    fn try_from(row: &'a TursoRow) -> Result<Self, Self::Error> {
        AnyRow::map_from(row, row.column_names.clone())
    }
}

impl<'a> TryFrom<&'a AnyConnectOptions> for TursoConnectOptions {
    type Error = sqlx_core::Error;

    fn try_from(opts: &'a AnyConnectOptions) -> Result<Self, Self::Error> {
        let mut opts_out = TursoConnectOptions::from_url(&opts.database_url)?;
        opts_out.log_settings = opts.log_settings.clone();

        Ok(opts_out)
    }
}

// Infallible alternative to AnyArguments::convert_into()
fn map_arguments(args: AnyArguments) -> TursoArguments {
    let values = args
        .values
        .0
        .into_iter()
        .map(|val| match val {
            AnyValueKind::Null(_) => TursoArgumentValue::Null,
            AnyValueKind::Bool(b) => TursoArgumentValue::Int(b as i32),
            AnyValueKind::SmallInt(i) => TursoArgumentValue::Int(i as i32),
            AnyValueKind::Integer(i) => TursoArgumentValue::Int(i),
            AnyValueKind::BigInt(i) => TursoArgumentValue::Int64(i),
            AnyValueKind::Real(r) => TursoArgumentValue::Double(r as f64),
            AnyValueKind::Double(d) => TursoArgumentValue::Double(d),
            AnyValueKind::Text(t) => TursoArgumentValue::Text(Arc::new(t.to_string())),
            AnyValueKind::Blob(b) => TursoArgumentValue::Blob(Arc::new(b.to_vec())),
            // AnyValueKind is `#[non_exhaustive]` but we should have covered everything
            _ => unreachable!("BUG: missing mapping for {val:?}"),
        })
        .collect();

    TursoArguments {
        values: TursoArgumentsBuffer::new(values),
    }
}

