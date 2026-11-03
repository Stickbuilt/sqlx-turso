use crate::arguments::TursoArgumentsBuffer;
use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::type_info::DataType;
use crate::types::Type;
use crate::{Turso, TursoArgumentValue, TursoTypeInfo, TursoValueRef};

impl Type<Turso> for u8 {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Int4)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        matches!(ty.0, DataType::Int4 | DataType::Integer)
    }
}

impl Encode<'_, Turso> for u8 {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Int(*self as i32));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for u8 {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        // NOTE: using `sqlite3_value_int64()` here because `sqlite3_value_int()` silently truncates
        // which leads to bugs, e.g.:
        // https://github.com/launchbadge/sqlx/issues/3179
        // Similar bug in Postgres: https://github.com/launchbadge/sqlx/issues/3161
        Ok(value.int64()?.try_into()?)
    }
}

impl Type<Turso> for u16 {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Int4)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        matches!(ty.0, DataType::Int4 | DataType::Integer)
    }
}

impl Encode<'_, Turso> for u16 {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Int(*self as i32));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for u16 {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.int64()?.try_into()?)
    }
}

impl Type<Turso> for u32 {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Integer)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        matches!(ty.0, DataType::Int4 | DataType::Integer)
    }
}

impl Encode<'_, Turso> for u32 {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Int64(*self as i64));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for u32 {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.int64()?.try_into()?)
    }
}

impl Type<Turso> for u64 {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Integer)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        matches!(ty.0, DataType::Int4 | DataType::Integer)
    }
}

impl<'r> Decode<'r, Turso> for u64 {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.int64()?.try_into()?)
    }
}
