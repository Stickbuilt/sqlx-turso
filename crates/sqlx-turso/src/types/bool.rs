use crate::arguments::TursoArgumentsBuffer;
use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::type_info::DataType;
use crate::types::Type;
use crate::{Turso, TursoArgumentValue, TursoTypeInfo, TursoValueRef};

impl Type<Turso> for bool {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Bool)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        matches!(ty.0, DataType::Bool | DataType::Int4 | DataType::Integer)
    }
}

impl Encode<'_, Turso> for bool {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Int((*self).into()));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for bool {
    fn decode(value: TursoValueRef<'r>) -> Result<bool, BoxDynError> {
        Ok(value.int64()? != 0)
    }
}
