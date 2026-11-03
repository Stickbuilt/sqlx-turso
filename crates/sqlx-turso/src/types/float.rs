use crate::arguments::TursoArgumentsBuffer;
use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::type_info::DataType;
use crate::types::Type;
use crate::{Turso, TursoArgumentValue, TursoTypeInfo, TursoValueRef};

impl Type<Turso> for f32 {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Float)
    }
}

impl Encode<'_, Turso> for f32 {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Double((*self).into()));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for f32 {
    fn decode(value: TursoValueRef<'r>) -> Result<f32, BoxDynError> {
        // Truncation is intentional
        #[allow(clippy::cast_possible_truncation)]
        Ok(value.double()? as f32)
    }
}

impl Type<Turso> for f64 {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Float)
    }
}

impl Encode<'_, Turso> for f64 {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Double(*self));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for f64 {
    fn decode(value: TursoValueRef<'r>) -> Result<f64, BoxDynError> {
        Ok(value.double()?)
    }
}
