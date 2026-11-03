use crate::arguments::TursoArgumentsBuffer;
use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::type_info::DataType;
use crate::types::Type;
use crate::{Turso, TursoArgumentValue, TursoTypeInfo, TursoValueRef};
use std::sync::Arc;
use uuid::{
    Uuid,
    fmt::{Hyphenated, Simple},
};

impl Type<Turso> for Uuid {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Blob)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        matches!(ty.0, DataType::Blob | DataType::Text)
    }
}

impl Encode<'_, Turso> for Uuid {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Blob(Arc::new(self.as_bytes().to_vec())));

        Ok(IsNull::No)
    }
}

impl Decode<'_, Turso> for Uuid {
    fn decode(value: TursoValueRef<'_>) -> Result<Self, BoxDynError> {
        // construct a Uuid from the returned bytes
        Uuid::from_slice(value.blob_borrowed()).map_err(Into::into)
    }
}

impl Type<Turso> for Hyphenated {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Text)
    }
}

impl Encode<'_, Turso> for Hyphenated {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Text(Arc::new(self.to_string())));

        Ok(IsNull::No)
    }
}

impl Decode<'_, Turso> for Hyphenated {
    fn decode(value: TursoValueRef<'_>) -> Result<Self, BoxDynError> {
        let uuid: Result<Uuid, BoxDynError> =
            Uuid::parse_str(&value.text_borrowed().map(ToOwned::to_owned)?).map_err(Into::into);

        Ok(uuid?.hyphenated())
    }
}

impl Type<Turso> for Simple {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Text)
    }
}

impl Encode<'_, Turso> for Simple {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Text(Arc::new(self.to_string())));

        Ok(IsNull::No)
    }
}

impl Decode<'_, Turso> for Simple {
    fn decode(value: TursoValueRef<'_>) -> Result<Self, BoxDynError> {
        let uuid: Result<Uuid, BoxDynError> =
            Uuid::parse_str(&value.text_borrowed().map(ToOwned::to_owned)?).map_err(Into::into);

        Ok(uuid?.simple())
    }
}
