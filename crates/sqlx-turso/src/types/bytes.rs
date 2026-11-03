use std::borrow::Cow;
use std::rc::Rc;
use std::sync::Arc;

use crate::arguments::TursoArgumentsBuffer;
use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::type_info::DataType;
use crate::types::Type;
use crate::{Turso, TursoArgumentValue, TursoTypeInfo, TursoValueRef};

impl Type<Turso> for [u8] {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Blob)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        matches!(ty.0, DataType::Blob | DataType::Text)
    }
}

impl Encode<'_, Turso> for &'_ [u8] {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Blob(Arc::new(self.to_vec())));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for &'r [u8] {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.blob_borrowed())
    }
}

impl Encode<'_, Turso> for Box<[u8]> {
    fn encode(self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Blob(Arc::new(self.into_vec())));

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Blob(Arc::new(self.clone().into_vec())));

        Ok(IsNull::No)
    }
}

impl Type<Turso> for Vec<u8> {
    fn type_info() -> TursoTypeInfo {
        <&[u8] as Type<Turso>>::type_info()
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <&[u8] as Type<Turso>>::compatible(ty)
    }
}

impl Encode<'_, Turso> for Vec<u8> {
    fn encode(self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Blob(Arc::new(self)));

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Blob(Arc::new(self.clone())));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for Vec<u8> {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.blob_owned())
    }
}

impl Encode<'_, Turso> for Cow<'_, [u8]> {
    fn encode(self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Blob(Arc::new(self.into())));

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Blob(Arc::new(self.to_vec())));

        Ok(IsNull::No)
    }
}

impl Encode<'_, Turso> for Arc<[u8]> {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        <Vec<u8> as Encode<'_, Turso>>::encode(self.to_vec(), args)
    }
}

impl Encode<'_, Turso> for Rc<[u8]> {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        <Vec<u8> as Encode<'_, Turso>>::encode(self.to_vec(), args)
    }
}
