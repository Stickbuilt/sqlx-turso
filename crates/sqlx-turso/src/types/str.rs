use crate::arguments::TursoArgumentsBuffer;
use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::type_info::DataType;
use crate::types::Type;
use crate::{Turso, TursoArgumentValue, TursoTypeInfo, TursoValueRef};
use sqlx_core::database::Database;
use std::borrow::Cow;
use std::rc::Rc;
use std::sync::Arc;

impl Type<Turso> for str {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Text)
    }
}

impl Encode<'_, Turso> for &'_ str {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Text(Arc::new(self.to_string())));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for &'r str {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.text_borrowed()?)
    }
}

impl Encode<'_, Turso> for Box<str> {
    fn encode(self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::TextSlice(Arc::from(self)));

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Text(Arc::new(self.to_string())));

        Ok(IsNull::No)
    }
}

impl Type<Turso> for String {
    fn type_info() -> TursoTypeInfo {
        <&str as Type<Turso>>::type_info()
    }
}

impl Encode<'_, Turso> for String {
    fn encode(self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Text(Arc::new(self)));

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Text(Arc::new(self.clone())));

        Ok(IsNull::No)
    }
}

impl<'r> Decode<'r, Turso> for String {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(value.text_owned()?)
    }
}

impl Encode<'_, Turso> for Cow<'_, str> {
    fn encode(self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Text(Arc::new(self.into())));

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        args.push(TursoArgumentValue::Text(Arc::new(self.to_string())));

        Ok(IsNull::No)
    }
}

impl Encode<'_, Turso> for Arc<str> {
    fn encode(self, args: &mut <Turso as Database>::ArgumentBuffer) -> Result<IsNull, BoxDynError>
    where
        Self: Sized,
    {
        args.push(TursoArgumentValue::TextSlice(self));

        Ok(IsNull::No)
    }

    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        <String as Encode<'_, Turso>>::encode(self.to_string(), args)
    }
}

impl Encode<'_, Turso> for Rc<str> {
    fn encode_by_ref(&self, args: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        <String as Encode<'_, Turso>>::encode(self.to_string(), args)
    }
}
