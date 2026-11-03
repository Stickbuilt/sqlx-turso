use crate::arguments::TursoArgumentsBuffer;
use crate::{Turso, TursoTypeInfo, TursoValueRef};
use sqlx_core::decode::Decode;
use sqlx_core::encode::{Encode, IsNull};
use sqlx_core::error::BoxDynError;
use sqlx_core::types::{Text, Type};
use std::fmt::Display;
use std::str::FromStr;

impl<T> Type<Turso> for Text<T> {
    fn type_info() -> TursoTypeInfo {
        <String as Type<Turso>>::type_info()
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <String as Type<Turso>>::compatible(ty)
    }
}

impl<T> Encode<'_, Turso> for Text<T>
where
    T: Display,
{
    fn encode_by_ref(&self, buf: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        Encode::<Turso>::encode(self.0.to_string(), buf)
    }
}

impl<'r, T> Decode<'r, Turso> for Text<T>
where
    T: FromStr,
    BoxDynError: From<<T as FromStr>::Err>,
{
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Self(value.with_temp_text(|text| text.parse::<T>())??))
    }
}
