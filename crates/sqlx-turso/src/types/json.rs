use serde::{Deserialize, Serialize};

use crate::arguments::TursoArgumentsBuffer;
use crate::decode::Decode;
use crate::encode::{Encode, IsNull};
use crate::error::BoxDynError;
use crate::types::{Json, Type};
use crate::{Turso, TursoTypeInfo, TursoValueRef, type_info::DataType};

impl<T> Type<Turso> for Json<T> {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Text)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <&str as Type<Turso>>::compatible(ty)
    }
}

impl<T> Encode<'_, Turso> for Json<T>
where
    T: Serialize,
{
    fn encode_by_ref(&self, buf: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        Encode::<Turso>::encode(self.encode_to_string()?, buf)
    }
}

impl<'r, T> Decode<'r, Turso> for Json<T>
where
    T: 'r + Deserialize<'r>,
{
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        // Saves a pass over the data by making `serde_json` check UTF-8.
        Self::decode_from_bytes(Decode::<Turso>::decode(value)?)
    }
}
