// Ported verbatim from sqlx-sqlite, which builds these formats with the `time`
// components deprecated in later 0.3 releases.
#![allow(deprecated)]

use crate::arguments::TursoArgumentsBuffer;
use crate::value::ValueRef;
use crate::{
    Turso, TursoTypeInfo, TursoValueRef,
    decode::Decode,
    encode::{Encode, IsNull},
    error::BoxDynError,
    type_info::DataType,
    types::Type,
};
use time::format_description::{BorrowedFormatItem, well_known::Rfc3339};
use time::macros::format_description as fd;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

impl Type<Turso> for OffsetDateTime {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Datetime)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        <PrimitiveDateTime as Type<Turso>>::compatible(ty)
    }
}

impl Type<Turso> for PrimitiveDateTime {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Datetime)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        matches!(
            ty.0,
            DataType::Datetime | DataType::Text | DataType::Integer | DataType::Int4
        )
    }
}

impl Type<Turso> for Date {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Date)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        matches!(ty.0, DataType::Date | DataType::Text)
    }
}

impl Type<Turso> for Time {
    fn type_info() -> TursoTypeInfo {
        TursoTypeInfo(DataType::Time)
    }

    fn compatible(ty: &TursoTypeInfo) -> bool {
        matches!(ty.0, DataType::Time | DataType::Text)
    }
}

impl Encode<'_, Turso> for OffsetDateTime {
    fn encode_by_ref(&self, buf: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        Encode::<Turso>::encode(self.format(&Rfc3339)?, buf)
    }
}

impl Encode<'_, Turso> for PrimitiveDateTime {
    fn encode_by_ref(&self, buf: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        let format = fd!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");
        Encode::<Turso>::encode(self.format(&format)?, buf)
    }
}

impl Encode<'_, Turso> for Date {
    fn encode_by_ref(&self, buf: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        let format = fd!("[year]-[month]-[day]");
        Encode::<Turso>::encode(self.format(&format)?, buf)
    }
}

impl Encode<'_, Turso> for Time {
    fn encode_by_ref(&self, buf: &mut TursoArgumentsBuffer) -> Result<IsNull, BoxDynError> {
        let format = fd!("[hour]:[minute]:[second].[subsecond]");
        Encode::<Turso>::encode(self.format(&format)?, buf)
    }
}

impl<'r> Decode<'r, Turso> for OffsetDateTime {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        decode_offset_datetime(value)
    }
}

impl<'r> Decode<'r, Turso> for PrimitiveDateTime {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        decode_datetime(value)
    }
}

impl<'r> Decode<'r, Turso> for Date {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Date::parse(
            value.text_borrowed()?,
            &fd!("[year]-[month]-[day]"),
        )?)
    }
}

impl<'r> Decode<'r, Turso> for Time {
    fn decode(value: TursoValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = value.text_borrowed()?;

        let sqlite_time_formats = &[
            fd!("[hour]:[minute]:[second].[subsecond]"),
            fd!("[hour]:[minute]:[second]"),
            fd!("[hour]:[minute]"),
        ];

        for format in sqlite_time_formats {
            if let Ok(dt) = Time::parse(value, &format) {
                return Ok(dt);
            }
        }

        Err(format!("invalid time: {value}").into())
    }
}

fn decode_offset_datetime(value: TursoValueRef<'_>) -> Result<OffsetDateTime, BoxDynError> {
    let dt = match value.type_info().0 {
        DataType::Text => decode_offset_datetime_from_text(value.text_borrowed()?),
        DataType::Int4 | DataType::Integer => {
            Some(OffsetDateTime::from_unix_timestamp(value.int64()?)?)
        }

        _ => None,
    };

    if let Some(dt) = dt {
        Ok(dt)
    } else {
        Err(format!("invalid offset datetime: {}", value.text_borrowed()?).into())
    }
}

fn decode_offset_datetime_from_text(value: &str) -> Option<OffsetDateTime> {
    if let Ok(dt) = OffsetDateTime::parse(value, &Rfc3339) {
        return Some(dt);
    }

    if let Ok(dt) = OffsetDateTime::parse(value, formats::OFFSET_DATE_TIME) {
        return Some(dt);
    }

    if let Some(dt) = decode_datetime_from_text(value) {
        return Some(dt.assume_utc());
    }

    None
}

fn decode_datetime(value: TursoValueRef<'_>) -> Result<PrimitiveDateTime, BoxDynError> {
    let dt = match value.type_info().0 {
        DataType::Text => decode_datetime_from_text(value.text_borrowed()?),
        DataType::Int4 | DataType::Integer => {
            let parsed = OffsetDateTime::from_unix_timestamp(value.int64()?).unwrap();
            Some(PrimitiveDateTime::new(parsed.date(), parsed.time()))
        }

        _ => None,
    };

    if let Some(dt) = dt {
        Ok(dt)
    } else {
        Err(format!("invalid datetime: {}", value.text_borrowed()?).into())
    }
}

fn decode_datetime_from_text(value: &str) -> Option<PrimitiveDateTime> {
    let default_format = fd!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");
    if let Ok(dt) = PrimitiveDateTime::parse(value, &default_format) {
        return Some(dt);
    }

    let formats = [
        BorrowedFormatItem::Compound(formats::PRIMITIVE_DATE_TIME_SPACE_SEPARATED),
        BorrowedFormatItem::Compound(formats::PRIMITIVE_DATE_TIME_T_SEPARATED),
    ];

    if let Ok(dt) = PrimitiveDateTime::parse(value, &BorrowedFormatItem::First(&formats)) {
        return Some(dt);
    }

    None
}

mod formats {
    use time::format_description::BorrowedFormatItem::{Component, Literal, Optional};
    use time::format_description::{BorrowedFormatItem, Component::*, modifier};

    const YEAR: BorrowedFormatItem<'_> = Component(Year({
        let mut value = modifier::Year::default();
        value.padding = modifier::Padding::Zero;
        value.repr = modifier::YearRepr::Full;
        value.iso_week_based = false;
        value.sign_is_mandatory = false;
        value
    }));

    const MONTH: BorrowedFormatItem<'_> = Component(Month({
        let mut value = modifier::Month::default();
        value.padding = modifier::Padding::Zero;
        value.repr = modifier::MonthRepr::Numerical;
        value.case_sensitive = true;
        value
    }));

    const DAY: BorrowedFormatItem<'_> = Component(Day({
        let mut value = modifier::Day::default();
        value.padding = modifier::Padding::Zero;
        value
    }));

    const HOUR: BorrowedFormatItem<'_> = Component(Hour({
        let mut value = modifier::Hour::default();
        value.padding = modifier::Padding::Zero;
        value.is_12_hour_clock = false;
        value
    }));

    const MINUTE: BorrowedFormatItem<'_> = Component(Minute({
        let mut value = modifier::Minute::default();
        value.padding = modifier::Padding::Zero;
        value
    }));

    const SECOND: BorrowedFormatItem<'_> = Component(Second({
        let mut value = modifier::Second::default();
        value.padding = modifier::Padding::Zero;
        value
    }));

    const SUBSECOND: BorrowedFormatItem<'_> = Component(Subsecond({
        let mut value = modifier::Subsecond::default();
        value.digits = modifier::SubsecondDigits::OneOrMore;
        value
    }));

    const OFFSET_HOUR: BorrowedFormatItem<'_> = Component(OffsetHour({
        let mut value = modifier::OffsetHour::default();
        value.sign_is_mandatory = true;
        value.padding = modifier::Padding::Zero;
        value
    }));

    const OFFSET_MINUTE: BorrowedFormatItem<'_> = Component(OffsetMinute({
        let mut value = modifier::OffsetMinute::default();
        value.padding = modifier::Padding::Zero;
        value
    }));

    pub(super) const OFFSET_DATE_TIME: &[BorrowedFormatItem<'_>] = {
        &[
            YEAR,
            Literal(b"-"),
            MONTH,
            Literal(b"-"),
            DAY,
            Optional(&Literal(b" ")),
            Optional(&Literal(b"T")),
            HOUR,
            Literal(b":"),
            MINUTE,
            Optional(&Literal(b":")),
            Optional(&SECOND),
            Optional(&Literal(b".")),
            Optional(&SUBSECOND),
            Optional(&OFFSET_HOUR),
            Optional(&Literal(b":")),
            Optional(&OFFSET_MINUTE),
        ]
    };

    pub(super) const PRIMITIVE_DATE_TIME_SPACE_SEPARATED: &[BorrowedFormatItem<'_>] = {
        &[
            YEAR,
            Literal(b"-"),
            MONTH,
            Literal(b"-"),
            DAY,
            Literal(b" "),
            HOUR,
            Literal(b":"),
            MINUTE,
            Optional(&Literal(b":")),
            Optional(&SECOND),
            Optional(&Literal(b".")),
            Optional(&SUBSECOND),
            Optional(&Literal(b"Z")),
        ]
    };

    pub(super) const PRIMITIVE_DATE_TIME_T_SEPARATED: &[BorrowedFormatItem<'_>] = {
        &[
            YEAR,
            Literal(b"-"),
            MONTH,
            Literal(b"-"),
            DAY,
            Literal(b"T"),
            HOUR,
            Literal(b":"),
            MINUTE,
            Optional(&Literal(b":")),
            Optional(&SECOND),
            Optional(&Literal(b".")),
            Optional(&SUBSECOND),
            Optional(&Literal(b"Z")),
        ]
    };
}
