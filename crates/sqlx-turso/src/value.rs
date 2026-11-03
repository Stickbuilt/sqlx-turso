//! Owned result values.
//!
//! sqlx-sqlite wraps `sqlite3_value` handles and relies on SQLite's C API for
//! type coercions (`sqlite3_value_int64()` on a TEXT value, `sqlite3_value_text()`
//! on an INTEGER, ...). Turso hands back owned values, so the same coercions are
//! reproduced here to keep decoding behaviour identical to sqlx-sqlite.

use std::borrow::Cow;
use std::convert::Infallible;
use std::str;
use std::sync::OnceLock;

use sqlx_core::type_info::TypeInfo;
pub(crate) use sqlx_core::value::{Value, ValueRef};
use turso::Value as RawValue;

use crate::type_info::DataType;
use crate::{Turso, TursoTypeInfo};

/// An owned value read from a Turso result row.
#[derive(Debug, Clone)]
pub struct TursoValue {
    raw: RawValue,
    /// Declared type of the column the value came from, if known.
    column_type: Option<TursoTypeInfo>,
    /// Textual rendering of numeric values, created on first borrow so that
    /// `&str` / `&[u8]` decodes of INTEGER and REAL values can borrow from it.
    text: OnceLock<Box<str>>,
}

/// A borrowed value read from a Turso result row.
#[derive(Debug, Clone)]
pub struct TursoValueRef<'r>(&'r TursoValue);

impl TursoValue {
    pub(crate) fn new(raw: RawValue, column_type: Option<TursoTypeInfo>) -> Self {
        Self {
            raw,
            column_type,
            text: OnceLock::new(),
        }
    }

    /// The raw storage-class value.
    pub fn raw(&self) -> &RawValue {
        &self.raw
    }

    /// Consume the value, returning the raw storage-class value.
    pub fn into_raw(self) -> RawValue {
        self.raw
    }

    fn value_type_info(&self) -> TursoTypeInfo {
        TursoTypeInfo(DataType::of_value(&self.raw))
    }

    fn type_info_inner(&self) -> TursoTypeInfo {
        let value_type = self.value_type_info();

        // The actual storage class is more accurate than the declared type, unless NULL.
        match &self.column_type {
            Some(column_type) if value_type.is_null() => column_type.clone(),
            _ => value_type,
        }
    }

    fn int64(&self) -> i64 {
        match &self.raw {
            RawValue::Null => 0,
            RawValue::Integer(i) => *i,
            RawValue::Real(f) => real_to_i64(*f),
            RawValue::Text(s) => text_to_i64(s.as_bytes()),
            RawValue::Blob(b) => text_to_i64(b),
        }
    }

    fn double(&self) -> f64 {
        match &self.raw {
            RawValue::Null => 0.0,
            RawValue::Integer(i) => *i as f64,
            RawValue::Real(f) => *f,
            RawValue::Text(s) => text_to_f64(s.as_bytes()),
            RawValue::Blob(b) => text_to_f64(b),
        }
    }

    fn numeric_text(&self) -> &str {
        self.text.get_or_init(|| match &self.raw {
            RawValue::Integer(i) => i.to_string().into_boxed_str(),
            RawValue::Real(f) => format_real(*f).into_boxed_str(),
            _ => Box::from(""),
        })
    }

    fn bytes(&self) -> &[u8] {
        match &self.raw {
            RawValue::Null => &[],
            RawValue::Text(s) => s.as_bytes(),
            RawValue::Blob(b) => b,
            RawValue::Integer(_) | RawValue::Real(_) => self.numeric_text().as_bytes(),
        }
    }
}

impl Value for TursoValue {
    type Database = Turso;

    fn as_ref(&self) -> TursoValueRef<'_> {
        TursoValueRef(self)
    }

    fn type_info(&self) -> Cow<'_, TursoTypeInfo> {
        Cow::Owned(self.type_info_inner())
    }

    fn is_null(&self) -> bool {
        matches!(self.raw, RawValue::Null)
    }
}

impl<'r> TursoValueRef<'r> {
    pub(crate) fn value(value: &'r TursoValue) -> Self {
        Self(value)
    }

    /// The raw storage-class value.
    pub fn raw(&self) -> &RawValue {
        &self.0.raw
    }

    pub(super) fn int64(&self) -> Result<i64, Infallible> {
        Ok(self.0.int64())
    }

    pub(super) fn double(&self) -> Result<f64, Infallible> {
        Ok(self.0.double())
    }

    pub(super) fn blob_borrowed(&self) -> &'r [u8] {
        self.0.bytes()
    }

    pub(super) fn blob_owned(&self) -> Vec<u8> {
        self.0.bytes().to_vec()
    }

    pub(super) fn text_borrowed(&self) -> Result<&'r str, str::Utf8Error> {
        str::from_utf8(self.blob_borrowed())
    }

    #[allow(dead_code)]
    pub(super) fn with_temp_text<R>(
        &self,
        op: impl FnOnce(&str) -> R,
    ) -> Result<R, str::Utf8Error> {
        str::from_utf8(self.0.bytes()).map(op)
    }

    pub(super) fn text_owned(&self) -> Result<String, str::Utf8Error> {
        self.with_temp_text(str::to_string)
    }
}

impl<'r> ValueRef<'r> for TursoValueRef<'r> {
    type Database = Turso;

    fn to_owned(&self) -> TursoValue {
        self.0.clone()
    }

    fn type_info(&self) -> Cow<'_, TursoTypeInfo> {
        Cow::Owned(self.0.type_info_inner())
    }

    fn is_null(&self) -> bool {
        matches!(self.0.raw, RawValue::Null)
    }
}

/// `sqlite3_value_int64()` on a REAL: truncate toward zero, saturating at the i64 bounds.
fn real_to_i64(f: f64) -> i64 {
    if f.is_nan() {
        0
    } else {
        // `as` saturates for out-of-range floats.
        f as i64
    }
}

/// `sqlite3_value_int64()` on TEXT / BLOB: parse the leading integer prefix,
/// ignoring leading whitespace; 0 if there is none.
fn text_to_i64(bytes: &[u8]) -> i64 {
    let s = trim_start_ascii(bytes);
    let (negative, digits) = match s.first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };

    let mut value: i64 = 0;
    for &b in digits.iter().take_while(|b| b.is_ascii_digit()) {
        let digit = i64::from(b - b'0');
        value = match value.checked_mul(10).and_then(|v| {
            if negative {
                v.checked_sub(digit)
            } else {
                v.checked_add(digit)
            }
        }) {
            Some(v) => v,
            None => return if negative { i64::MIN } else { i64::MAX },
        };
    }
    value
}

/// `sqlite3_value_double()` on TEXT / BLOB: parse the longest leading numeric prefix,
/// ignoring leading whitespace; 0.0 if there is none.
fn text_to_f64(bytes: &[u8]) -> f64 {
    let s = trim_start_ascii(bytes);
    let mut end = 0;
    let mut seen_digit = false;
    let mut seen_dot = false;
    let mut seen_exp = false;

    while end < s.len() {
        let b = s[end];
        match b {
            b'0'..=b'9' => seen_digit = true,
            b'+' | b'-' if end == 0 || matches!(s[end - 1], b'e' | b'E') => {}
            b'.' if !seen_dot && !seen_exp => seen_dot = true,
            b'e' | b'E' if seen_digit && !seen_exp => seen_exp = true,
            _ => break,
        }
        end += 1;
    }

    // Back off an incomplete exponent / sign suffix ("1e", "1e+", "-").
    while end > 0 {
        let prefix = &s[..end];
        if let Ok(v) = str::from_utf8(prefix).unwrap_or("").parse::<f64>() {
            return v;
        }
        end -= 1;
    }
    0.0
}

fn trim_start_ascii(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    &bytes[start..]
}

/// Render a REAL the way SQLite does (`%!.15g`): up to 15 significant digits,
/// always with a decimal point or exponent so it reads back as REAL.
fn format_real(f: f64) -> String {
    if f.is_nan() {
        return String::new();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Inf".into() } else { "-Inf".into() };
    }
    if f == 0.0 {
        return if f.is_sign_negative() {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    }

    let exponent = f.abs().log10().floor() as i32;
    if !(-5..15).contains(&exponent) {
        // Scientific notation with 15 significant digits, trailing zeros trimmed.
        let s = format!("{:.14e}", f);
        let (mantissa, exp) = s.split_once('e').expect("formatted with exponent");
        let mantissa = trim_fraction(mantissa);
        let exp: i32 = exp.parse().expect("valid exponent");
        let mantissa = if mantissa.contains('.') {
            mantissa.to_string()
        } else {
            format!("{mantissa}.0")
        };
        return format!(
            "{mantissa}e{}{:02}",
            if exp < 0 { '-' } else { '+' },
            exp.abs()
        );
    }

    let decimals = (14 - exponent).max(0) as usize;
    let s = format!("{:.*}", decimals, f);
    let s = trim_fraction(&s);
    if s.contains('.') {
        s.to_string()
    } else {
        format!("{s}.0")
    }
}

fn trim_fraction(s: &str) -> &str {
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.')
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_coercions() {
        assert_eq!(text_to_i64(b"  42abc"), 42);
        assert_eq!(text_to_i64(b"-17"), -17);
        assert_eq!(text_to_i64(b"1.9"), 1);
        assert_eq!(text_to_i64(b"abc"), 0);
        assert_eq!(text_to_i64(b"99999999999999999999"), i64::MAX);
        assert_eq!(text_to_f64(b" 1.5e3xyz"), 1500.0);
        assert_eq!(text_to_f64(b"1e"), 1.0);
        assert_eq!(text_to_f64(b"-"), 0.0);
        assert_eq!(text_to_f64(b".5"), 0.5);
    }

    #[test]
    fn real_rendering() {
        assert_eq!(format_real(1.0), "1.0");
        assert_eq!(format_real(0.1), "0.1");
        assert_eq!(format_real(-2.5), "-2.5");
        assert_eq!(format_real(123456789.125), "123456789.125");
        assert_eq!(format_real(1e20), "1.0e+20");
        assert_eq!(format_real(1.5e-7), "1.5e-07");
    }
}
