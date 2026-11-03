//! Port of sqlx's SQLite integration tests (`sqlx/tests/sqlite/`) to the Turso driver.
//!
//! Each module mirrors the upstream file of the same name. Tests that need SQLite C APIs
//! (handles, hooks, collations, extensions, serialization) are not ported; tests that fail
//! because of a Turso engine difference are `#[ignore]`d with the reason, and recorded in
//! `TURSO_DRIVER_PLAN.md`.

#[macro_use]
mod common;

#[cfg(feature = "any")]
mod any;
mod derives;
mod error;
#[cfg(feature = "migrate")]
mod migrate;
mod sqlite;
mod types;
