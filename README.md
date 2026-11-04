# sqlx-turso

A [Turso](https://turso.tech) driver for [SQLx](https://github.com/launchbadge/sqlx), with
a matching backend for [SeaORM](https://github.com/SeaQL/sea-orm).

One driver covers every way of using Turso:

| Mode | How to select it | Cargo feature |
|---|---|---|
| Embedded file | `turso:data.db` or `TursoConnectOptions::filename` | default |
| In-memory | `turso::memory:` or `TursoConnectOptions::in_memory` | default |
| Turso Cloud over HTTP | `libsql://…`, `turso://…` or `https://…` URLs | `serverless` |
| Embedded replica synced with Turso Cloud (push / pull) | `TursoConnectOptions::sync_url` | `sync` |
| Concurrent writes (MVCC, `BEGIN CONCURRENT`) | `TursoConnectOptions::concurrent_writes(true)` | default |

It is built on the `turso` crate (the Rust rewrite of SQLite) and structured as a port of
`sqlx-sqlite`, so types, migrations and the `Any` driver behave like SQLx's SQLite driver.

## Status

Built for the author's own projects and tested against SQLx's SQLite test suite, SeaORM's full
integration suite and live Turso Cloud databases. It is not affiliated with Turso, SQLx or
SeaQL. `TURSO_DRIVER_PLAN.md` records design decisions and every known difference from SQLite.

Versions are pinned exactly: `sqlx-core` 0.9.0, `turso` 0.8.3, `turso_serverless` 0.1.3.

## SQLx

```toml
[dependencies]
sqlx = { version = "=0.9.0", default-features = false, features = ["runtime-tokio"] }
sqlx-turso = { git = "https://github.com/Stickbuilt/sqlx-turso", features = ["serverless", "sync"] }
```

```rust
use sqlx_turso::{TursoConnectOptions, TursoPoolOptions};

// Embedded file
let pool = TursoPoolOptions::new()
    .connect_with(TursoConnectOptions::new().filename("app.db").create_if_missing(true))
    .await?;

// Turso Cloud
let opts = TursoConnectOptions::new()
    .remote(std::env::var("TURSO_DATABASE_URL")?)
    .auth_token(std::env::var("TURSO_AUTH_TOKEN")?);
let pool = TursoPoolOptions::new().connect_with(opts).await?;

// Embedded replica: reads and writes are local; push and pull sync with Turso Cloud.
let opts = TursoConnectOptions::new()
    .filename("replica.db")
    .create_if_missing(true)
    .sync_url(std::env::var("TURSO_DATABASE_URL")?)
    .auth_token(std::env::var("TURSO_AUTH_TOKEN")?);
let pool = TursoPoolOptions::new().connect_with(opts).await?;
pool.connect_options().pull().await?;
pool.connect_options().push().await?;
```

If your build also enables `sqlx-core/offline` (for example through `sqlx`'s `macros`
feature), enable this crate's `offline` feature too.

## SeaORM

The SeaORM backend lives in forks of `sea-orm` and `sea-schema` on their `turso` branches.
Point the published crates at them with a patch:

```toml
[dependencies]
sea-orm = { version = "2.0", features = ["sqlx-turso", "sqlx-turso-serverless", "sqlx-turso-sync", "runtime-tokio"] }
sea-orm-migration = { version = "2.0", features = ["sqlx-turso", "runtime-tokio"] }

[patch.crates-io]
sea-orm = { git = "https://github.com/Stickbuilt/sea-orm", branch = "turso" }
sea-orm-migration = { git = "https://github.com/Stickbuilt/sea-orm", branch = "turso" }
sea-schema = { git = "https://github.com/Stickbuilt/sea-schema", branch = "turso" }
```

```rust
let db = sea_orm::Database::connect("turso:app.db?mode=rwc").await?;
let db = sea_orm::Database::connect(format!("{url}?authToken={token}")).await?; // Turso Cloud
```

The connection reports `DbBackend::Sqlite`, so queries and migrations use SeaORM's SQLite
dialect. Use `ConnectOptions::map_sqlx_turso_opts` for builder-only settings (sync, concurrent
writes, encryption) and `DatabaseConnection::get_turso_connection_pool()` to reach
`push`/`pull`.

## Differences from SQLite worth knowing

- Turso stores table and index names lowercased.
- `BEGIN CONCURRENT` needs concurrent writes enabled (embedded) or enabled on the Turso Cloud
  database. Transactions default to plain `BEGIN`.
- Turso Cloud connections reject per-connection `PRAGMA`s, because the server's session expires
  when idle and the setting would silently revert.
- Decimals are stored as REAL, as in SQLite, so they are not exact.

The full list is in `TURSO_DRIVER_PLAN.md`.

## Tests

```sh
cargo test -p sqlx-turso --all-features
```

Cloud tests run when `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are set (for example in an
untracked `.env`), and are skipped otherwise.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT), at your option. Large parts are ported from SQLx's `sqlx-sqlite`
(© SQLx Contributors, LaunchBadge, LLC), under the same licenses.
