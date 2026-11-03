# Turso driver for sqlx + SeaORM — Plan

Approved: 2026-11-03. Approach **A**: a third-party sqlx `Database` impl (`sqlx-turso`) built
by porting sqlx-sqlite's pure-Rust layer onto `turso` / `turso::sync` / `turso_serverless`,
plus a `SqlxTurso` backend variant in private forks of SeaORM / sea-schema / sea-orm-migration.

## Decisions
- **Approach A** (sqlx driver + SeaORM variant). Proxy approach rejected (no real txns, no
  streaming, column ordering broken). Native-SeaORM-only (B) rejected: no sqlx driver.
- **Crate name:** `sqlx-turso` (single crate; `backend` module holds the turso/sync/serverless
  engine layer). Types: `Turso`, `TursoConnection`, `TursoConnectOptions`, `TursoPool`, ...
- **SeaORM changes:** private forks wired via `[patch]`; no effort spent on upstream acceptability.
- **Modes:** all required (order free): local file + `:memory:`, Turso Cloud over HTTP
  (`turso_serverless`), local-with-sync (push/pull), concurrent writes (MVCC / `BEGIN CONCURRENT`).
- **URL schemes:** `turso:` and `libsql:` (local: `turso:path`, `turso::memory:`;
  remote: `turso://host`, `libsql://host`), plus `https://` / `http://` for Turso Cloud
  (same as toasty). Sync mode configured via the builder, not the URL. `libsql` URLs default to
  `BEGIN`; others default to `BEGIN CONCURRENT` on remote (toasty behaviour).
- **Cloud tests:** read `TURSO_DATABASE_URL` / `TURSO_AUTH_TOKEN`; skipped when unset.
- **Versions:** `sqlx-core = "=0.9.0"` from crates.io (matches sea-orm 2.0.4); `turso` 0.8.x
  (`default-features = false`, like toasty); `turso_serverless` 0.1.3; sea-schema 0.18.1.
  Local `sqlx/`, `sea-query/`, `toasty/` clones are reference only — not modified.
- **Toolchain:** sqlx MSRV 1.94; local rustc 1.99.
- sea-orm-turso (Mohit-Panaliya) assessed: not used as reference (broken txns, lossy types).

## Layout
```
turso-sqlx-driver/
  Cargo.toml              # workspace; [patch] sea-orm, sea-orm-migration, sea-schema -> local forks
  crates/sqlx-turso/
  sea-orm/                # private fork
  sea-schema/             # private fork, tag 0.18.1
  sqlx/ sea-query/ toasty/  # reference only
```

## Phases

### Phase 0 — Compatibility check
Small test binary against embedded Turso, with and without MVCC: `RETURNING`;
`SAVEPOINT`/`RELEASE`/`ROLLBACK TO`; `pragma_table_info(?)` table-valued fn;
`PRAGMA table_xinfo/index_list/index_info/foreign_key_list`; `PRAGMA foreign_keys`;
`sqlite_master` queries; multi-statement `execute_batch`. Record gaps below.
(Source inspection of turso_core 0.8.3 suggests all exist; unverified at runtime.)

### Phase 1 — `backend` module (adapted from toasty-driver-turso)
- `AnyDatabase {Local, Sync, Serverless}`, `AnyConn`, `AnyRows`, `AnyStatement`.
- One shared database handle per connect-options (`Arc`), so pool connections share it
  (required for `:memory:`).
- Error mapping -> sqlx `DatabaseError` with `ErrorKind` (unique / FK / not-null / check).
- Busy-retry with backoff for serverless `BEGIN`.
- MVCC: `PRAGMA journal_mode='mvcc'` on local connections when concurrent writes enabled.

### Phase 2 — sqlx `Database` impl
- `Turso`, `TursoConnection`, `TursoConnectOptions` (URL `FromStr` + builder with all toasty
  options: file / memory / remote, sync opts, auth token / token fn, concurrent writes, default
  txn mode, experimental flags, busy timeout, foreign keys, statement cache).
- `Executor`, `TursoRow`, `TursoColumn`, `TursoValue`/`TursoValueRef` (owned `turso::Value`),
  `TursoTypeInfo` (copy of sqlx-sqlite `DataType`), `TursoArguments` (`Vec<turso::Value>`),
  `TursoStatement`, `TursoQueryResult`.
- Port from sqlx-sqlite: `types/*`, `options/`, `migrate.rs`, `type_checking.rs`, `any.rs`.
  Type mappings identical to sqlx-sqlite.
- Transaction manager: depth-based; top-level `BEGIN` / `BEGIN CONCURRENT` / `IMMEDIATE` per
  config; nested = savepoints.
- Streaming: true row streaming for local/sync; buffered for serverless.
- Multi-statement query strings: split via `turso_parser` (sqlx-sqlite parity). If impractical,
  STOP and ask the user before choosing an alternative.
- `describe`: from decl types; nullability unknown for now.
- `Migrate` + `MigrateDatabase` (create/exists/drop for local files; clear error for remote).
- `Any` driver registration.

### Phase 3 — Turso-specific API
- Sync handle: `push`, `pull`, `checkpoint`, `stats` — reachable from options and pool.
- Auth token callback (rotation).
- Per-transaction begin-mode override.

### Phase 4 — SeaORM integration (forks)
1. sqlx-turso: `impl IntoArguments<Turso> for sea_query_sqlx::SqlxValues` behind a `sea-query`
   feature; every `sea_query::Value` variant mapped explicitly (no catch-all).
2. sea-orm: `sqlx-turso` feature; variants in `DatabaseConnectionType`, `InnerConnection`,
   `QueryResultRow`, `ExecResultHolder`, `RuntimeErr`; `src/driver/sqlx_turso.rs` (from
   `sqlx_sqlite.rs`); all match arms incl. `TryGetable` / `ColIdx` macros, JSON, streams,
   transactions; reports `DbBackend::Sqlite`; `Database::connect` routes turso/libsql/http(s)
   schemes; `from_sqlx_turso_pool`; sync-handle accessor.
3. sea-schema: `SqlxRow::Turso` + feature; sqlite discovery/probe accepts Turso.
4. sea-orm-migration: enable probes for `sqlx-turso` feature.

### Phase 5 — Tests
- Port `sqlx/tests/sqlite/*` to Turso local.
- Run sea-orm test suite with a Turso `DATABASE_URL`.
- MVCC concurrent-writer test.
- Turso Cloud serverless + sync tests (env-gated).
- Record every Turso incompatibility below.

### Deferred
`query!` macros (own proc-macro crate); `#[sqlx::test]` support. `sqlx-cli` cannot support
third-party drivers.

## Risks
- `sqlx-core` (semver-exempt) and turso 0.8 are pre-stable: upgrade churn.
- Turso SQL compatibility gaps (Phase 0).
- Ported sqlx-sqlite code must be re-synced manually on sqlx upgrades.

## Progress
- [x] Phase 0 — done 2026-11-03 (turso 0.8.3, local engine, MVCC on/off). Spike code (throwaway):
  scratchpad `phase0/`. Everything required works; gaps below.
- [x] Phase 1 — `backend` module (local / sync / serverless engines, error mapping, MVCC pragma).
- [x] Phase 2 — sqlx `Database` impl, ported types/migrate/any/type_checking; smoke tests pass.
  Multi-statement splitting via `turso_parser` worked (no fallback needed).
- [x] Phase 3 — `push`/`pull`/`checkpoint`/`sync_stats` on `TursoConnectOptions`
  (`pool.connect_options().push()`), `auth_token_fn`, per-transaction mode via `begin_with`.
- [x] Phase 4 — sea-schema fork (branch `turso`, 4a480f6); sea-orm fork (branch `turso`,
  3bed3073 backend, 63e118bf tests, 72e358da sea-orm-migration). `sqlx-turso` feature in both;
  `Database::connect` routes turso/libsql/http/https; `get_turso_connection_pool()` for sync;
  `map_sqlx_turso_opts`/`_pool_opts`/`_before_acquire` hooks; bind errors propagate as
  `DbErr` instead of upstream's panics. Tests: sea-orm 10 Turso tests + 259 lib tests,
  migration Turso test + main suite with `DATABASE_URL=turso::memory:` all pass.
- [x] Phase 5 — done: sqlx SQLite suite ported (120 pass / 3 ignored), MVCC conflict test,
  cloud serverless + sync tests pass against Turso Cloud (libsql:// URL, 2026-11-03).
  SeaORM's integration suite (`cargo test --test '*' --features
  tests-features,sqlx-turso,runtime-tokio`, `DATABASE_URL=turso::memory:`) passes: 236 passed /
  1 ignored across 65 test binaries, vs 226 / 1 with sqlx-sqlite (difference: `turso_tests`).
  Harness changes: test macro and SQLite-dialect cfgs include `sqlx-turso`; one assertion
  adjusted for gap 8.

## Implementation decisions (made during build)
- Branches: root repo `sqlx-turso`; sea-orm fork `turso` (based on master, 18 commits past
  2.0.4, published deps only); sea-schema fork `turso` (based on tag 0.18.1).
- sqlx-turso default features: `any`, `migrate`, `json`. `sync` and `serverless` are opt-in
  cargo features (like toasty); the engine is chosen at runtime from the options.
- `offline` is opt-in, like every SQLx driver: it must be enabled whenever `sqlx-core/offline`
  is (e.g. an app using `sqlx` with default features, whose `macros` feature switches it on).
  Forcing it on was tried and reverted: it breaks sqlx-sqlite/mysql/postgres in the same build.
- `rows_affected()` uses the statement's own change count (`Statement::n_change()`), not the
  connection-wide `sqlite3_changes()` that sqlx-sqlite reports (which is stale for SELECT/DDL).
  Resolves gap 2: DDL reports the engine's internal writes; SELECT reports 0.
- Errors: `TursoError::code()` returns SQLite extended result codes (2067, 1555, 787, 1299,
  275, ...) derived from the engine's error class + message, so sqlx `ErrorKind` and SeaORM's
  `sql_err()` behave as with sqlx-sqlite. Resolves gap 3. `TursoError::is_conflict()` for MVCC
  retry loops.
- `Drop`-time rollbacks are queued and executed before the next operation; cancelled
  BEGIN/COMMIT futures are reconciled with the engine's autocommit flag; a cancelled savepoint
  operation marks the connection broken so the pool discards it.
- Shared database handle lives in `TursoConnectOptions` (`Arc`); every builder method detaches
  it, so changed options open a fresh handle.
- Remote connections never send pragmas; `pragma(..)` / explicit `foreign_keys(..)` on a remote
  target is a connect-time error (2026-11-04). Turso Cloud enforces FKs by default
  (`PRAGMA foreign_keys` -> 1, verified 2026-11-03).
- Transactions default to `BEGIN` everywhere (embedded and remote); `concurrent_writes(true)`
  is the single opt-in to `BEGIN CONCURRENT` (88675da). Migrations always use `BEGIN IMMEDIATE`.
- No engine statement reuse: every statement is prepared fresh, because turso's statement cache
  returned stale columns after schema changes and can't be invalidated. Measured cost ~5-6 us per
  query on in-memory embedded (1-2.6 us cached vs 6.4-8.6 us); revisit with a schema-aware cache
  if embedded hot paths need it.
- Column metadata is read after the first step; a row whose width doesn't match its columns is
  a retryable `TursoErrorKind::Schema` error (code 17). Statements while the engine has aborted
  the transaction fail with `TursoErrorKind::Aborted` (code 516). Write conflicts report 517.
- Review fixes 2026-11-04: sqlx-turso c98f4df..88675da; sea-orm 77789385..c6e6abbc; sea-schema
  d3dfaf5, 842c564.
- SeaORM value binding (`sea_query::Value` -> `TursoArguments`) lives in the sea-orm fork, not
  sqlx-turso, because the set of `Value` variants depends on sea-query features that sea-orm
  forwards 1:1.
- sea-schema: `sqlx-turso` feature enables `sea-query/thread-safe` (as sea-query-sqlx does).

## Decisions from the user (2026-11-03)
- Not aiming for upstream merge; optimise for failsafe/correct/clean behaviour for our own use.
- sea-orm + sea-orm-migration expose `sqlx-turso-serverless` and `sqlx-turso-sync` (e8d61106).
- `sqlcipher_key` with a Turso URL is a connect-time error (never silently unencrypted).
- `TursoConnection` state is boxed in sqlx-turso (pointer-sized; ad101ab).
- Testing: user will provide TURSO_DATABASE_URL / TURSO_AUTH_TOKEN and free disk; then run
  cloud tests and the full SeaORM integration suite against Turso.
- sea-orm-sync and sea-orm-cli: no Turso support (out of scope unless requested).
- Decimal/BigDecimal (2026-11-04): unchanged, identical to stock SeaORM on SQLite (stored as
  REAL, approximate). For exact money values use integer minor units (e.g. cents).
- Read-only / isolation-level requests: keep upstream's warning (not enforced).
- Default SeaORM pool size for Turso: sqlx default (10), not 1 (shared handle; toasty doesn't
  cap Turso pools either).
- Remote (Turso Cloud): per-connection pragmas and explicit foreign_keys are rejected at
  connect time; HTTP streams expire when idle and lose session state (PROTOCOL.md 4.2/4.5).

## Turso incompatibilities found
Phase 0 verified working (MVCC on and off): INSERT/UPDATE/UPSERT `RETURNING`; `last_insert_rowid`;
`SAVEPOINT`/`ROLLBACK TO`/`RELEASE` inside `BEGIN` and `BEGIN CONCURRENT` and standalone;
`pragma_table_info(?)` TVF; `PRAGMA table_xinfo/index_list/index_info/foreign_key_list` (SQLite
column layout); `PRAGMA foreign_keys` read/write (FKs enforced); multi-statement `execute_batch`;
`decl_type` (None for expressions); ALTER ADD COLUMN / RENAME; triggers; `sqlite_version()` = 3.50.4
(passes SeaORM's >= 3.35 RETURNING check). Constraint errors use SQLite message text
(`UNIQUE constraint failed: parent.name`, `FOREIGN KEY constraint failed`,
`CHECK constraint failed: v >= 0`, `NOT NULL constraint failed: parent.name`).

1. **Internal tables visible in `sqlite_master` as `type='table'`:**
   `__turso_internal_seq___turso_internal_autoincrement_<table>` (per AUTOINCREMENT table) and
   `__turso_internal_mvcc_meta` (MVCC). sea-schema `query_tables` (`name <> 'sqlite_sequence'`)
   would list them; sea-orm-migration `fresh`/`reset` would try to DROP them (toasty notes they
   cannot be dropped). Fix in forks: also exclude `name NOT LIKE '\_\_turso\_%' ESCAPE '\'`
   (toasty's filter) in sea-schema probes/discovery and migration drop_everything.
   **Done** in sea-schema: filter narrowed to `__turso\_internal\_%` (842c564) so user tables
   such as `__turso_cache` stay visible; applied in table listing and index discovery.
2. **`changes` after DDL is non-zero:** DROP TABLE -> 2; under MVCC ALTER ADD COLUMN -> 1,
   RENAME -> 6, CREATE TRIGGER -> 1 (internal schema writes counted). Affects
   `rows_affected()` for DDL only. Decide in Phase 2 whether to normalise (compare with
   sqlx-sqlite semantics).
3. **Error classification:** need to check whether `turso::Error` exposes extended codes or
   only message text (toasty's `error.rs` is the reference) — Phase 1.
4. **`VACUUM` is gated behind an experimental flag:** `VACUUM` fails with
   `Parse error: VACUUM is an experimental feature. Enable with --experimental-vacuum flag`
   (code 1) unless the database is opened with `experimental_vacuum(true)`; SQLite always
   accepts it. With the flag the statement succeeds. Ported test `migrate::no_tx` (a
   `-- no-transaction` migration containing `VACUUM;`) is ignored.
5. **Journal mode is fixed to WAL:** on a file database `PRAGMA journal_mode = DELETE` is
   accepted but returns `wal` and the mode stays `wal` (`PRAGMA journal_mode` -> `wal`); SQLite
   switches to `delete`. Ported test `migrate::no_tx_reversible` (up: `JOURNAL_MODE = WAL`,
   down: `JOURNAL_MODE = DELETE`, asserts `delete` after undo) is ignored.
6. **Driver parity notes (not engine gaps):** `turso:` with an empty path opens an in-memory
   database (sqlx-sqlite's `sqlite:` opens a temporary on-disk file); `[u8; N]` encode/decode
   is absent, as in published sqlx-sqlite 0.9.0; `#[derive(sqlx::Type)]` on enums only emits
   impls for the built-in drivers, so third-party drivers need a hand-written `Type<Turso>`.
   Ported suite status: 120 pass, 3 ignored (gaps 4 and 5, plus `issue_1467` ignored upstream).
7. **Turso Cloud prefixes error messages with `SQLite error: `** (e.g.
   `SQLite error: UNIQUE constraint failed: t.x`); embedded errors have no prefix. Handled in
   `TursoError::sqlite_code()` so `ErrorKind` / codes are the same on both transports.
8. **No extended result code for primary-key violations:** after
   `CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES (1);`, a second
   `INSERT INTO t VALUES (1);` fails in SQLite with `UNIQUE constraint failed: t.id` and extended
   code 1555 (`SQLITE_CONSTRAINT_PRIMARYKEY`; same for `TEXT PRIMARY KEY`); a `UNIQUE` column
   gives the same message with 2067. Turso (turso_core 0.8.3 `vdbe/execute.rs` halt) formats
   both as `UNIQUE constraint failed: ...` and the API exposes only the primary code 19
   (`turso::Error::Constraint(String)`), so sqlx-turso reports 2067 for both. `ErrorKind` is
   `UniqueViolation` either way and SeaORM's `sql_err()` matches `"1555" | "2067"`, so only code
   that compares the raw code to 1555 sees a difference. SeaORM `tests/crud/error.rs` expects
   2067 under `sqlx-turso`. (Recovering 1555 would need a schema lookup in the error path; not
   done.)
9. **Identifiers are stored lowercased:** `CREATE TABLE "CakeOrder"` appears as `cakeorder` in
   `sqlite_master` (also index names, `tbl_name`, FK target tables, the referenced `to` column in
   `PRAGMA foreign_key_list`, and columns of automatic UNIQUE indexes in `PRAGMA index_info`).
   Ordinary column names keep their case. SQLite keeps the original case. Handled: sea-schema and
   sea-orm schema sync compare SQLite identifiers case-insensitively.
10. **`BEGIN CONCURRENT` needs MVCC:** embedded without `journal_mode = 'mvcc'` fails with
    "Concurrent transaction mode is only supported when MVCC is enabled"; Turso Cloud databases
    without concurrent writes enabled reject it as a syntax error. Hence BEGIN is the default.
11. **Turso Cloud can return stale column metadata right after a schema change** (rows have the
    new width, columns the old list; ~1 in 6 runs under parallel DDL). The driver rejects such
    results with the retryable `Schema` error.
