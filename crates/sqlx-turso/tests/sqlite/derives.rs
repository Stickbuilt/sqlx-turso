//! Port of `sqlx/tests/sqlite/derives.rs`.

use sqlx_turso::Turso;

#[derive(Debug, PartialEq, sqlx::Type)]
#[repr(u32)]
enum Origin {
    Foo = 1,
    Bar = 2,
}

test_type!(origin_enum<Origin>(Turso,
    "1" == Origin::Foo,
    "2" == Origin::Bar,
));

#[derive(PartialEq, Eq, Debug, sqlx::Type)]
#[sqlx(transparent)]
struct TransparentTuple(i64);

#[derive(PartialEq, Eq, Debug, sqlx::Type)]
#[sqlx(transparent)]
struct TransparentNamed {
    field: i64,
}

test_type!(transparent_tuple<TransparentTuple>(Turso,
    "0" == TransparentTuple(0),
    "23523" == TransparentTuple(23523)
));

test_type!(transparent_named<TransparentNamed>(Turso,
    "0" == TransparentNamed { field: 0 },
    "23523" == TransparentNamed { field: 23523 },
));
