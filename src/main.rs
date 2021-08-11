#[macro_use]
extern crate diesel;

use diesel::prelude::*;
use uuid::Uuid;

// So here's my diesel schema.
//
// It's a concatentation of "identity" with "object-specific" fields.
table! {
    objects (id) {
        // Identity-specific
        id -> Uuid,
        name -> Text,

        // Object-specific
        data -> Integer,
    }
}

// This is common metadata - think "time updated", "description", etc.
//
// It would effectively be a prefix that's shared by multiple objects. We can
// see this in the "Object" example below.
#[derive(Queryable)]
pub struct Identity {
    pub id: Uuid,
    pub name: String,
}

const IDENTITY_COLUMNS: (objects::dsl::id, objects::dsl::name) = (objects::dsl::id, objects::dsl::name);

// An example object which uses identity.
//
// In my non-sample codebase, I have many of these objects, all which embed
// Identity as the first field.
pub struct Object {
    pub identity: Identity,
    pub data: i32,
}

type FlatIdentityType = (Uuid, String);
type SerializedIdentityType = (diesel::sql_types::Uuid, diesel::sql_types::Text);
// NOTE: Should be Identity concat non-identity.
type FlatObjectType = (Uuid, String, i32);
type SerializedObjectType = (diesel::sql_types::Uuid, diesel::sql_types::Text, diesel::sql_types::Integer);

use diesel::pg::Pg;
use diesel::deserialize::FromSqlRow;

// TODO: *could* be more generic over DB
// TODO: *could* be more generic over Query (ST vs FlatObjectType).
impl Queryable<SerializedObjectType, Pg> for Object
where
//    FlatIdentityType: diesel::deserialize::FromSqlRow<FlatIdentityType, Pg>,
    FlatObjectType: FromSqlRow<SerializedObjectType, Pg>,

//    Identity: Queryable<FlatObjectType, Pg>,
    Identity: Queryable<SerializedIdentityType, Pg>,
//    Uuid: FromSqlRow<diesel::sql_types::Uuid, Pg>,


//    Identity: Queryable<FlatIdentityType, Pg>,
//    FlatIdentityType: diesel::deserialize::Queryable<FlatIdentityType, Pg>,


//    i32: Queryable<FlatObjectType, Pg>,
//    i32: Queryable<diesel::sql_types::Integer, Pg>,
//    i32: diesel::deserialize::FromSqlRow<diesel::sql_types::Integer, Pg>,

{
    type Row = FlatObjectType;

    fn build(row: Self::Row) -> Self {
        let identity_row: FlatIdentityType = (row.0, row.1);
        Self {
            identity: Queryable::<SerializedIdentityType, Pg>::build(identity_row),
            data: Queryable::<_, Pg>::build(row.2),
        }
    }
}

fn main() {
    use objects::dsl;
    // This connection is not real, but we only care about compiling for now.
    let connection = PgConnection::establish("pretend-i'm-a-URL").unwrap();

    // This invocation works: we explicitly separate identity from
    // non-identity columns, so the right Queryable method can be dispatched.
    // let _ = dsl::objects
    //     .select(((IDENTITY_COLUMNS), dsl::data))
    //     .first::<Object>(&connection)
    //     .unwrap();

    // This invocation fails with the following error:
    //
    // "the trait Queryable<(uid, Text, Integer), _> is not implemented for (Identity, i32)"
    //
    // Basically, diesel attempts to map the table fields directly
    // to the Object structure, and sees no auto-derived mapping.
    let _ = dsl::objects
        .first::<Object>(&connection)
        .unwrap();
}
