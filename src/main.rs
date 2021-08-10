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
#[derive(Queryable)]
pub struct Object {
    pub identity: Identity,
    pub data: i32,
}

fn main() {
    use objects::dsl;
    // This connection is not real, but we only care about compiling for now.
    let connection = PgConnection::establish("pretend-i'm-a-URL").unwrap();

    // This invocation works: we explicitly separate identity from
    // non-identity columns, so the right Queryable method can be dispatched.
    let _ = dsl::objects
        .select(((IDENTITY_COLUMNS), dsl::data))
        .first::<Object>(&connection)
        .unwrap();

    // This invocation fails with the following error:
    //
    // "the trait Queryable<(uid, Text, Integer), _> is not implemented for (Identity, i32)"
    //
    // Basically, diesel attempts to map the table fields directly
    // to the Object structure, and sees no auto-derived mapping.

    // let _ = dsl::objects
    //     .first::<Object>(&connection)
    //     .unwrap();
}
