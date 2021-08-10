#[macro_use]
extern crate diesel;

use diesel::prelude::*;
use uuid::Uuid;

// So here's my diesel schema.
//
// It's essentially a concatentation of
// "identity" with "object-specific" fields.
table! {
    objects (id) {
        // Identity-specific
        id -> Uuid,
        name -> Text,

        // Object-specific
        data -> Text,
    }
}

// This is common metadata - think "time updated",
// "description", etc, all that sorta fun stuff.
//
// It would effectively be a prefix that's shared
// by multiple objects. We can see this in the
// "Object" example below.
#[derive(Queryable)]
pub struct Identity {
    pub id: Uuid,
    pub name: String,
}

// An example object which uses identity.
//
// In my non-sample codebase, I have many of these objects,
// all which embed Identity as the first field.
#[derive(Queryable)]
pub struct Object {
    pub identity: Identity,
    pub data: String,
}

fn main() {
    use objects::dsl;
    // This connection is not real, but we only care about
    // compiling for now.
    let connection = PgConnection::establish("pretend-i'm-a-URL").unwrap();

    let object = dsl::objects
        .filter(dsl::name.eq("my-object"))
        .limit(1)
        .load::<Object>(&connection)
        .unwrap();

}
