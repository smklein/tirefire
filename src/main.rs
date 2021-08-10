#[macro_use]
extern crate diesel;

use diesel::prelude::*;
use uuid::Uuid;

table! {
    objects (id) {
        id -> Uuid,
        name -> Text,
        data -> Text,
    }
}

#[derive(Queryable)]
pub struct Identity {
    pub id: Uuid,
    pub name: String,
}

#[derive(Queryable)]
pub struct Object {
    pub identity: Identity,
    pub data: String,
}

fn main() {
    use objects::dsl;

    let connection = PgConnection::establish("pretend-i'm-a-URL").unwrap();
    let object = dsl::objects
        .filter(dsl::name.eq("my-object"))
        .limit(1)
        .load::<Object>(&connection)
        .unwrap();

}
