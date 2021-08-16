#[macro_use]
extern crate diesel;

use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_source::Table;
use uuid::Uuid;

table! {
    use diesel::sql_types::*;

    objects (id) {
        id -> Uuid,
        runtime -> Text,
        gen -> Integer,
    }
}

#[derive(Queryable)]
pub struct Object {
    pub id: Uuid,
    pub runtime: String,
    pub data: i32,
}

/// Wrapper around [`diesel::update`] for a Table, which allows
/// callers to distinguish between "not found", "found but not updated", and
/// "updated.
pub trait ConditionalUpdate<K, U, V>: Sized + Table {
    fn update_if(self, key: K, update_statement: UpdateStatement<Self, U, V>) -> ConditionallyUpdated<Self, K, U, V>;
}

impl<T: Table, K, U, V> ConditionalUpdate<K, U, V> for T {
    fn update_if(self, key: K, update_statement: UpdateStatement<Self, U, V>) -> ConditionallyUpdated<Self, K, U, V> {
        ConditionallyUpdated  {
            table: self,
            update_statement,
            key,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ConditionallyUpdated<T: Table, K, U, V> {
    table: T,
    update_statement: UpdateStatement<T, U, V>,
    key: K,
}

impl<T: Table, K, U, V> Query for ConditionallyUpdated<T, K, U, V> {
    type SqlType = diesel::sql_types::Bool;
}

impl<T: Table, K, U, V> RunQueryDsl<PgConnection> for ConditionallyUpdated<T, K, U, V> {}

impl<T, K, U, V> QueryFragment<Pg> for ConditionallyUpdated<T, K, U, V>
where
    T: Table + diesel::query_dsl::methods::FindDsl<K> + Copy,
    K: Copy,
    <T as diesel::query_dsl::methods::FindDsl<K>>::Output: QueryFragment<Pg>,
    UpdateStatement<T, U, V>: QueryFragment<Pg>
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("WITH found AS (");
        self.table.find(self.key).walk_ast(out.reborrow())?;
        out.push_sql("), updated AS (");
        self.update_statement.walk_ast(out.reborrow())?;
        // TODO: Confirm the incoming Update has no RETURNING already...
        // TODO: Only need primary?
        out.push_sql(" RETURNING *)");

        out.push_sql("SELECT ");

        // TODO: found.<column> as found_<column>
        // TODO: updated.<column> as updated_<column>

        out.push_sql("FROM found LEFT JOIN updated ON");

        // TODO
        // found.id = updated.id

        Ok(())
    }
}

// WITH found   AS (SELECT id, generation FROM T WHERE id = 1),
//      updated AS (UPDATE T SET generation = 2 WHERE id = 1 AND generation = 1 RETURNING *)
// SELECT 'example: successful conditional update with CTE',
//        found.id AS found_id,
//        found.generation AS found_gen,
//        updated.id AS updated_id,
//        updated.generation AS updated_gen
// FROM
//         found
// FULL OUTER JOIN
//         updated
// ON
//         found.id = updated.id;

fn main() {
    use objects::dsl;
    // This connection is not real, but we only care about compiling for now.
    let connection = PgConnection::establish("pretend-i'm-a-URL").unwrap();

    // Querying for an object compiles.
    let _ = dsl::objects
        .first::<Object>(&connection)
        .unwrap();

    let id = Uuid::new_v4();
    dsl::objects.update_if(
        id,
        diesel::update(
            dsl::objects.filter(dsl::id.eq(id))
                        .filter(dsl::gen.ge(2))
        )
    );
}
