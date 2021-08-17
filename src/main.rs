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
struct Object {
    pub id: Uuid,
    pub runtime: String,
    pub data: i32,
}

/// Wrapper around [`diesel::update`] for a Table, which allows
/// callers to distinguish between "not found", "found but not updated", and
/// "updated.
pub trait ConditionalUpdate<K, U, V>
where
    Self: Sized + Table,
{
    fn update_if(self, key: K, update_statement: UpdateStatement<Self, U, V>) -> ConditionallyUpdated<Self, K, U, V>;
}

impl<T, K, U, V> ConditionalUpdate<K, U, V> for T
where
    T: Table,
{
    fn update_if(self, key: K, update_statement: UpdateStatement<Self, U, V>) -> ConditionallyUpdated<Self, K, U, V> {
        ConditionallyUpdated  {
            table: self,
            update_statement,
            key,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ConditionallyUpdated<T, K, U, V>
{
    table: T,
    update_statement: UpdateStatement<T, U, V>,
    key: K,
}

impl<T, K, U, V> Query for ConditionallyUpdated<T, K, U, V>
where
    T: Table,
{
    // TODO: ... Wrong? We're returning two IDs right now
    // TODO: derive from primary
    type SqlType = (diesel::sql_types::Uuid, diesel::sql_types::Uuid);
}

impl<T, K, U, V> RunQueryDsl<PgConnection> for ConditionallyUpdated<T, K, U, V>
where
    T: Table,
{}

/// This implementation uses the following CTE:
///
/// ```
/// // WITH found   AS (SELECT <primary key> FROM T WHERE <primary key = value>)
/// //      updated AS (UPDATE T SET <constraints> RETURNING *)
/// // SELECT
/// //        found.<primary key> AS found_<primary_key>,
/// //        updated.<primary key> AS updated_<primary_key>
/// // FROM
/// //        found
/// // LEFT JOIN
/// //        updated
/// // ON
/// //        found.<primary_key> = updated.<primary_key>;
/// ```
impl<T, K, U, V> QueryFragment<Pg> for ConditionallyUpdated<T, K, U, V>
where
    T: Table + diesel::query_dsl::methods::FindDsl<K> + Copy,
    K: Copy,
    <T as diesel::query_dsl::methods::FindDsl<K>>::Output: QueryFragment<Pg>,
    <T as Table>::PrimaryKey: diesel::Column,
    UpdateStatement<T, U, V>: QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("WITH found AS (");
        self.table.find(self.key).walk_ast(out.reborrow())?;
        out.push_sql("), updated AS (");
        self.update_statement.walk_ast(out.reborrow())?;
        // TODO: Confirm the incoming Update has no RETURNING already...
        // TODO: Only need primary?
        out.push_sql(" RETURNING *) ");

        out.push_sql("SELECT");

        let name = <T::PrimaryKey as Column>::NAME;
        // XXX parsed as found."id" updated."id"FROM found LEFT
        out.push_sql(" found.");
        out.push_identifier(name)?;
        out.push_sql(" updated.");
        out.push_identifier(name)?;

        out.push_sql(" FROM found LEFT JOIN updated ON");

        out.push_sql(" found.");
        out.push_identifier(name)?;
        out.push_sql(" = ");
        out.push_sql("updated.");
        out.push_identifier(name)?;

        Ok(())
    }
}

// SIMPLIFIED
// -------------
// WITH found   AS (SELECT id FROM T WHERE id = 1),
//      updated AS (UPDATE T SET generation = 2 WHERE id = 1 AND generation = 1 RETURNING *)
// SELECT 'example: successful conditional update with CTE',
//        found.id AS found_id,
//        updated.id AS updated_id
// FROM
//        found
// LEFT JOIN
//        updated
// ON
//        found.id = updated.id;



// ORIGINAL
// ------------
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

    /*
    let connection = PgConnection::establish("pretend-i'm-a-URL").unwrap();
    let _ = dsl::objects
        .first::<Object>(&connection)
        .unwrap();
    */

    let id = Uuid::new_v4();
    let query = dsl::objects.update_if(
        id,
        diesel::update(
            dsl::objects.filter(dsl::id.eq(id))
                        .filter(dsl::gen.ge(2))
        ).set(dsl::runtime.eq("new-runtime"))
    );

    println!("{}", diesel::debug_query::<Pg, _>(&query));
    println!("{:#?}", diesel::debug_query::<Pg, _>(&query));
}
