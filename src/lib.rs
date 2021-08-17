#[allow(unused_imports)]
#[macro_use]
extern crate diesel;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_source::Table;

// QueryExistsMaybeUpdate
//
/// Wrapper around [`diesel::update`] for a Table, which allows
/// callers to distinguish between "not found", "found but not updated", and
/// "updated".
pub trait ConditionalUpdate<K, U, V>
where
    Self: Sized + Table,
{
    fn query_exists_maybe_update(
        self,
        key: K,
        update_statement: UpdateStatement<Self, U, V>,
    ) -> ConditionallyUpdated<Self, K, U, V>;
}

impl<T, K, U, V> ConditionalUpdate<K, U, V> for T
where
    T: Table,
{
    fn query_exists_maybe_update(
        self,
        key: K,
        update_statement: UpdateStatement<Self, U, V>,
    ) -> ConditionallyUpdated<Self, K, U, V> {
        ConditionallyUpdated {
            table: self,
            update_statement,
            key,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ConditionallyUpdated<T, K, U, V> {
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
    type SqlType = (); // diesel::sql_types::Uuid, diesel::sql_types::Uuid);
}

impl<T, K, U, V> RunQueryDsl<PgConnection> for ConditionallyUpdated<T, K, U, V> where T: Table {}

/// This implementation uses the following CTE:
///
/// ```text
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    diesel::table! {
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

    #[test]
    fn check_output() {
        use objects::dsl;
        /*
        let connection = PgConnection::establish("pretend-i'm-a-URL").unwrap();
        let _ = dsl::objects
            .first::<Object>(&connection)
            .unwrap();
        */

        let id = Uuid::new_v4();
        let query = dsl::objects.query_exists_maybe_update(
            id,
            diesel::update(dsl::objects)
                .filter(dsl::id.eq(id))
                .filter(dsl::gen.ge(2))
                .set(dsl::runtime.eq("new-runtime")),
        );

        println!("{}", diesel::debug_query::<Pg, _>(&query));
        println!("{:#?}", diesel::debug_query::<Pg, _>(&query));
    }

    #[test]
    fn try_to_see_result() {
        use objects::dsl;
        let connection = PgConnection::establish("pretend-i'm-a-URL").unwrap();
        let _ = dsl::objects
            .first::<Object>(&connection)
            .unwrap();

        let instance_id = Uuid::new_v4();
        let query = diesel::update(dsl::objects)
            .filter(dsl::id.eq(instance_id))
            .filter(dsl::gen.gt(3))
            .set(dsl::runtime.eq("new-runtime"))
            .query_exists_maybe_update(instance_id);

        println!("{}", diesel::debug_query::<Pg, _>(&query));
        println!("{:#?}", diesel::debug_query::<Pg, _>(&query));
    }

}
