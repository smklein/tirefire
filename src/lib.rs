#[allow(unused_imports)]
#[macro_use]
extern crate diesel;
use diesel::associations::HasTable;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::*;
use diesel::query_source::Table;
use diesel::sql_types;

/// Wrapper around [`diesel::update`] for a Table, which allows
/// callers to distinguish between "not found", "found but not updated", and
/// "updated".
pub trait UpdateCte<T, K, U, V>: Sized {
    /// Nests the existing update statement in a CTE which
    /// identifies if the row exists (by ID), even if the row
    /// cannot be successfully updated.
    fn check_if_exists(
        self,
        key: K,
    ) -> UpdateAndQueryStatement<T, K, U, V>;
}

impl<T, K, U, V> UpdateCte<T, K, U, V> for UpdateStatement<T, U, V> {
    fn check_if_exists(
        self,
        key: K,
    ) -> UpdateAndQueryStatement<T, K, U, V> {
        UpdateAndQueryStatement {
            update_statement: self,
            key,
        }
    }
}

/// An UPDATE statement which can be combined (via a CTE)
/// with other statements to also SELECT a row.
#[derive(Debug, Clone, Copy)]
pub struct UpdateAndQueryStatement<T, K, U, V> {
    update_statement: UpdateStatement<T, U, V>,
    key: K,
}

impl<T, K, U, V> QueryId for UpdateAndQueryStatement<T, K, U, V> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
    fn query_id() -> Option<core::any::TypeId> { None }
}

pub enum UpdateAndQueryResult {
    Updated,
    NotUpdatedButExists,
}

impl <T, K, U, V> UpdateAndQueryStatement<T, K, U, V>
where
    T: Table,
    Pg: sql_types::HasSqlType<<Self as AsQuery>::SqlType>,
    Self: AsQuery + RunQueryDsl<PgConnection>,
    (<<T as Table>::PrimaryKey as Expression>::SqlType, <<T as Table>::PrimaryKey as Expression>::SqlType): QueryId,
    <Self as AsQuery>::Query: QueryFragment<Pg> + QueryId,
    (K, K): Queryable<<Self as AsQuery>::SqlType, Pg>,
    K: PartialEq,
{
    pub fn execute_and_check(self, conn: &PgConnection) -> Result<UpdateAndQueryResult, diesel::result::Error> {
        let results = self.load::<(K, K)>(conn)?;
        let ids = results.get(0).unwrap();
        if ids.0 == ids.1 {
            Ok(UpdateAndQueryResult::Updated)
        } else {
            Ok(UpdateAndQueryResult::NotUpdatedButExists)
        }
    }
}

impl<T, K, U, V> Query for UpdateAndQueryStatement<T, K, U, V>
where
    T: Table,
{
type SqlType =
    (<<T as Table>::PrimaryKey as Expression>::SqlType,
     <<T as Table>::PrimaryKey as Expression>::SqlType);
}

impl<T, K, U, V> RunQueryDsl<PgConnection> for UpdateAndQueryStatement<T, K, U, V> where T: Table {}

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
impl<T, K, U, V> QueryFragment<Pg> for UpdateAndQueryStatement<T, K, U, V>
where
    T: HasTable<Table=T> + Table + diesel::query_dsl::methods::FindDsl<K> + Copy,
    K: Copy,
    <T as diesel::query_dsl::methods::FindDsl<K>>::Output: QueryFragment<Pg>,
    <T as Table>::PrimaryKey: diesel::Column,
    UpdateStatement<T, U, V>: QueryFragment<Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("WITH found AS (");
        let subquery = T::table().find(self.key);
        subquery.walk_ast(out.reborrow())?;
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

    /*
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
        let query = dsl::objects.check_if_exists(
            id,
            diesel::update(dsl::objects)
                .filter(dsl::id.eq(id))
                .filter(dsl::gen.ge(2))
                .set(dsl::runtime.eq("new-runtime")),
        );

        println!("{}", diesel::debug_query::<Pg, _>(&query));
        println!("{:#?}", diesel::debug_query::<Pg, _>(&query));
    }
    */

    #[test]
    fn try_to_see_result() {
        use objects::dsl;
        let connection = PgConnection::establish("pretend-i'm-a-URL").unwrap();
        let _ = dsl::objects
            .first::<Object>(&connection)
            .unwrap();

        let instance_id = Uuid::new_v4();
        let result = diesel::update(dsl::objects)
            .filter(dsl::id.eq(instance_id))
            .filter(dsl::gen.gt(3))
            .set(dsl::runtime.eq("new-runtime"))
            .check_if_exists(instance_id)
            .execute_and_check(&connection)
            .unwrap();
    }

}