use std::sync::{Arc, Mutex};

use crate::buffer::buffer_pool::{BufferPool, BufferPoolHandle};
use crate::common::schema::Schema;
use crate::common::value::Value;
use crate::exec::catalog::Catalog;
use crate::exec::lower;
use crate::exec::operator::Row;
use crate::frontend::sql::lower::Lowered;
use crate::frontend::sql::{lower as sql_lower, parser, pretty_ast::pretty_ast};
use crate::ir::pretty::pretty;
use crate::optimizer::optimize;
use crate::storage::btree::DiskBPlusTree;
use crate::storage::btree::node::IndexKey;
use crate::storage::page::RowId;
use crate::storage::page_manager::FilePageManager;
use crate::storage::table::{HeapTable, Table};

#[derive(Debug)]
pub enum QueryResult {
    Rows(Vec<Row>),
    Explain(String),
}

pub struct Database {
    catalog: Catalog,
    bp: BufferPoolHandle,
}

impl Database {
    pub fn new() -> Self {
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open("/tmp/db.db").unwrap(),
        ))));
        Self {
            catalog: Catalog::new(),
            bp,
        }
    }

    pub fn create_index(&mut self, name: &str, table: &str, column: &str) {
        let table_ref = self.catalog.get(table).expect("table not found");

        // 1. Create disk B+Tree
        let mut index = DiskBPlusTree::new(4, self.bp.clone());

        // 2. Scan table and populate index
        let mut cursor = table_ref.clone().scan();
        while let Some(row) = cursor.next() {
            let col_idx = table_ref
                .schema()
                .iter()
                .position(|c| c == column)
                .expect("column not found");

            let key = IndexKey::try_from(&row.values[col_idx]).unwrap();
            index.insert(key, row.rid);
        }

        let index = Arc::new(Mutex::new(index));
        // 3. Register in catalog
        self.catalog.add_index(
            name.to_string(),
            table.to_string(),
            column.to_string(),
            index,
        );
    }

    pub fn insert_table(&mut self, table: &str, schema: Schema, rows: Vec<Row>) {
        let path = format!("/tmp/{}.db", table);

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut heap = HeapTable::new(table.to_string(), schema.clone(), 128, bp.clone());

        for row in rows {
            let mut values = Vec::with_capacity(schema.len());

            for col in &schema {
                let v = row
                    .get(&format!("{table}.{col}"))
                    .or_else(|| row.get(col))
                    .cloned()
                    .unwrap_or(Value::Null);

                values.push(v);
            }

            let rid = heap.insert(values.clone());

            for entry in self.catalog.indexes_for_table(table) {
                let col_idx = heap
                    .schema()
                    .iter()
                    .position(|c| c == &entry.column)
                    .unwrap();

                let key = IndexKey::try_from(&values[col_idx]).unwrap();
                entry.index.lock().unwrap().insert(key, rid);
            }
        }
        self.catalog.insert(table.to_string(), Arc::new(heap));
    }

    pub fn delete_row(&self, table: &str, rid: RowId) {
        let table_ref = self.catalog.get(table).unwrap();

        // fetch row BEFORE delete
        let row = table_ref.fetch(rid);

        // index maintenance
        for idx in self.catalog.indexes_for_table(table) {
            let col_idx = table_ref
                .schema()
                .iter()
                .position(|c| c == &idx.column)
                .unwrap();

            let key = IndexKey::try_from(&row.values[col_idx]).unwrap();
            idx.index.lock().unwrap().delete(&key, rid);
        }

        // delete from heap
        table_ref.delete(rid);
    }

    pub fn update_row(&self, table: &str, rid: RowId, new_values: Vec<Value>) {
        let table_ref = self.catalog.get(table).unwrap();

        let old_row = table_ref.fetch(rid);

        // remove old keys
        for idx in self.catalog.indexes_for_table(table) {
            let col_idx = table_ref
                .schema()
                .iter()
                .position(|c| c == &idx.column)
                .unwrap();
            let old_key = IndexKey::try_from(&old_row.values[col_idx]).unwrap();
            idx.index.lock().unwrap().delete(&old_key, rid);
        }

        // update heap
        table_ref.update(rid, new_values.clone());

        // insert new keys
        for idx in self.catalog.indexes_for_table(table) {
            let col_idx = table_ref
                .schema()
                .iter()
                .position(|c| c == &idx.column)
                .unwrap();
            let new_key = IndexKey::try_from(&new_values[col_idx]).unwrap();
            idx.index.lock().unwrap().insert(new_key, rid);
        }
    }

    pub fn debug_query(&mut self, sql: &str) -> Lowered {
        let stmt = parser::parse(sql);
        println!("=== AST ===\n{}", pretty_ast(&stmt));

        let lowered = sql_lower::lower_stmt(stmt, &self.catalog);

        match &lowered {
            Lowered::Plan(p) => {
                println!("=== LOGICAL PLAN ===\n{}", pretty(p));
            }
            Lowered::Explain { plan, .. } => {
                println!("=== EXPLAIN PLAN ===\n{}", pretty(plan));
            }
            Lowered::CreateIndex {
                name,
                table,
                column,
            } => {
                println!("======CREATE INDEX======\n{} {} {}", name, table, column);
            }
            Lowered::DropIndex { name } => {
                println!("====DROP INDEX======\n{}", name);
            }
        }
        lowered
    }

    pub fn query(&mut self, sql: &str) -> QueryResult {
        let lowered = self.debug_query(sql);
        match lowered {
            Lowered::Plan(plan) => {
                let plan = optimize(&plan, &self.catalog);
                let mut exec = lower(&plan, &self.catalog);
                exec.open();

                let mut rows = Vec::new();
                while let Some(row) = exec.next() {
                    rows.push(row);
                }

                QueryResult::Rows(rows)
            }

            Lowered::Explain { analyze, plan } => {
                let plan = optimize(&plan, &self.catalog);

                if !analyze {
                    return QueryResult::Explain(pretty(&plan));
                }

                let start = std::time::Instant::now();
                let mut exec = lower(&plan, &self.catalog);
                exec.open();

                let mut rows = 0;
                while exec.next().is_some() {
                    rows += 1;
                }

                let elapsed = start.elapsed().as_micros();
                QueryResult::Explain(format!(
                    "{}\n\nrows={} time={}µs",
                    pretty(&plan),
                    rows,
                    elapsed
                ))
            }
            Lowered::CreateIndex {
                name,
                table,
                column,
            } => {
                self.create_index(&name, &table, &column);
                QueryResult::Explain(format!("Index {} created", name))
            }
            Lowered::DropIndex { name } => {
                let dropped = self.catalog.drop_index(&name);
                if !dropped {
                    panic!("index not found");
                }
                QueryResult::Explain(format!("Index {} dropped", name))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use maplit::hashmap;

    use super::*;
    use crate::common::value::Value;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn sql_query_with_and_and_limit() {
        let mut db = Database::new();

        let schema = vec!["name".into(), "age".into(), "score".into()];

        db.insert_table(
            "users",
            schema,
            vec![
                row(&[
                    ("name", Value::String("Alice".into())),
                    ("age", Value::Int64(30)),
                    ("score", Value::Int64(80)),
                ]),
                row(&[
                    ("name", Value::String("Bob".into())),
                    ("age", Value::Int64(15)),
                    ("score", Value::Int64(90)),
                ]),
                row(&[
                    ("name", Value::String("Carol".into())),
                    ("age", Value::Int64(40)),
                    ("score", Value::Int64(40)),
                ]),
            ],
        );

        match db.query("SELECT name FROM users WHERE age > 18 AND score > 50 LIMIT 1;") {
            QueryResult::Rows(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].get("name"), Some(&Value::String("Alice".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn sql_order_by_works() {
        let mut db = Database::new();

        let schema = vec!["name".into(), "age".into()];

        db.insert_table(
            "users",
            schema,
            vec![
                row(&[
                    ("name", Value::String("Bob".into())),
                    ("age", Value::Int64(30)),
                ]),
                row(&[
                    ("name", Value::String("Alice".into())),
                    ("age", Value::Int64(20)),
                ]),
            ],
        );

        match db.query("SELECT name FROM users ORDER BY age ASC;") {
            QueryResult::Rows(rows) => {
                assert_eq!(rows[0].get("name"), Some(&Value::String("Alice".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn sql_join_works() {
        let mut db = Database::new();

        let users_schema = vec!["id".into(), "name".into()];

        let orders_schema = vec!["user_id".into(), "amount".into()];

        db.insert_table(
            "users",
            users_schema,
            vec![row(&[
                ("id", Value::Int64(1)),
                ("name", Value::String("Alice".into())),
            ])],
        );

        db.insert_table(
            "orders",
            orders_schema,
            vec![row(&[
                ("user_id", Value::Int64(1)),
                ("amount", Value::Int64(200)),
            ])],
        );

        match db.query("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id;") {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].get("name"), Some(&Value::String("Alice".into())));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn create_index_registers_in_catalog() {
        let mut db = Database::new();

        db.insert_table("users", vec!["id".into(), "age".into()], vec![]);

        db.query("CREATE INDEX idx_users_age ON users(age)");

        let index = db.catalog.get_index("users", "age");

        assert!(index.is_some());
    }

    #[test]
    fn create_index_enables_index_scan() {
        let mut db = Database::new();
        let rows = vec![
            hashmap! { "id".into() => Value::Int64(1), "age".into() => Value::Int64(20) },
            hashmap! { "id".into() => Value::Int64(2), "age".into() => Value::Int64(30) },
        ];
        println!("{:?}", rows);

        db.insert_table("users", vec!["id".into(), "age".into()], rows);

        let res = db.query("CREATE INDEX idx_users_age ON users(age)");
        println!("{:?}", res);

        let result = db.query("SELECT * FROM users WHERE age = 20");

        match result {
            QueryResult::Rows(rows) => {
                assert_eq!(rows.len(), 1);
                println!("RETURNED ROWS = {:?}", rows);
                assert_eq!(rows[0]["age"], Value::Int64(20));
            }
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn create_index_twice_errors_or_overwrites() {
        let mut db = Database::new();

        db.insert_table("users", vec!["id".into(), "age".into()], vec![]);

        db.query("CREATE INDEX idx_users_age ON users(age)");

        // Second create — choose ONE behavior:
        // either panic OR overwrite OR return error
        let result = db.query("CREATE INDEX idx_users_age ON users(age)");

        // If you choose overwrite or ignore:
        matches!(result, QueryResult::Explain(_));
    }

    #[test]
    fn drop_index_removes_from_catalog() {
        let mut db = Database::new();

        db.insert_table("users", vec!["id".into(), "age".into()], vec![]);

        db.query("CREATE INDEX idx_users_age ON users(age)");
        assert!(db.catalog.get_index("users", "age").is_some());

        db.query("DROP INDEX idx_users_age");
        assert!(db.catalog.get_index("users", "age").is_none());
    }

    #[test]
    fn drop_index_disables_index_scan() {
        let mut db = Database::new();

        db.insert_table(
            "users",
            vec!["id".into(), "age".into()],
            vec![hashmap! { "id".into() => Value::Int64(1), "age".into() => Value::Int64(20) }],
        );

        db.query("CREATE INDEX idx_users_age ON users(age)");
        db.query("DROP INDEX idx_users_age");

        let plan = match db.query("EXPLAIN SELECT * FROM users WHERE age = 20") {
            QueryResult::Explain(e) => e,
            _ => panic!(),
        };

        assert!(!plan.contains("IndexScan"));
    }
}
