use std::sync::Mutex;

use crate::buffer::buffer_pool::BufferPool;
use crate::exec::expr_eval::eval_predicate;
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::Expr;
use crate::storage::page_manager::FilePageManager;
use crate::storage::table::{HeapTable, Table};

pub struct JoinExec {
    left: Box<dyn Operator>,
    right: Box<dyn Operator>,
    on: Expr,

    left_rows: Vec<Row>,
    right_rows: Vec<Row>,

    i: usize,
    j: usize,

    opened: bool,
}

impl JoinExec {
    pub fn new(left: Box<dyn Operator>, right: Box<dyn Operator>, on: Expr) -> Self {
        Self {
            left,
            right,
            on,
            left_rows: Vec::new(),
            right_rows: Vec::new(),
            i: 0,
            j: 0,
            opened: false,
        }
    }

    fn merge_rows(left: &Row, right: &Row) -> Row {
        let mut out = left.clone();
        for (k, v) in right {
            out.insert(k.clone(), v.clone());
        }
        out
    }
}

impl Operator for JoinExec {
    fn open(&mut self) {
        if self.opened {
            return;
        }
        self.opened = true;

        self.left.open();
        self.right.open();

        // Materialize both sides (simple, correct)
        while let Some(row) = self.left.next() {
            self.left_rows.push(row);
        }

        while let Some(row) = self.right.next() {
            self.right_rows.push(row);
        }

        self.i = 0;
        self.j = 0;
    }

    fn next(&mut self) -> Option<Row> {
        while self.i < self.left_rows.len() {
            while self.j < self.right_rows.len() {
                let l = &self.left_rows[self.i];
                let r = &self.right_rows[self.j];
                self.j += 1;

                let merged = Self::merge_rows(l, r);
                if eval_predicate(&self.on, &merged) {
                    println!("JOIN ROW = {:?}", merged);

                    return Some(merged);
                }
            }

            self.j = 0;
            self.i += 1;
        }

        None
    }

    fn close(&mut self) {
        self.left.close();
        self.right.close();
        self.left_rows.clear();
        self.right_rows.clear();
        self.opened = false;
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::join::JoinExec;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::storage::page::PageId;
    use crate::storage::page::{RowId, StorageRow};
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::HeapTable;
    use std::sync::{Arc, Mutex};

    #[test]
    fn join_matches_rows() {
        // ---------- schema ----------
        let user_schema = vec!["id".into(), "name".into()];
        let order_schema = vec!["user_id".into(), "amount".into()];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // ---------- left table (users) ----------
        let mut users_table = HeapTable::new("users".into(), user_schema.clone(), 4, bp.clone());

        users_table.insert_rows(vec![
            vec![Value::Int64(1), Value::String("Alice".into())],
            vec![Value::Int64(2), Value::String("Bob".into())],
        ]);

        // ---------- right table (orders) ----------
        let mut orders_table = HeapTable::new("orders".into(), order_schema.clone(), 4, bp.clone());

        orders_table.insert_rows(vec![
            vec![Value::Int64(1), Value::Int64(200)],
            vec![Value::Int64(3), Value::Int64(50)],
        ]);

        // ---------- scans ----------
        let left = ScanExec::new(Arc::new(users_table), "u".into(), user_schema);
        let right = ScanExec::new(Arc::new(orders_table), "o".into(), order_schema);

        // ---------- join condition ----------
        let on = Expr::bin(
            Expr::bound_col("u", "id"),
            BinaryOp::Eq,
            Expr::bound_col("o", "user_id"),
        );

        let mut join = JoinExec::new(Box::new(left), Box::new(right), on);
        join.open();

        let result = join.next().unwrap();

        assert_eq!(result.get("u.name"), Some(&Value::String("Alice".into())));
        assert_eq!(result.get("o.amount"), Some(&Value::Int64(200)));

        assert!(join.next().is_none());
    }
}
