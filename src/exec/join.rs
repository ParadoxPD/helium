use crate::common::schema::Schema;
use crate::exec::expr_eval::eval_predicate;
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::Expr;
use crate::storage::page::{RowId, StorageRow};

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

#[test]
fn join_matches_rows() {
    use crate::common::value::Value;
    use crate::exec::join::JoinExec;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::storage::in_memory::InMemoryTable;
    use crate::storage::page::PageId;
    use std::sync::Arc;

    // ---------- schema ----------
    let user_schema = vec!["id".into(), "name".into()];
    let order_schema = vec!["user_id".into(), "amount".into()];

    // ---------- left table (users) ----------
    let users = vec![
        StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: 0,
            },
            values: vec![Value::Int64(1), Value::String("Alice".into())],
        },
        StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: 1,
            },
            values: vec![Value::Int64(2), Value::String("Bob".into())],
        },
    ];

    let users_table = Arc::new(InMemoryTable::new(
        "users".into(),
        user_schema.clone(),
        users,
    ));

    // ---------- right table (orders) ----------
    let orders = vec![
        StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: 0,
            },
            values: vec![Value::Int64(1), Value::Int64(200)],
        },
        StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: 1,
            },
            values: vec![Value::Int64(3), Value::Int64(50)],
        },
    ];

    let orders_table = Arc::new(InMemoryTable::new(
        "orders".into(),
        order_schema.clone(),
        orders,
    ));

    // ---------- scans ----------
    let left = ScanExec::new(users_table, "u".into(), user_schema);

    let right = ScanExec::new(orders_table, "o".into(), order_schema);

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
