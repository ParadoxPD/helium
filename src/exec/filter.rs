use crate::exec::expr_eval::eval_predicate;
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::Expr;

pub struct FilterExec {
    input: Box<dyn Operator>,
    predicate: Expr,
}

impl FilterExec {
    pub fn new(input: Box<dyn Operator>, predicate: Expr) -> Self {
        Self { input, predicate }
    }
}

impl Operator for FilterExec {
    fn open(&mut self) {
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        while let Some(row) = self.input.next() {
            let val = eval_predicate(&self.predicate, &row);
            println!("[FilterExec] row = {:?}, predicate = {:?}", row, val);

            if matches!(val, true) {
                println!("[FilterExec] PASSED");

                return Some(row);
            } else {
                println!("[FilterExec] REJECTED");
            }
        }

        println!("[FilterExec] EOF");
        None
    }
    fn close(&mut self) {
        self.input.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        buffer::buffer_pool::BufferPool,
        common::value::Value,
        exec::{filter::FilterExec, operator::Operator, scan::ScanExec},
        ir::expr::{BinaryOp, Expr},
        storage::{
            page::{PageId, RowId, StorageRow},
            page_manager::FilePageManager,
            table::{HeapTable, Table},
        },
    };

    #[test]
    fn filter_removes_rows() {
        let schema = vec!["id".into(), "age".into()];

        let rows = vec![
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(2), Value::Int64(30)],
        ];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());

        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        // -------- create table --------
        let mut table = HeapTable::new("users".into(), schema.clone(), 4, bp.clone());
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "users".into(), schema);

        let predicate = Expr::bin(
            Expr::bound_col("users", "age"),
            BinaryOp::Gt,
            Expr::lit(Value::Int64(18)),
        );

        let mut filter = FilterExec::new(Box::new(scan), predicate);
        filter.open();

        let row = filter.next().unwrap();
        assert_eq!(row.get("users.age"), Some(&Value::Int64(30)));
        assert!(filter.next().is_none());
    }
}
