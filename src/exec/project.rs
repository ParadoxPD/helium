use crate::exec::expr_eval::eval_value;
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::Expr;

pub struct ProjectExec {
    input: Box<dyn Operator>,
    exprs: Vec<(Expr, String)>,
}

impl ProjectExec {
    pub fn new(input: Box<dyn Operator>, exprs: Vec<(Expr, String)>) -> Self {
        Self { input, exprs }
    }
}

impl Operator for ProjectExec {
    fn open(&mut self) {
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        let row = self.input.next()?;
        let mut out = Row::new();

        for (expr, alias) in &self.exprs {
            let value = eval_value(expr, &row);
            out.insert(alias.clone(), value);
        }

        debug_assert!(
            out.keys().all(|k| !k.contains('.')),
            "Project output must be unqualified"
        );

        Some(out)
    }

    fn close(&mut self) {
        self.input.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::buffer::buffer_pool::BufferPool;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::storage::page_manager::FilePageManager;
    use crate::storage::table::HeapTable;

    #[test]
    fn project_selects_columns() {
        let schema = vec!["name".into(), "age".into()];
        let rows = vec![vec![Value::String("Alice".into()), Value::Int64(30)]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);

        let mut project = ProjectExec::new(
            Box::new(scan),
            vec![(
                Expr::BoundColumn {
                    table: "t".into(),
                    name: "name".into(),
                },
                "name".into(),
            )],
        );

        project.open();
        let out = project.next().unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out.get("name"), Some(&Value::String("Alice".into())));
    }

    #[test]
    fn project_computes_expressions() {
        let schema = vec!["age".into()];
        let rows = vec![vec![Value::Int64(20)]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);

        let expr = Expr::bin(
            Expr::bound_col("t", "age"),
            BinaryOp::Add,
            Expr::lit(Value::Int64(1)),
        );

        let mut project = ProjectExec::new(Box::new(scan), vec![(expr, "next_age".into())]);

        project.open();
        let out = project.next().unwrap();

        assert_eq!(out.get("next_age"), Some(&Value::Int64(21)));
    }

    #[test]
    fn project_handles_missing_column_as_null() {
        let schema = vec!["name".into()];
        let rows = vec![vec![Value::String("Bob".into())]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);

        let mut project = ProjectExec::new(
            Box::new(scan),
            vec![(Expr::bound_col("t", "age"), "age".into())],
        );

        project.open();
        let out = project.next().unwrap();

        assert_eq!(out.get("age"), Some(&Value::Null));
    }

    #[test]
    fn project_preserves_row_count() {
        let schema = vec!["x".into()];
        let rows = vec![vec![Value::Int64(1)], vec![Value::Int64(2)]];

        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut table = HeapTable::new("t".into(), schema.clone(), 4, bp);
        table.insert_rows(rows);

        let scan = ScanExec::new(Arc::new(table), "t".into(), schema);

        let mut project = ProjectExec::new(
            Box::new(scan),
            vec![(Expr::bound_col("t", "x"), "x".into())],
        );

        project.open();
        assert!(project.next().is_some());
        assert!(project.next().is_some());
        assert!(project.next().is_none());
    }
}
