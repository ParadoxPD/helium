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
    use std::sync::Arc;

    use super::*;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::ir::expr::{BinaryOp, Expr};
    use crate::storage::in_memory::InMemoryTable;
    use crate::storage::page::{PageId, RowId, StorageRow};

    fn srow(slot: u16, values: Vec<Value>) -> StorageRow {
        StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: slot,
            },
            values,
        }
    }

    #[test]
    fn project_selects_columns() {
        let schema = vec!["name".into(), "age".into()];
        let rows = vec![srow(
            0,
            vec![Value::String("Alice".into()), Value::Int64(30)],
        )];

        let table = Arc::new(InMemoryTable::new("t".into(), schema.clone(), rows));
        let scan = ScanExec::new(table, "t".into(), schema);

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
        let rows = vec![srow(0, vec![Value::Int64(20)])];

        let table = Arc::new(InMemoryTable::new("t".into(), schema.clone(), rows));
        let scan = ScanExec::new(table, "t".into(), schema);

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
        let rows = vec![srow(0, vec![Value::String("Bob".into())])];

        let table = Arc::new(InMemoryTable::new("t".into(), schema.clone(), rows));
        let scan = ScanExec::new(table, "t".into(), schema);

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
        let rows = vec![
            srow(0, vec![Value::Int64(1)]),
            srow(1, vec![Value::Int64(2)]),
        ];

        let table = Arc::new(InMemoryTable::new("t".into(), schema.clone(), rows));
        let scan = ScanExec::new(table, "t".into(), schema);

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
