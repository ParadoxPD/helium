use crate::common::value::Value;
use crate::exec::expr_eval::eval_value;
use crate::exec::operator::{Operator, Row};
use crate::ir::expr::{BinaryOp, Expr, UnaryOp};

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
        println!("PROJECT input row = {:?}", row);
        let mut out = Row::new();

        for (expr, alias) in &self.exprs {
            println!("PROJECT expr = {:?}", expr);
            let value = eval_value(expr, &row);
            out.insert(alias.clone(), value);
        }
        println!("PROJECT output row = {:?}", out);

        Some(out)
    }

    fn close(&mut self) {
        self.input.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::value::Value;
    use crate::exec::operator::Operator;
    use crate::exec::scan::ScanExec;
    use crate::exec::test_util::qrow;
    use crate::ir::expr::{BinaryOp, Expr};

    #[test]
    fn project_selects_columns() {
        let data = vec![qrow(
            "t",
            &[
                ("name", Value::String("Alice".into())),
                ("age", Value::Int64(30)),
            ],
        )];

        let scan = ScanExec::new(data);
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
        let data = vec![qrow("t", &[("age", Value::Int64(20))])];

        let scan = ScanExec::new(data);

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
        let data = vec![qrow("t", &[("name", Value::String("Bob".into()))])];

        let scan = ScanExec::new(data);

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
        let data = vec![
            qrow("t", &[("x", Value::Int64(1))]),
            qrow("t", &[("x", Value::Int64(2))]),
        ];

        let scan = ScanExec::new(data);

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
