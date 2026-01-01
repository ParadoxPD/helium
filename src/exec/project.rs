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
