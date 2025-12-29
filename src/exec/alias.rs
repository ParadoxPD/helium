use crate::exec::operator::{Operator, Row};

pub struct AliasExec {
    input: Box<dyn Operator>,
    from: String,
    to: String,
}

impl AliasExec {
    pub fn new(input: Box<dyn Operator>, from: String, to: String) -> Self {
        Self { input, from, to }
    }
}

impl Operator for AliasExec {
    fn open(&mut self) {
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        let row = self.input.next()?;
        eprintln!("[AliasExec] input  = {:?}", row);

        let mut out = Row::new();

        let prefix = format!("{}.", self.from);

        for (k, v) in row {
            if let Some(col) = k.strip_prefix(&prefix) {
                out.insert(format!("{}.{}", self.to, col), v);
            } else {
                out.insert(k, v);
            }
        }

        debug_assert!(
            out.keys().all(|k| k.starts_with(&format!("{}.", self.to))),
            "AliasExec must output alias-qualified columns"
        );

        eprintln!("[AliasExec] output = {:?}", out);
        Some(out)
    }

    fn close(&mut self) {
        self.input.close();
    }
}
