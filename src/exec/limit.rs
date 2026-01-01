use crate::exec::operator::{Operator, Row};

pub struct LimitExec {
    input: Box<dyn Operator>,
    limit: usize,
    seen: usize,
}

impl LimitExec {
    pub fn new(input: Box<dyn Operator>, limit: usize) -> Self {
        Self {
            input,
            limit,
            seen: 0,
        }
    }
}

impl Operator for LimitExec {
    fn open(&mut self) {
        self.seen = 0;
        self.input.open();
    }

    fn next(&mut self) -> Option<Row> {
        if self.seen >= self.limit {
            return None;
        }

        match self.input.next() {
            Some(row) => {
                self.seen += 1;
                Some(row)
            }
            None => None,
        }
    }

    fn close(&mut self) {
        self.input.close();
    }
}
