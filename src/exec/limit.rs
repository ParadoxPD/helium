use crate::exec::{
    evaluator::ExecError,
    operator::{Operator, Row},
};

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
    fn open(&mut self) -> Result<(), ExecError> {
        self.seen = 0;
        self.input.open()
    }

    fn next(&mut self) -> Result<Option<Row>, ExecError> {
        if self.limit == 0 || self.seen >= self.limit {
            return Ok(None);
        }

        match self.input.next()? {
            Some(row) => {
                self.seen += 1;
                Ok(Some(row))
            }
            None => Ok(None),
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.input.close()
    }
}
