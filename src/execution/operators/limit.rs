use crate::execution::context::ExecutionContext;
use crate::execution::executor::{Executor, Row};

pub struct LimitExecutor {
    input: Box<dyn Executor>,
    limit: u64,
    offset: u64,
    seen: u64,
    produced: u64,
}

impl LimitExecutor {
    pub fn new(input: Box<dyn Executor>, limit: u64, offset: u64) -> Self {
        Self {
            input,
            limit,
            offset,
            seen: 0,
            produced: 0,
        }
    }
}

impl Executor for LimitExecutor {
    fn open(&mut self, ctx: &ExecutionContext) {
        self.seen = 0;
        self.produced = 0;
        self.input.open(ctx);
    }

    fn next(&mut self) -> Option<Row> {
        if self.produced >= self.limit {
            return None;
        }

        while let Some(row) = self.input.next() {
            if self.seen < self.offset {
                self.seen += 1;
                continue;
            }

            self.produced += 1;
            return Some(row);
        }

        None
    }

    fn close(&mut self) {
        self.input.close();
    }
}
