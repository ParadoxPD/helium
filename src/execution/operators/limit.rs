use crate::execution::context::ExecutionContext;
use crate::execution::errors::TableMutationStats;
use crate::execution::executor::{ExecResult, Executor, Row};

pub struct LimitExecutor<'a> {
    input: Box<dyn Executor<'a>>,
    limit: u64,
    offset: u64,
    seen: u64,
    produced: u64,
}

impl<'a> LimitExecutor<'a> {
    pub fn new(input: Box<dyn Executor<'a>>, limit: u64, offset: u64) -> Self {
        Self {
            input,
            limit,
            offset,
            seen: 0,
            produced: 0,
        }
    }
}

impl<'a> Executor<'a> for LimitExecutor<'a> {
    fn open(&mut self, ctx: &ExecutionContext) -> ExecResult<()> {
        self.seen = 0;
        self.produced = 0;
        self.input.open(ctx)
    }

    fn next(&mut self, ctx: &ExecutionContext) -> ExecResult<Option<Row>> {
        if self.produced >= self.limit {
            return Ok(None);
        }

        while let Some(row) = self.input.next(ctx)? {
            if self.seen < self.offset {
                self.seen += 1;
                continue;
            }

            self.produced += 1;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn close(&mut self, ctx: &ExecutionContext) -> ExecResult<Vec<TableMutationStats>> {
        self.input.close(ctx)
    }
}
