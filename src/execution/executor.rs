use crate::execution::context::ExecutionContext;
use crate::types::value::Value;

pub type Row = Vec<Value>;

pub trait Executor {
    fn open(&mut self, ctx: &ExecutionContext);
    fn next(&mut self) -> Option<Row>;
    fn close(&mut self);
}
