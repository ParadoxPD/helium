use crate::{
    common::value::Value,
    storage::{
        btree::{internal::InternalNode, leaf::LeafNode},
        page::RowId,
    },
};

pub trait Index: Send + Sync {
    fn insert(&mut self, key: IndexKey, rid: RowId);
    fn delete(&mut self, key: &IndexKey, rid: RowId);
    fn get(&self, key: &IndexKey) -> Vec<RowId>;
    fn range(&self, from: &IndexKey, to: &IndexKey) -> Vec<RowId>;
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum IndexKey {
    Int64(i64),
    String(String),
}

impl TryFrom<&Value> for IndexKey {
    type Error = ();

    fn try_from(v: &Value) -> Result<Self, ()> {
        match v {
            Value::Int64(i) => Ok(IndexKey::Int64(*i)),
            Value::String(s) => Ok(IndexKey::String(s.clone())),
            _ => Err(()),
        }
    }
}

pub type NodeId = usize;

#[derive(Clone)]
pub enum BPlusNode {
    Internal(InternalNode),
    Leaf(LeafNode),
}
