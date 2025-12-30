use crate::storage::btree::node::{IndexKey, NodeId};

#[derive(Clone)]
pub struct InternalNode {
    pub keys: Vec<IndexKey>,
    pub children: Vec<NodeId>, // children.len() = keys.len() + 1
}
