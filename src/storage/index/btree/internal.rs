use crate::storage::{
    btree::node::{IndexKey, NodeId},
    page::PageId,
};

#[derive(Clone)]
pub struct InternalNode {
    pub keys: Vec<IndexKey>,
    pub children: Vec<NodeId>, // children.len() = keys.len() + 1
}

#[derive(Clone)]
pub struct DiskInternalNode {
    pub keys: Vec<IndexKey>,
    pub children: Vec<PageId>,
}
