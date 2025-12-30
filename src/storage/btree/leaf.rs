use crate::storage::{
    btree::node::{IndexKey, NodeId},
    page::RowId,
};

#[derive(Clone)]
pub struct LeafNode {
    pub keys: Vec<IndexKey>,
    pub values: Vec<Vec<RowId>>, // duplicates grouped
    pub next: Option<NodeId>,    // right sibling
}
