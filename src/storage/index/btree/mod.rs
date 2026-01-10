pub mod disk;
pub mod index_impl;
pub mod key;
pub mod node;

use crate::storage::index::btree::disk::BPlusTree;

pub struct BTreeIndex {
    tree: BPlusTree,
}

impl BTreeIndex {
    pub fn new(tree: BPlusTree) -> Self {
        Self { tree }
    }
}
