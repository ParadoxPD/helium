pub mod cursor;
pub mod internal;
pub mod leaf;
pub mod node;

use crate::{
    common::value::Value,
    storage::{
        btree::{
            internal::InternalNode,
            leaf::LeafNode,
            node::{BPlusNode, Index, IndexKey, NodeId},
        },
        page::RowId,
    },
};

pub struct BPlusTree {
    order: usize,
    root: NodeId,
    nodes: Vec<BPlusNode>,
}

impl BPlusTree {
    pub fn new(order: usize) -> Self {
        assert!(order >= 3, "B+Tree order must be ≥ 3");

        let root = LeafNode {
            keys: Vec::new(),
            values: Vec::new(),
            next: None,
        };

        Self {
            order,
            root: 0,
            nodes: vec![BPlusNode::Leaf(root)],
        }
    }
}
impl BPlusTree {
    fn find_leaf(&self, key: &IndexKey) -> NodeId {
        let mut node = self.root;

        loop {
            match &self.nodes[node] {
                BPlusNode::Leaf(_) => return node,
                BPlusNode::Internal(internal) => {
                    let idx = match internal.keys.binary_search(key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    node = internal.children[idx];
                }
            }
        }
    }
}
impl BPlusTree {
    pub fn insert(&mut self, key: IndexKey, rid: RowId) {
        if let Some((sep, right)) = self.insert_recursive(self.root, key, rid) {
            // Root split → grow tree height
            let new_root = InternalNode {
                keys: vec![sep],
                children: vec![self.root, right],
            };

            self.root = self.nodes.len();
            self.nodes.push(BPlusNode::Internal(new_root));
        }
    }
    pub fn delete(&mut self, key: &IndexKey, rid: RowId) {
        self.delete_recursive(self.root, key, rid);

        // Normalize root fully
        loop {
            let collapse = match &self.nodes[self.root] {
                BPlusNode::Internal(i) if i.keys.is_empty() => Some(i.children[0]),
                _ => None,
            };

            if let Some(new_root) = collapse {
                self.root = new_root;
            } else {
                break;
            }
        }

        // ONLY now invariants must hold
        #[cfg(debug_assertions)]
        self.assert_invariants();
    }
}

impl BPlusTree {
    fn insert_into_leaf(&mut self, leaf_id: NodeId, key: IndexKey, rid: RowId) {
        let leaf = match &mut self.nodes[leaf_id] {
            BPlusNode::Leaf(l) => l,
            _ => unreachable!(),
        };

        match leaf.keys.binary_search(&key) {
            Ok(i) => leaf.values[i].push(rid),
            Err(i) => {
                leaf.keys.insert(i, key);
                leaf.values.insert(i, vec![rid]);
            }
        }
    }

    fn leaf_overflow(&self, leaf_id: NodeId) -> bool {
        match &self.nodes[leaf_id] {
            BPlusNode::Leaf(l) => l.keys.len() > self.max_keys(),
            _ => false,
        }
    }
}
impl BPlusTree {
    pub fn get(&self, key: &IndexKey) -> Vec<RowId> {
        let leaf_id = self.find_leaf(key);

        match &self.nodes[leaf_id] {
            BPlusNode::Leaf(l) => match l.keys.binary_search(key) {
                Ok(i) => l.values[i].clone(),
                Err(_) => Vec::new(),
            },
            _ => unreachable!(),
        }
    }
}
impl BPlusTree {
    pub fn range(&self, from: &IndexKey, to: &IndexKey) -> Vec<RowId> {
        let mut out = Vec::new();
        let mut node = self.find_leaf(from);

        loop {
            let leaf = match &self.nodes[node] {
                BPlusNode::Leaf(l) => l,
                _ => unreachable!(),
            };

            for (k, rids) in leaf.keys.iter().zip(&leaf.values) {
                if k >= from && k <= to {
                    out.extend(rids.iter().copied());
                }
            }

            match leaf.next {
                Some(next) => node = next,
                None => break,
            }
        }

        out
    }
}

impl BPlusTree {
    fn insert_recursive(
        &mut self,
        node_id: NodeId,
        key: IndexKey,
        rid: RowId,
    ) -> Option<(IndexKey, NodeId)> {
        match &mut self.nodes[node_id] {
            BPlusNode::Leaf(_) => {
                self.insert_into_leaf(node_id, key, rid);

                if self.leaf_overflow(node_id) {
                    let res = self.split_leaf(node_id);
                    self.assert_invariants();
                    Some(res)
                } else {
                    None
                }
            }

            BPlusNode::Internal(internal) => {
                let idx = match internal.keys.binary_search(&key) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };

                let child = internal.children[idx];

                if let Some((sep, new_child)) = self.insert_recursive(child, key, rid) {
                    self.insert_into_internal(node_id, sep, new_child);

                    if self.internal_overflow(node_id) {
                        let res = self.split_internal(node_id);
                        self.assert_invariants();
                        Some(res)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    fn delete_recursive(&mut self, node_id: NodeId, key: &IndexKey, rid: RowId) -> bool {
        match &mut self.nodes[node_id] {
            BPlusNode::Leaf(l) => {
                if let Ok(i) = l.keys.binary_search(key) {
                    l.values[i].retain(|r| *r != rid);
                    if l.values[i].is_empty() {
                        l.keys.remove(i);
                        l.values.remove(i);
                    }
                }
                l.keys.len() < self.min_keys()
            }

            BPlusNode::Internal(i) => {
                let idx = match i.keys.binary_search(key) {
                    Ok(j) => j + 1,
                    Err(j) => j,
                };

                // extract child id WITHOUT holding mutable borrow
                let child_id = i.children[idx];

                // IMPORTANT: mutable borrow of parent ends here

                let underflow = self.delete_recursive(child_id, key, rid);

                if underflow {
                    self.rebalance_child(node_id, idx);
                }

                // reborrow parent to check keys
                let i = match &self.nodes[node_id] {
                    BPlusNode::Internal(i) => i,
                    _ => unreachable!(),
                };

                i.keys.len() < self.min_keys()
            }
        }
    }

    fn node_key_count(&self, id: NodeId) -> usize {
        match &self.nodes[id] {
            BPlusNode::Leaf(l) => l.keys.len(),
            BPlusNode::Internal(i) => i.keys.len(),
        }
    }

    fn rebalance_child(&mut self, parent: NodeId, idx: usize) {
        let child_id = {
            let p = match &self.nodes[parent] {
                BPlusNode::Internal(p) => p,
                _ => unreachable!(),
            };
            p.children[idx]
        };

        match &self.nodes[child_id] {
            BPlusNode::Leaf(_) => {
                self.rebalance_leaf(parent, idx);
            }
            BPlusNode::Internal(_) => {
                self.rebalance_internal_node(parent, idx);
            }
        }
    }

    fn rebalance_leaf(&mut self, parent: NodeId, idx: usize) {
        let min_keys = self.min_keys();

        // ---- read parent immutably ----
        let (left_idx, right_idx, sep_left, sep_right, leaf_id) = {
            let p = match &self.nodes[parent] {
                BPlusNode::Internal(p) => p,
                _ => unreachable!(),
            };

            (
                if idx > 0 { Some(idx - 1) } else { None },
                if idx + 1 < p.children.len() {
                    Some(idx + 1)
                } else {
                    None
                },
                if idx > 0 { Some(idx - 1) } else { None },
                if idx + 1 < p.children.len() {
                    Some(idx)
                } else {
                    None
                },
                p.children[idx],
            )
        };

        // ---- try borrow from LEFT leaf ----
        if let (Some(lidx), Some(sep_idx)) = (left_idx, sep_left) {
            let left_id = {
                let p = match &self.nodes[parent] {
                    BPlusNode::Internal(p) => p,
                    _ => unreachable!(),
                };
                p.children[lidx]
            };

            if self.node_key_count(left_id) > min_keys {
                let (left, leaf) = if left_id < leaf_id {
                    let (l, r) = self.nodes.split_at_mut(leaf_id);
                    (&mut l[left_id], &mut r[0])
                } else {
                    let (l, r) = self.nodes.split_at_mut(left_id);
                    (&mut r[0], &mut l[leaf_id])
                };

                let (left, leaf) = match (left, leaf) {
                    (BPlusNode::Leaf(l), BPlusNode::Leaf(c)) => (l, c),
                    _ => unreachable!(),
                };
                leaf.keys.insert(0, left.keys.pop().unwrap());
                leaf.values.insert(0, left.values.pop().unwrap());

                let new_sep = {
                    let p = match &self.nodes[parent] {
                        BPlusNode::Internal(p) => p,
                        _ => unreachable!(),
                    };
                    let right_child_id = p.children[sep_idx + 1];
                    match &self.nodes[right_child_id] {
                        BPlusNode::Leaf(l) => l.keys[0].clone(),
                        BPlusNode::Internal(i) => i.keys[0].clone(),
                    }
                };

                let p = match &mut self.nodes[parent] {
                    BPlusNode::Internal(p) => p,
                    _ => unreachable!(),
                };
                p.keys[sep_idx] = new_sep;

                return;
            }
        }

        // ---- try borrow from RIGHT leaf ----
        if let (Some(ridx), Some(sep_idx)) = (right_idx, sep_right) {
            let right_id = {
                let p = match &self.nodes[parent] {
                    BPlusNode::Internal(p) => p,
                    _ => unreachable!(),
                };
                p.children[ridx]
            };

            if self.node_key_count(right_id) > min_keys {
                let (leaf, right) = if leaf_id < right_id {
                    let (l, r) = self.nodes.split_at_mut(right_id);
                    (&mut l[leaf_id], &mut r[0])
                } else {
                    let (l, r) = self.nodes.split_at_mut(leaf_id);
                    (&mut r[0], &mut l[right_id])
                };

                let (leaf, right) = match (leaf, right) {
                    (BPlusNode::Leaf(c), BPlusNode::Leaf(r)) => (c, r),
                    _ => unreachable!(),
                };

                leaf.keys.push(right.keys.remove(0));
                leaf.values.push(right.values.remove(0));

                let new_sep = {
                    let p = match &self.nodes[parent] {
                        BPlusNode::Internal(p) => p,
                        _ => unreachable!(),
                    };
                    let right_child_id = p.children[sep_idx + 1];
                    match &self.nodes[right_child_id] {
                        BPlusNode::Leaf(l) => l.keys[0].clone(),
                        BPlusNode::Internal(i) => i.keys[0].clone(),
                    }
                };

                let p = match &mut self.nodes[parent] {
                    BPlusNode::Internal(p) => p,
                    _ => unreachable!(),
                };

                p.keys[sep_idx] = new_sep;
                return;
            }
        }

        // ---- must MERGE ----
        let (left, right, sep_idx) = if let Some(lidx) = left_idx {
            (lidx, idx, lidx)
        } else {
            (idx, idx + 1, idx)
        };

        let (left_id, right_id) = {
            let p = match &self.nodes[parent] {
                BPlusNode::Internal(p) => p,
                _ => unreachable!(),
            };
            (p.children[left], p.children[right])
        };

        let (left_leaf, right_leaf) = if left_id < right_id {
            let (l, r) = self.nodes.split_at_mut(right_id);
            (&mut l[left_id], &mut r[0])
        } else {
            let (l, r) = self.nodes.split_at_mut(left_id);
            (&mut r[0], &mut l[right_id])
        };

        let (left_leaf, right_leaf) = match (left_leaf, right_leaf) {
            (BPlusNode::Leaf(l), BPlusNode::Leaf(r)) => (l, r),
            _ => unreachable!(),
        };

        left_leaf.keys.extend(right_leaf.keys.drain(..));
        left_leaf.values.extend(right_leaf.values.drain(..));
        left_leaf.next = right_leaf.next.take();

        let p = match &mut self.nodes[parent] {
            BPlusNode::Internal(p) => p,
            _ => unreachable!(),
        };
        p.keys.remove(sep_idx);
        p.children.remove(right);
    }

    fn rebalance_internal_node(&mut self, parent: NodeId, idx: usize) {
        // ---- Step 1: read parent immutably ----
        let (left_idx, right_idx, sep_idx, left_id, right_id) = {
            let parent_node = match &self.nodes[parent] {
                BPlusNode::Internal(p) => p,
                _ => unreachable!(),
            };

            let left_idx = if idx > 0 { Some(idx - 1) } else { None };
            let right_idx = if idx + 1 < parent_node.children.len() {
                Some(idx + 1)
            } else {
                None
            };

            let sep_idx = if idx > 0 { idx - 1 } else { idx };

            let left_id = left_idx.map(|i| parent_node.children[i]);
            let right_id = right_idx.map(|i| parent_node.children[i]);

            (left_idx, right_idx, sep_idx, left_id, right_id)
        };

        // ---- Step 2: try borrow from LEFT ----
        if let (Some(lidx), Some(lid)) = (left_idx, left_id) {
            if self.node_key_count(lid) > self.min_keys() {
                self.borrow_from_left(parent, lidx, idx, sep_idx);
                return;
            }
        }

        // ---- Step 3: try borrow from RIGHT ----
        if let (Some(ridx), Some(rid)) = (right_idx, right_id) {
            if self.node_key_count(rid) > self.min_keys() {
                self.borrow_from_right(parent, idx, ridx, sep_idx);
                return;
            }
        }

        // ---- Step 4: must merge ----
        let (left, right) = if idx > 0 {
            (idx - 1, idx)
        } else {
            (idx, idx + 1)
        };

        let (left_id, right_id) = {
            let parent_node = match &self.nodes[parent] {
                BPlusNode::Internal(p) => p,
                _ => unreachable!(),
            };
            (parent_node.children[left], parent_node.children[right])
        };

        self.merge_nodes(parent, left, right, sep_idx, left_id, right_id);
    }

    fn borrow_internal_from_left(
        &mut self,
        parent: NodeId,
        left_idx: usize,
        child_idx: usize,
        sep_idx: usize,
    ) {
        // extract child ids
        let (left_id, child_id) = {
            let p = match &self.nodes[parent] {
                BPlusNode::Internal(p) => p,
                _ => unreachable!(),
            };
            (p.children[left_idx], p.children[child_idx])
        };

        // split borrow
        let (left, child) = if left_id < child_id {
            let (l, r) = self.nodes.split_at_mut(child_id);
            (&mut l[left_id], &mut r[0])
        } else {
            let (l, r) = self.nodes.split_at_mut(left_id);
            (&mut r[0], &mut l[child_id])
        };

        let (left, child) = match (left, child) {
            (BPlusNode::Internal(l), BPlusNode::Internal(c)) => (l, c),
            _ => unreachable!(),
        };

        // move ONE child pointer
        let borrowed_child = left.children.pop().unwrap();
        child.children.insert(0, borrowed_child);

        // recompute separator = min key of RIGHT child
        let new_sep = self.min_key(child.children[0]);

        let p = match &mut self.nodes[parent] {
            BPlusNode::Internal(p) => p,
            _ => unreachable!(),
        };
        p.keys[sep_idx] = new_sep;
    }

    fn borrow_from_left(
        &mut self,
        parent: NodeId,
        left_idx: usize,
        child_idx: usize,
        sep_idx: usize,
    ) {
        let sep = {
            let p = match &mut self.nodes[parent] {
                BPlusNode::Internal(p) => p,
                _ => unreachable!(),
            };
            p.keys[sep_idx].clone()
        };

        let (left, child) = {
            let left_id;
            let child_id;
            {
                let p = match &self.nodes[parent] {
                    BPlusNode::Internal(p) => p,
                    _ => unreachable!(),
                };
                left_id = p.children[left_idx];
                child_id = p.children[child_idx];
            }

            if left_id < child_id {
                let (l, r) = self.nodes.split_at_mut(child_id);
                (&mut l[left_id], &mut r[0])
            } else {
                let (l, r) = self.nodes.split_at_mut(left_id);
                (&mut r[0], &mut l[child_id])
            }
        };

        match (left, child) {
            (BPlusNode::Internal(l), BPlusNode::Internal(c)) => {
                c.keys.insert(0, sep);
                let borrowed = l.keys.pop().unwrap();

                let child_ptr = l.children.pop().unwrap();
                c.children.insert(0, child_ptr);

                let p = match &mut self.nodes[parent] {
                    BPlusNode::Internal(p) => p,
                    _ => unreachable!(),
                };
                p.keys[sep_idx] = borrowed;
            }
            _ => unreachable!(),
        }
    }

    fn borrow_internal_from_right(
        &mut self,
        parent: NodeId,
        child_idx: usize,
        right_idx: usize,
        sep_idx: usize,
    ) {
        let (child_id, right_id) = {
            let p = match &self.nodes[parent] {
                BPlusNode::Internal(p) => p,
                _ => unreachable!(),
            };
            (p.children[child_idx], p.children[right_idx])
        };

        let (child, right) = if child_id < right_id {
            let (l, r) = self.nodes.split_at_mut(right_id);
            (&mut l[child_id], &mut r[0])
        } else {
            let (l, r) = self.nodes.split_at_mut(child_id);
            (&mut r[0], &mut l[right_id])
        };

        let (child, right) = match (child, right) {
            (BPlusNode::Internal(c), BPlusNode::Internal(r)) => (c, r),
            _ => unreachable!(),
        };

        // move ONE child pointer
        let borrowed_child = right.children.remove(0);
        child.children.push(borrowed_child);

        // recompute separator
        let new_sep = self.min_key(right.children[0]);

        let p = match &mut self.nodes[parent] {
            BPlusNode::Internal(p) => p,
            _ => unreachable!(),
        };
        p.keys[sep_idx] = new_sep;
    }

    fn borrow_from_right(
        &mut self,
        parent: NodeId,
        child_idx: usize,
        right_idx: usize,
        sep_idx: usize,
    ) {
        let sep = {
            let p = match &mut self.nodes[parent] {
                BPlusNode::Internal(p) => p,
                _ => unreachable!(),
            };
            p.keys[sep_idx].clone()
        };

        let (child, right) = {
            let child_id;
            let right_id;
            {
                let p = match &self.nodes[parent] {
                    BPlusNode::Internal(p) => p,
                    _ => unreachable!(),
                };
                child_id = p.children[child_idx];
                right_id = p.children[right_idx];
            }

            if child_id < right_id {
                let (l, r) = self.nodes.split_at_mut(right_id);
                (&mut l[child_id], &mut r[0])
            } else {
                let (l, r) = self.nodes.split_at_mut(child_id);
                (&mut r[0], &mut l[right_id])
            }
        };

        match (child, right) {
            (BPlusNode::Internal(c), BPlusNode::Internal(r)) => {
                c.keys.push(sep);
                let borrowed = r.keys.remove(0);

                let child_ptr = r.children.remove(0);
                c.children.push(child_ptr);

                let p = match &mut self.nodes[parent] {
                    BPlusNode::Internal(p) => p,
                    _ => unreachable!(),
                };
                p.keys[sep_idx] = borrowed;
            }
            _ => unreachable!(),
        }
    }

    fn merge_nodes(
        &mut self,
        parent: NodeId,
        left_idx: usize,
        right_idx: usize,
        sep_idx: usize,
        left_id: NodeId,
        right_id: NodeId,
    ) {
        let sep = {
            let parent_node = match &mut self.nodes[parent] {
                BPlusNode::Internal(p) => p,
                _ => unreachable!(),
            };
            parent_node.keys.remove(sep_idx)
        };

        let (left_node, right_node) = if left_id < right_id {
            let (l, r) = self.nodes.split_at_mut(right_id);
            (&mut l[left_id], &mut r[0])
        } else {
            let (l, r) = self.nodes.split_at_mut(left_id);
            (&mut r[0], &mut l[right_id])
        };

        match (left_node, right_node) {
            (BPlusNode::Leaf(l), BPlusNode::Leaf(r)) => {
                l.keys.extend(r.keys.drain(..));
                l.values.extend(r.values.drain(..));
                l.next = r.next.take();
            }

            (BPlusNode::Internal(l), BPlusNode::Internal(r)) => {
                l.keys.push(sep);
                l.keys.extend(r.keys.drain(..));
                l.children.extend(r.children.drain(..));
            }

            _ => unreachable!(),
        }

        let parent_node = match &mut self.nodes[parent] {
            BPlusNode::Internal(p) => p,
            _ => unreachable!(),
        };

        parent_node.children.remove(right_idx);
    }
}
impl BPlusTree {
    fn insert_into_internal(&mut self, node_id: NodeId, key: IndexKey, right_child: NodeId) {
        let internal = match &mut self.nodes[node_id] {
            BPlusNode::Internal(i) => i,
            _ => unreachable!(),
        };

        let idx = match internal.keys.binary_search(&key) {
            Ok(i) => i + 1,
            Err(i) => i,
        };

        internal.keys.insert(idx, key);
        internal.children.insert(idx + 1, right_child);
    }

    fn internal_overflow(&self, node_id: NodeId) -> bool {
        matches!(
            &self.nodes[node_id],
            BPlusNode::Internal(i) if i.keys.len() > self.max_keys()
        )
    }
}
impl BPlusTree {
    fn split_leaf(&mut self, leaf_id: NodeId) -> (IndexKey, NodeId) {
        let new_leaf_id = self.nodes.len();

        let leaf = match &mut self.nodes[leaf_id] {
            BPlusNode::Leaf(l) => l,
            _ => unreachable!(),
        };

        let split_at = leaf.keys.len() / 2;

        let new_keys = leaf.keys.split_off(split_at);
        let new_values = leaf.values.split_off(split_at);

        let separator = new_keys[0].clone();

        let new_leaf = LeafNode {
            keys: new_keys,
            values: new_values,
            next: leaf.next.take(),
        };

        leaf.next = Some(new_leaf_id);

        self.nodes.push(BPlusNode::Leaf(new_leaf));
        (separator, new_leaf_id)
    }
}
impl BPlusTree {
    fn split_internal(&mut self, node_id: NodeId) -> (IndexKey, NodeId) {
        let new_internal_id = self.nodes.len();

        let internal = match &mut self.nodes[node_id] {
            BPlusNode::Internal(i) => i,
            _ => unreachable!(),
        };

        let mid = internal.keys.len() / 2;
        let separator = internal.keys[mid].clone();

        let right_keys = internal.keys.split_off(mid + 1);
        let right_children = internal.children.split_off(mid + 1);

        internal.keys.truncate(mid);

        let new_internal = InternalNode {
            keys: right_keys,
            children: right_children,
        };

        self.nodes.push(BPlusNode::Internal(new_internal));

        (separator, new_internal_id)
    }
}

impl BPlusTree {
    fn max_keys(&self) -> usize {
        self.order - 1
    }

    fn min_keys(&self) -> usize {
        (self.order - 1) / 2
    }

    fn min_key(&self, node: NodeId) -> IndexKey {
        match &self.nodes[node] {
            BPlusNode::Leaf(l) => l.keys[0].clone(),
            BPlusNode::Internal(i) => self.min_key(i.children[0]),
        }
    }
}

impl Index for BPlusTree {
    fn insert(&mut self, key: IndexKey, rid: RowId) {
        BPlusTree::insert(self, key, rid);
    }

    fn delete(&mut self, key: &IndexKey, rid: RowId) {
        BPlusTree::delete(self, key, rid);
    }

    fn get(&self, key: &IndexKey) -> Vec<RowId> {
        BPlusTree::get(self, key)
    }

    fn range(&self, from: &IndexKey, to: &IndexKey) -> Vec<RowId> {
        BPlusTree::range(self, from, to)
    }
}

#[cfg(debug_assertions)]
impl BPlusTree {
    fn assert_invariants(&self) {
        assert!(self.order >= 3);

        use std::collections::HashSet;
        let mut visited = HashSet::new();

        self.assert_node(self.root, &mut visited);
    }

    fn assert_node(&self, id: NodeId, visited: &mut std::collections::HashSet<NodeId>) {
        if !visited.insert(id) {
            return; // already checked
        }

        match &self.nodes[id] {
            BPlusNode::Leaf(l) => {
                // keys & values match
                assert_eq!(l.keys.len(), l.values.len());

                // sorted keys
                assert!(l.keys.windows(2).all(|w| w[0] <= w[1]));

                // size bound
                assert!(l.keys.len() <= self.max_keys());

                // next pointer validity (optional)
                if let Some(next) = l.next {
                    assert!(matches!(self.nodes[next], BPlusNode::Leaf(_)));
                }
            }

            BPlusNode::Internal(i) => {
                if id == self.root {
                    // root special case
                    if i.keys.is_empty() {
                        assert_eq!(i.children.len(), 1);
                    } else {
                        assert_eq!(i.children.len(), i.keys.len() + 1);
                    }
                } else {
                    assert_eq!(i.children.len(), i.keys.len() + 1);
                    assert!(i.keys.len() >= self.min_keys());
                }

                // sorted keys
                assert!(i.keys.windows(2).all(|w| w[0] < w[1]));

                // size bound
                assert!(i.keys.len() <= self.max_keys());

                // recurse into children
                for &child in &i.children {
                    self.assert_node(child, visited);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::page::PageId;

    use super::*;

    fn rid(n: u64) -> RowId {
        RowId {
            page_id: PageId(0),
            slot_id: n as u16,
        }
    }

    #[test]
    fn insert_and_get_single_key() {
        let mut tree = BPlusTree::new(4);

        tree.insert(IndexKey::Int64(10), rid(1));
        tree.insert(IndexKey::Int64(10), rid(2));

        let rids = tree.get(&IndexKey::Int64(10));
        assert_eq!(rids.len(), 2);
        assert!(rids.contains(&rid(1)));
        assert!(rids.contains(&rid(2)));
    }

    #[test]
    fn leaf_split_and_lookup() {
        let mut tree = BPlusTree::new(3);

        for i in 0..10 {
            tree.insert(IndexKey::Int64(i), rid(i as u64));
        }

        for i in 0..10 {
            let r = tree.get(&IndexKey::Int64(i));
            assert_eq!(r, vec![rid(i as u64)]);
        }
    }

    #[test]
    fn range_query_across_leaves() {
        let mut tree = BPlusTree::new(3);

        for i in 0..20 {
            tree.insert(IndexKey::Int64(i), rid(i as u64));
        }

        let rids = tree.range(&IndexKey::Int64(5), &IndexKey::Int64(12));
        let expected: Vec<_> = (5..=12).map(|i| rid(i as u64)).collect();

        assert_eq!(rids, expected);
    }

    #[test]
    fn delete_and_rebalance() {
        let mut tree = BPlusTree::new(3);

        for i in 0..10 {
            tree.insert(IndexKey::Int64(i), rid(i as u64));
        }

        for i in 0..10 {
            tree.delete(&IndexKey::Int64(i), rid(i as u64));
        }

        for i in 0..10 {
            assert!(tree.get(&IndexKey::Int64(i)).is_empty());
        }
    }

    #[test]
    fn delete_all_and_reinsert() {
        let mut tree = BPlusTree::new(3);

        for i in 0..20 {
            tree.insert(IndexKey::Int64(i), rid(i as u64));
        }

        for i in 0..20 {
            tree.delete(&IndexKey::Int64(i), rid(i as u64));
        }

        for i in 0..20 {
            tree.insert(IndexKey::Int64(i), rid(i as u64));
        }

        for i in 0..20 {
            assert_eq!(tree.get(&IndexKey::Int64(i)), vec![rid(i as u64)]);
        }
    }

    #[test]
    fn random_insert_delete_stress() {
        use rand::{rng, seq::SliceRandom};

        let mut tree = BPlusTree::new(4);
        let mut keys: Vec<i64> = (0..200).collect();

        for k in &keys {
            tree.insert(IndexKey::Int64(*k), rid(*k as u64));
        }

        keys.shuffle(&mut rng());

        for k in &keys {
            tree.delete(&IndexKey::Int64(*k), rid(*k as u64));
        }

        for k in 0..200 {
            assert!(tree.get(&IndexKey::Int64(k)).is_empty());
        }
    }
}
