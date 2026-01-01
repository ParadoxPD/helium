pub mod cursor;
pub mod disk;
pub mod internal;
pub mod leaf;
pub mod node;
pub mod unit_tests;

use crate::{
    buffer::{buffer_pool::BufferPoolHandle, frame::PageFrame},
    storage::{
        btree::{
            internal::DiskInternalNode,
            leaf::DiskLeafNode,
            node::{DiskBPlusNode, Index, IndexKey},
        },
        page::{PageId, RowId},
    },
};

pub struct DiskBPlusTree {
    root: PageId,
    order: usize,
    bp: BufferPoolHandle,
}

impl DiskBPlusTree {
    pub fn new(order: usize, bp: BufferPoolHandle) -> Self {
        assert!(order >= 3, "B+Tree order must be ≥ 3");

        // Allocate root page
        let root_pid = {
            let mut pool = bp.lock().unwrap();
            let pid = pool.pm.allocate_page();

            // Initialize root as empty leaf
            let root = DiskLeafNode {
                keys: Vec::new(),
                values: Vec::new(),
                next: None,
            };

            let page = pool.fetch_page(pid);
            Self::serialize_node(&DiskBPlusNode::Leaf(root), page);
            pool.unpin_page(pid, true);

            pid
        };

        Self {
            root: root_pid,
            order,
            bp,
        }
    }

    // Serialize node to page
    fn serialize_node(node: &DiskBPlusNode, page: &mut PageFrame) {
        let data = &mut page.data;
        data.fill(0);

        let mut out = Vec::new();

        match node {
            DiskBPlusNode::Leaf(leaf) => {
                out.push(0); // leaf tag
                out.extend_from_slice(&(leaf.keys.len() as u16).to_le_bytes());

                let next = leaf.next.map(|p| p.0).unwrap_or(u64::MAX);
                out.extend_from_slice(&next.to_le_bytes());

                for (k, rids) in leaf.keys.iter().zip(&leaf.values) {
                    k.serialize(&mut out);

                    out.extend_from_slice(&(rids.len() as u16).to_le_bytes());
                    for rid in rids {
                        out.extend_from_slice(&rid.page_id.0.to_le_bytes());
                        out.extend_from_slice(&rid.slot_id.to_le_bytes());
                    }
                }
            }

            DiskBPlusNode::Internal(internal) => {
                out.push(1); // internal tag
                out.extend_from_slice(&(internal.keys.len() as u16).to_le_bytes());

                for child in &internal.children {
                    out.extend_from_slice(&child.0.to_le_bytes());
                }

                for k in &internal.keys {
                    k.serialize(&mut out);
                }
            }
        }

        assert!(out.len() <= data.len());
        data[..out.len()].copy_from_slice(&out);
    }
    // Deserialize node from page
    fn deserialize_node(page: &PageFrame) -> DiskBPlusNode {
        let mut input = &page.data[..];

        let tag = input[0];
        input = &input[1..];

        let key_count = u16::from_le_bytes(input[..2].try_into().unwrap()) as usize;
        input = &input[2..];

        match tag {
            0 => {
                let next_raw = u64::from_le_bytes(input[..8].try_into().unwrap());
                input = &input[8..];

                let next = if next_raw == u64::MAX {
                    None
                } else {
                    Some(PageId(next_raw))
                };

                let mut keys = Vec::with_capacity(key_count);
                let mut values = Vec::with_capacity(key_count);

                for _ in 0..key_count {
                    let key = IndexKey::deserialize(&mut input);
                    let rid_count = u16::from_le_bytes(input[..2].try_into().unwrap()) as usize;
                    input = &input[2..];

                    let mut rids = Vec::with_capacity(rid_count);
                    for _ in 0..rid_count {
                        let pid = u64::from_le_bytes(input[..8].try_into().unwrap());
                        let sid = u16::from_le_bytes(input[8..10].try_into().unwrap());
                        input = &input[10..];

                        rids.push(RowId {
                            page_id: PageId(pid),
                            slot_id: sid,
                        });
                    }

                    keys.push(key);
                    values.push(rids);
                }

                DiskBPlusNode::Leaf(DiskLeafNode { keys, values, next })
            }

            1 => {
                let mut children = Vec::with_capacity(key_count + 1);
                for _ in 0..key_count + 1 {
                    let pid = u64::from_le_bytes(input[..8].try_into().unwrap());
                    input = &input[8..];
                    children.push(PageId(pid));
                }

                let mut keys = Vec::with_capacity(key_count);
                for _ in 0..key_count {
                    keys.push(IndexKey::deserialize(&mut input));
                }

                DiskBPlusNode::Internal(DiskInternalNode { keys, children })
            }

            _ => panic!("invalid B+Tree node tag"),
        }
    }

    fn find_leaf(&self, key: &IndexKey) -> PageId {
        let mut node_pid = self.root;

        loop {
            let mut pool = self.bp.lock().unwrap();
            let page = pool.fetch_page(node_pid);
            let node = Self::deserialize_node(page);
            pool.unpin_page(node_pid, false);

            match node {
                DiskBPlusNode::Leaf(_) => return node_pid,
                DiskBPlusNode::Internal(internal) => {
                    let idx = match internal.keys.binary_search(key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    node_pid = internal.children[idx];
                }
            }
        }
    }

    pub fn insert(&mut self, key: IndexKey, rid: RowId) {
        if let Some((sep, new_child)) = self.insert_recursive(self.root, key, rid) {
            // Root split
            let mut bp = self.bp.lock().unwrap();
            let new_root = bp.pm.allocate_page();
            drop(bp);

            let root = DiskBPlusNode::Internal(DiskInternalNode {
                keys: vec![sep],
                children: vec![self.root, new_child],
            });

            self.write_node(new_root, &root);
            self.root = new_root;
        }
    }

    fn insert_recursive(
        &mut self,
        node_id: PageId,
        key: IndexKey,
        rid: RowId,
    ) -> Option<(IndexKey, PageId)> {
        let mut node = self.load_node(node_id);

        match &mut node {
            // ---------------- LEAF ----------------
            DiskBPlusNode::Leaf(leaf) => {
                match leaf.keys.binary_search(&key) {
                    Ok(i) => leaf.values[i].push(rid),
                    Err(i) => {
                        leaf.keys.insert(i, key);
                        leaf.values.insert(i, vec![rid]);
                    }
                }

                if leaf.keys.len() <= self.max_leaf_keys() {
                    self.write_node(node_id, &node);
                    return None;
                }

                // ---- split leaf ----
                let mid = leaf.keys.len() / 2;

                let right_keys = leaf.keys.split_off(mid);
                let right_vals = leaf.values.split_off(mid);
                let sep = right_keys[0].clone();

                let mut bp = self.bp.lock().unwrap();
                let right_id = bp.pm.allocate_page();
                drop(bp);

                let right = DiskBPlusNode::Leaf(DiskLeafNode {
                    keys: right_keys,
                    values: right_vals,
                    next: leaf.next,
                });

                leaf.next = Some(right_id);

                self.write_node(node_id, &node);
                self.write_node(right_id, &right);

                return Some((sep, right_id));
            }

            // ---------------- INTERNAL ----------------
            DiskBPlusNode::Internal(internal) => {
                let idx = match internal.keys.binary_search(&key) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };

                let child = internal.children[idx];

                if let Some((sep, new_child)) = self.insert_recursive(child, key, rid) {
                    internal.keys.insert(idx, sep);
                    internal.children.insert(idx + 1, new_child);
                } else {
                    self.write_node(node_id, &node);
                    return None;
                }

                if internal.keys.len() <= self.max_internal_keys() {
                    self.write_node(node_id, &node);
                    return None;
                }

                // ---- split internal ----
                let mid = internal.keys.len() / 2;
                let sep = internal.keys[mid].clone();

                let right_keys = internal.keys.split_off(mid + 1);
                let right_children = internal.children.split_off(mid + 1);

                internal.keys.pop(); // remove sep

                let mut bp = self.bp.lock().unwrap();
                let right_id = bp.pm.allocate_page();
                drop(bp);

                let right = DiskBPlusNode::Internal(DiskInternalNode {
                    keys: right_keys,
                    children: right_children,
                });

                self.write_node(node_id, &node);
                self.write_node(right_id, &right);

                return Some((sep, right_id));
            }
        }
    }

    fn _split_leaf(&mut self, leaf_pid: PageId) -> (IndexKey, PageId) {
        let mut pool = self.bp.lock().unwrap();

        let page = pool.fetch_page(leaf_pid);
        let mut node = Self::deserialize_node(page);
        pool.unpin_page(leaf_pid, false);

        let leaf = match &mut node {
            DiskBPlusNode::Leaf(l) => l,
            _ => unreachable!(),
        };

        let split_at = leaf.keys.len() / 2;

        let new_keys = leaf.keys.split_off(split_at);
        let new_values = leaf.values.split_off(split_at);
        let separator = new_keys[0].clone();

        let new_leaf_pid = pool.pm.allocate_page();

        let new_leaf = DiskLeafNode {
            keys: new_keys,
            values: new_values,
            next: leaf.next.take(),
        };

        leaf.next = Some(new_leaf_pid);

        // Write original leaf
        let page = pool.fetch_page(leaf_pid);
        Self::serialize_node(&node, page);
        pool.unpin_page(leaf_pid, true);

        // Write new leaf
        let page = pool.fetch_page(new_leaf_pid);
        Self::serialize_node(&DiskBPlusNode::Leaf(new_leaf), page);
        pool.unpin_page(new_leaf_pid, true);

        drop(pool);

        (separator, new_leaf_pid)
    }

    fn _split_internal(&mut self, node_pid: PageId) -> (IndexKey, PageId) {
        let mut pool = self.bp.lock().unwrap();

        let page = pool.fetch_page(node_pid);
        let mut node = Self::deserialize_node(page);
        pool.unpin_page(node_pid, false);

        let internal = match &mut node {
            DiskBPlusNode::Internal(i) => i,
            _ => unreachable!(),
        };

        let mid = internal.keys.len() / 2;
        let separator = internal.keys.remove(mid);

        let right_keys = internal.keys.split_off(mid);
        let right_children = internal.children.split_off(mid + 1);

        let new_internal_pid = pool.pm.allocate_page();

        let new_internal = DiskInternalNode {
            keys: right_keys,
            children: right_children,
        };

        // Write original internal
        let page = pool.fetch_page(node_pid);
        Self::serialize_node(&node, page);
        pool.unpin_page(node_pid, true);

        // Write new internal
        let page = pool.fetch_page(new_internal_pid);
        Self::serialize_node(&DiskBPlusNode::Internal(new_internal), page);
        pool.unpin_page(new_internal_pid, true);

        drop(pool);

        (separator, new_internal_pid)
    }

    pub fn get(&self, key: &IndexKey) -> Vec<RowId> {
        let leaf_pid = self.find_leaf(key);

        let mut pool = self.bp.lock().unwrap();
        let page = pool.fetch_page(leaf_pid);
        let node = Self::deserialize_node(page);
        pool.unpin_page(leaf_pid, false);

        match node {
            DiskBPlusNode::Leaf(leaf) => match leaf.keys.binary_search(key) {
                Ok(i) => leaf.values[i].clone(),
                Err(_) => Vec::new(),
            },
            _ => unreachable!(),
        }
    }

    pub fn range(&self, from: &IndexKey, to: &IndexKey) -> Vec<RowId> {
        let mut out = Vec::new();
        let mut node_pid = self.find_leaf(from);

        loop {
            let mut pool = self.bp.lock().unwrap();
            let page = pool.fetch_page(node_pid);
            let node = Self::deserialize_node(page);
            pool.unpin_page(node_pid, false);
            drop(pool);

            let leaf = match node {
                DiskBPlusNode::Leaf(l) => l,
                _ => unreachable!(),
            };

            for (k, rids) in leaf.keys.iter().zip(&leaf.values) {
                if k > to {
                    return out;
                }
                if k >= from {
                    out.extend_from_slice(rids);
                }
            }

            match leaf.next {
                Some(next_pid) => node_pid = next_pid,
                None => break,
            }
        }

        out
    }

    pub fn delete(&mut self, key: &IndexKey, rid: RowId) {
        let underflow = self.delete_recursive(self.root, key, rid);

        if underflow {
            let root_node = self.load_node(self.root);
            if let DiskBPlusNode::Internal(internal) = root_node {
                if internal.children.len() == 1 {
                    self.root = internal.children[0];
                }
            }
        }
    }

    fn delete_recursive(&mut self, node_id: PageId, key: &IndexKey, rid: RowId) -> bool {
        let mut node = self.load_node(node_id);

        match &mut node {
            DiskBPlusNode::Leaf(leaf) => {
                if let Ok(i) = leaf.keys.binary_search(key) {
                    leaf.values[i].retain(|r| *r != rid);
                    if leaf.values[i].is_empty() {
                        leaf.keys.remove(i);
                        leaf.values.remove(i);
                    }
                }

                let underflow = leaf.keys.len() < self.min_leaf_keys();
                self.write_node(node_id, &node);
                return underflow;
            }

            DiskBPlusNode::Internal(internal) => {
                let idx = match internal.keys.binary_search(key) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };

                let child = internal.children[idx];

                // Drop the node to release the borrow before recursive call
                drop(node);

                let child_underflow = self.delete_recursive(child, key, rid);

                if child_underflow {
                    self.rebalance_internal(node_id, idx);
                }

                // Reload the node after potential rebalancing
                let node = self.load_node(node_id);
                let underflow = match &node {
                    DiskBPlusNode::Internal(i) => i.keys.len() < self.min_internal_keys(),
                    _ => unreachable!(),
                };

                // No need to write back - rebalance_internal already did it
                underflow
            }
        }
    }

    fn borrow_from_left(&mut self, parent_id: PageId, idx: usize) {
        let parent = self.load_node(parent_id);

        let (left_id, cur_id) = match &parent {
            DiskBPlusNode::Internal(i) => (i.children[idx - 1], i.children[idx]),
            _ => unreachable!(),
        };

        let mut left = self.load_node(left_id);
        let mut cur = self.load_node(cur_id);
        let mut parent = parent; // Make parent mutable

        match (&mut parent, &mut left, &mut cur) {
            (DiskBPlusNode::Internal(p), DiskBPlusNode::Leaf(l), DiskBPlusNode::Leaf(c)) => {
                c.keys.insert(0, l.keys.pop().unwrap());
                c.values.insert(0, l.values.pop().unwrap());
                p.keys[idx - 1] = c.keys[0].clone();
            }

            (
                DiskBPlusNode::Internal(p),
                DiskBPlusNode::Internal(l),
                DiskBPlusNode::Internal(c),
            ) => {
                c.keys.insert(0, p.keys[idx - 1].clone());
                p.keys[idx - 1] = l.keys.pop().unwrap();
                c.children.insert(0, l.children.pop().unwrap());
            }

            _ => unreachable!(),
        }

        self.write_node(left_id, &left);
        self.write_node(cur_id, &cur);
        self.write_node(parent_id, &parent);
    }

    fn borrow_from_right(&mut self, parent_id: PageId, idx: usize) {
        let parent = self.load_node(parent_id);

        let (cur_id, right_id) = match &parent {
            DiskBPlusNode::Internal(i) => (i.children[idx], i.children[idx + 1]),
            _ => unreachable!(),
        };

        let mut cur = self.load_node(cur_id);
        let mut right = self.load_node(right_id);
        let mut parent = parent; // Make parent mutable

        match (&mut parent, &mut cur, &mut right) {
            (DiskBPlusNode::Internal(p), DiskBPlusNode::Leaf(c), DiskBPlusNode::Leaf(r)) => {
                c.keys.push(r.keys.remove(0));
                c.values.push(r.values.remove(0));
                p.keys[idx] = r.keys[0].clone();
            }

            (
                DiskBPlusNode::Internal(p),
                DiskBPlusNode::Internal(c),
                DiskBPlusNode::Internal(r),
            ) => {
                c.keys.push(p.keys[idx].clone());
                p.keys[idx] = r.keys.remove(0);
                c.children.push(r.children.remove(0));
            }

            _ => unreachable!(),
        }

        self.write_node(cur_id, &cur);
        self.write_node(right_id, &right);
        self.write_node(parent_id, &parent);
    }

    fn merge_children(&mut self, parent_id: PageId, idx: usize) {
        let mut parent = self.load_node(parent_id);

        let (left_id, right_id, sep) = match &mut parent {
            DiskBPlusNode::Internal(i) => {
                let left_id = i.children[idx];
                let right_id = i.children[idx + 1];
                let sep = i.keys.remove(idx);
                i.children.remove(idx + 1);
                (left_id, right_id, sep)
            }
            _ => unreachable!(),
        };

        let mut left = self.load_node(left_id);
        let right = self.load_node(right_id);

        match (&mut left, right) {
            (DiskBPlusNode::Leaf(l), DiskBPlusNode::Leaf(r)) => {
                l.keys.extend(r.keys);
                l.values.extend(r.values);
                l.next = r.next;
            }

            (DiskBPlusNode::Internal(l), DiskBPlusNode::Internal(r)) => {
                l.keys.push(sep);
                l.keys.extend(r.keys);
                l.children.extend(r.children);
            }

            _ => unreachable!(),
        }

        self.write_node(left_id, &left);
        self.write_node(parent_id, &parent);
    }

    fn rebalance_internal(&mut self, parent_id: PageId, child_idx: usize) {
        let parent = self.load_node(parent_id);

        let (children_len, _child_id) = match &parent {
            DiskBPlusNode::Internal(i) => (i.children.len(), i.children[child_idx]),
            _ => unreachable!(),
        };

        // Try borrow from left
        if child_idx > 0 {
            let left_id = match &parent {
                DiskBPlusNode::Internal(i) => i.children[child_idx - 1],
                _ => unreachable!(),
            };

            if self.can_lend(left_id) {
                self.borrow_from_left(parent_id, child_idx);
                return;
            }
        }

        // Try borrow from right
        if child_idx + 1 < children_len {
            let right_id = match &parent {
                DiskBPlusNode::Internal(i) => i.children[child_idx + 1],
                _ => unreachable!(),
            };

            if self.can_lend(right_id) {
                self.borrow_from_right(parent_id, child_idx);
                return;
            }
        }

        // Must merge
        if child_idx > 0 {
            self.merge_children(parent_id, child_idx - 1);
        } else {
            self.merge_children(parent_id, child_idx);
        }
    }
    fn can_lend(&self, node_id: PageId) -> bool {
        match self.load_node(node_id) {
            DiskBPlusNode::Leaf(l) => l.keys.len() > self.min_leaf_keys(),
            DiskBPlusNode::Internal(i) => i.keys.len() > self.min_internal_keys(),
        }
    }

    fn load_node(&self, pid: PageId) -> DiskBPlusNode {
        let mut bp = self.bp.lock().unwrap();
        let frame = bp.fetch_page(pid);
        let node = Self::deserialize_node(frame);
        bp.unpin_page(pid, false);
        node
    }

    fn write_node(&self, pid: PageId, node: &DiskBPlusNode) {
        let mut bp = self.bp.lock().unwrap();
        let frame = bp.fetch_page(pid);
        Self::serialize_node(node, frame);
        bp.unpin_page(pid, true);
    }

    pub fn search(&self, key: &IndexKey) -> Vec<RowId> {
        let mut current = self.root;

        loop {
            let node = self.load_node(current);

            match node {
                DiskBPlusNode::Leaf(leaf) => {
                    return match leaf.keys.binary_search(key) {
                        Ok(i) => leaf.values[i].clone(),
                        Err(_) => Vec::new(),
                    };
                }

                DiskBPlusNode::Internal(internal) => {
                    let idx = match internal.keys.binary_search(key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    current = internal.children[idx];
                }
            }
        }
    }

    fn min_leaf_keys(&self) -> usize {
        // ceil(order / 2)
        (self.order + 1) / 2
    }

    fn min_internal_keys(&self) -> usize {
        // ceil(order / 2) - 1
        (self.order + 1) / 2 - 1
    }

    fn max_leaf_keys(&self) -> usize {
        self.order
    }

    fn max_internal_keys(&self) -> usize {
        self.order - 1
    }

    fn _max_keys(&self) -> usize {
        self.order - 1
    }

    fn _min_keys(&self) -> usize {
        (self.order - 1) / 2
    }

    pub fn flush(&self) {
        let mut pool = self.bp.lock().unwrap();
        pool.flush_all();
    }
}

impl Index for DiskBPlusTree {
    fn insert(&mut self, key: IndexKey, rid: RowId) {
        DiskBPlusTree::insert(self, key, rid);
    }

    fn delete(&mut self, key: &IndexKey, rid: RowId) {
        DiskBPlusTree::delete(self, key, rid);
    }

    fn get(&self, key: &IndexKey) -> Vec<RowId> {
        DiskBPlusTree::get(self, key)
    }

    fn range(&self, from: &IndexKey, to: &IndexKey) -> Vec<RowId> {
        DiskBPlusTree::range(self, from, to)
    }
}
