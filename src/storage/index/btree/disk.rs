use crate::storage::{
    buffer::{frame::PageFrame, pool::BufferPoolHandle},
    index::btree::{key::IndexKey, node::BTreeNode},
    page::{page_id::PageId, row_id::RowId},
};

pub struct BPlusTree {
    root: PageId,
    order: usize,
    bp: BufferPoolHandle,
}

impl BPlusTree {
    pub fn new(order: usize, bp: BufferPoolHandle) -> Self {
        assert!(order >= 3, "B+Tree order must be ≥ 3");

        // Allocate root page
        let root_pid = {
            let mut pool = bp.lock().unwrap();
            let pid = pool.pm.allocate_page();

            // Initialize root as empty leaf
            let root = BTreeNode::Leaf {
                keys: Vec::new(),
                values: Vec::new(),
                next: None,
            };

            let page = pool.fetch_page(pid);
            Self::serialize_node(&root, page);
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
    fn serialize_node(node: &BTreeNode, page: &mut PageFrame) {
        let data = &mut page.data;
        data.fill(0);

        let mut out = Vec::new();

        match node {
            BTreeNode::Leaf { keys, values, next } => {
                out.push(0); // leaf tag
                out.extend_from_slice(&(keys.len() as u16).to_le_bytes());

                let next_val = next.map(|p| p.0).unwrap_or(u64::MAX);
                out.extend_from_slice(&next_val.to_le_bytes());

                for (k, rids) in keys.iter().zip(values) {
                    k.serialize(&mut out);

                    out.extend_from_slice(&(rids.len() as u16).to_le_bytes());
                    for rid in rids {
                        out.extend_from_slice(&rid.page_id.0.to_le_bytes());
                        out.extend_from_slice(&rid.slot_id.to_le_bytes());
                    }
                }
            }

            BTreeNode::Internal { keys, children } => {
                out.push(1); // internal tag
                out.extend_from_slice(&(keys.len() as u16).to_le_bytes());

                for child in children {
                    out.extend_from_slice(&child.0.to_le_bytes());
                }

                for k in keys {
                    k.serialize(&mut out);
                }
            }
        }

        assert!(out.len() <= data.len());
        data[..out.len()].copy_from_slice(&out);
    }

    // Deserialize node from page
    fn deserialize_node(page: &PageFrame) -> BTreeNode {
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

                BTreeNode::Leaf { keys, values, next }
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

                BTreeNode::Internal { keys, children }
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
                BTreeNode::Leaf { .. } => return node_pid,
                BTreeNode::Internal { keys, children } => {
                    let idx = match keys.binary_search(key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    node_pid = children[idx];
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

            let root = BTreeNode::Internal {
                keys: vec![sep],
                children: vec![self.root, new_child],
            };

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
            BTreeNode::Leaf { keys, values, next } => {
                match keys.binary_search(&key) {
                    Ok(i) => values[i].push(rid),
                    Err(i) => {
                        keys.insert(i, key);
                        values.insert(i, vec![rid]);
                    }
                }

                if keys.len() <= self.max_leaf_keys() {
                    self.write_node(node_id, &node);
                    return None;
                }

                // ---- split leaf ----
                let mid = keys.len() / 2;

                let right_keys = keys.split_off(mid);
                let right_vals = values.split_off(mid);
                let sep = right_keys[0].clone();

                let mut bp = self.bp.lock().unwrap();
                let right_id = bp.pm.allocate_page();
                drop(bp);

                let old_next = *next;
                *next = Some(right_id);

                let right = BTreeNode::Leaf {
                    keys: right_keys,
                    values: right_vals,
                    next: old_next,
                };

                self.write_node(node_id, &node);
                self.write_node(right_id, &right);

                Some((sep, right_id))
            }

            // ---------------- INTERNAL ----------------
            BTreeNode::Internal { keys, children } => {
                let idx = match keys.binary_search(&key) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };

                let child = children[idx];

                if let Some((sep, new_child)) = self.insert_recursive(child, key, rid) {
                    keys.insert(idx, sep);
                    children.insert(idx + 1, new_child);
                } else {
                    self.write_node(node_id, &node);
                    return None;
                }

                if keys.len() <= self.max_internal_keys() {
                    self.write_node(node_id, &node);
                    return None;
                }

                // ---- split internal ----
                let mid = keys.len() / 2;
                let sep = keys[mid].clone();

                let right_keys = keys.split_off(mid + 1);
                let right_children = children.split_off(mid + 1);

                keys.pop(); // remove sep

                let mut bp = self.bp.lock().unwrap();
                let right_id = bp.pm.allocate_page();
                drop(bp);

                let right = BTreeNode::Internal {
                    keys: right_keys,
                    children: right_children,
                };

                self.write_node(node_id, &node);
                self.write_node(right_id, &right);

                Some((sep, right_id))
            }
        }
    }

    pub fn get(&self, key: &IndexKey) -> Vec<RowId> {
        let leaf_pid = self.find_leaf(key);

        let mut pool = self.bp.lock().unwrap();
        let page = pool.fetch_page(leaf_pid);
        let node = Self::deserialize_node(page);
        pool.unpin_page(leaf_pid, false);

        match node {
            BTreeNode::Leaf { keys, values, .. } => match keys.binary_search(key) {
                Ok(i) => values[i].clone(),
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

            let (keys, values, next) = match node {
                BTreeNode::Leaf { keys, values, next } => (keys, values, next),
                _ => unreachable!(),
            };

            for (k, rids) in keys.iter().zip(&values) {
                if k > to {
                    return out;
                }
                if k >= from {
                    out.extend_from_slice(rids);
                }
            }

            match next {
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
            if let BTreeNode::Internal { children, .. } = root_node {
                if children.len() == 1 {
                    self.root = children[0];
                }
            }
        }
    }

    fn delete_recursive(&mut self, node_id: PageId, key: &IndexKey, rid: RowId) -> bool {
        let mut node = self.load_node(node_id);

        match &mut node {
            BTreeNode::Leaf { keys, values, .. } => {
                if let Ok(i) = keys.binary_search(key) {
                    values[i].retain(|r| *r != rid);
                    if values[i].is_empty() {
                        keys.remove(i);
                        values.remove(i);
                    }
                }

                let underflow = keys.len() < self.min_leaf_keys();
                self.write_node(node_id, &node);
                underflow
            }

            BTreeNode::Internal { keys, children } => {
                let idx = match keys.binary_search(key) {
                    Ok(i) => i + 1,
                    Err(i) => i,
                };

                let child = children[idx];

                // Drop the node to release the borrow before recursive call
                drop(node);

                let child_underflow = self.delete_recursive(child, key, rid);

                if child_underflow {
                    self.rebalance_internal(node_id, idx);
                }

                // Reload the node after potential rebalancing
                let node = self.load_node(node_id);
                let underflow = match &node {
                    BTreeNode::Internal { keys, .. } => keys.len() < self.min_internal_keys(),
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
            BTreeNode::Internal { children, .. } => (children[idx - 1], children[idx]),
            _ => unreachable!(),
        };

        let mut left = self.load_node(left_id);
        let mut cur = self.load_node(cur_id);
        let mut parent = parent;

        match (&mut parent, &mut left, &mut cur) {
            (
                BTreeNode::Internal { keys: p_keys, .. },
                BTreeNode::Leaf {
                    keys: l_keys,
                    values: l_values,
                    ..
                },
                BTreeNode::Leaf {
                    keys: c_keys,
                    values: c_values,
                    ..
                },
            ) => {
                c_keys.insert(0, l_keys.pop().unwrap());
                c_values.insert(0, l_values.pop().unwrap());
                p_keys[idx - 1] = c_keys[0].clone();
            }

            (
                BTreeNode::Internal { keys: p_keys, .. },
                BTreeNode::Internal {
                    keys: l_keys,
                    children: l_children,
                },
                BTreeNode::Internal {
                    keys: c_keys,
                    children: c_children,
                },
            ) => {
                c_keys.insert(0, p_keys[idx - 1].clone());
                p_keys[idx - 1] = l_keys.pop().unwrap();
                c_children.insert(0, l_children.pop().unwrap());
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
            BTreeNode::Internal { children, .. } => (children[idx], children[idx + 1]),
            _ => unreachable!(),
        };

        let mut cur = self.load_node(cur_id);
        let mut right = self.load_node(right_id);
        let mut parent = parent;

        match (&mut parent, &mut cur, &mut right) {
            (
                BTreeNode::Internal { keys: p_keys, .. },
                BTreeNode::Leaf {
                    keys: c_keys,
                    values: c_values,
                    ..
                },
                BTreeNode::Leaf {
                    keys: r_keys,
                    values: r_values,
                    ..
                },
            ) => {
                c_keys.push(r_keys.remove(0));
                c_values.push(r_values.remove(0));
                p_keys[idx] = r_keys[0].clone();
            }

            (
                BTreeNode::Internal { keys: p_keys, .. },
                BTreeNode::Internal {
                    keys: c_keys,
                    children: c_children,
                },
                BTreeNode::Internal {
                    keys: r_keys,
                    children: r_children,
                },
            ) => {
                c_keys.push(p_keys[idx].clone());
                p_keys[idx] = r_keys.remove(0);
                c_children.push(r_children.remove(0));
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
            BTreeNode::Internal { keys, children } => {
                let left_id = children[idx];
                let right_id = children[idx + 1];
                let sep = keys.remove(idx);
                children.remove(idx + 1);
                (left_id, right_id, sep)
            }
            _ => unreachable!(),
        };

        let mut left = self.load_node(left_id);
        let right = self.load_node(right_id);

        match (&mut left, right) {
            (
                BTreeNode::Leaf {
                    keys: l_keys,
                    values: l_values,
                    next: l_next,
                },
                BTreeNode::Leaf {
                    keys: r_keys,
                    values: r_values,
                    next: r_next,
                },
            ) => {
                l_keys.extend(r_keys);
                l_values.extend(r_values);
                *l_next = r_next;
            }

            (
                BTreeNode::Internal {
                    keys: l_keys,
                    children: l_children,
                },
                BTreeNode::Internal {
                    keys: r_keys,
                    children: r_children,
                },
            ) => {
                l_keys.push(sep);
                l_keys.extend(r_keys);
                l_children.extend(r_children);
            }

            _ => unreachable!(),
        }

        self.write_node(left_id, &left);
        self.write_node(parent_id, &parent);
    }

    fn rebalance_internal(&mut self, parent_id: PageId, child_idx: usize) {
        let parent = self.load_node(parent_id);

        let children_len = match &parent {
            BTreeNode::Internal { children, .. } => children.len(),
            _ => unreachable!(),
        };

        // Try borrow from left
        if child_idx > 0 {
            let left_id = match &parent {
                BTreeNode::Internal { children, .. } => children[child_idx - 1],
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
                BTreeNode::Internal { children, .. } => children[child_idx + 1],
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
            BTreeNode::Leaf { keys, .. } => keys.len() > self.min_leaf_keys(),
            BTreeNode::Internal { keys, .. } => keys.len() > self.min_internal_keys(),
        }
    }

    fn load_node(&self, pid: PageId) -> BTreeNode {
        let mut bp = self.bp.lock().unwrap();
        let frame = bp.fetch_page(pid);
        let node = Self::deserialize_node(frame);
        bp.unpin_page(pid, false);
        node
    }

    fn write_node(&self, pid: PageId, node: &BTreeNode) {
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
                BTreeNode::Leaf { keys, values, .. } => {
                    return match keys.binary_search(key) {
                        Ok(i) => values[i].clone(),
                        Err(_) => Vec::new(),
                    };
                }

                BTreeNode::Internal { keys, children } => {
                    let idx = match keys.binary_search(key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };
                    current = children[idx];
                }
            }
        }
    }

    fn min_leaf_keys(&self) -> usize {
        (self.order + 1) / 2
    }

    fn min_internal_keys(&self) -> usize {
        (self.order + 1) / 2 - 1
    }

    fn max_leaf_keys(&self) -> usize {
        self.order
    }

    fn max_internal_keys(&self) -> usize {
        self.order - 1
    }

    pub fn flush(&self) {
        let mut pool = self.bp.lock().unwrap();
        pool.flush_all();
    }
}
