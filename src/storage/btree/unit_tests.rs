#[cfg(test)]
mod tests {
    use crate::storage::btree::node::IndexKey;

    #[test]
    fn index_key_roundtrip() {
        let keys = vec![
            IndexKey::Int64(42),
            IndexKey::Int64(-7),
            IndexKey::String("abc".into()),
            IndexKey::String("longer_index_key".into()),
        ];

        for k in keys {
            let mut buf = Vec::new();
            k.serialize(&mut buf);

            let mut slice = buf.as_slice();
            let k2 = IndexKey::deserialize(&mut slice);

            assert_eq!(k, k2);
            assert!(slice.is_empty());
        }
    }
    use std::sync::{Arc, Mutex};

    use crate::{
        buffer::buffer_pool::BufferPool,
        storage::{page::PageId, page_manager::FilePageManager},
    };

    use super::*;

    fn rid(n: u64) -> RowId {
        RowId {
            page_id: PageId(0),
            slot_id: n as u16,
        }
    }

    #[test]
    fn insert_and_get_single_key() {
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut tree = DiskBPlusTree::new(4, bp);

        tree.insert(IndexKey::Int64(10), rid(1));
        tree.insert(IndexKey::Int64(10), rid(2));

        let rids = tree.get(&IndexKey::Int64(10));
        assert_eq!(rids.len(), 2);
        assert!(rids.contains(&rid(1)));
        assert!(rids.contains(&rid(2)));
    }

    #[test]
    fn leaf_split_and_lookup() {
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut tree = DiskBPlusTree::new(3, bp);

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
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut tree = DiskBPlusTree::new(3, bp);

        for i in 0..20 {
            tree.insert(IndexKey::Int64(i), rid(i as u64));
        }

        let rids = tree.range(&IndexKey::Int64(5), &IndexKey::Int64(12));
        let expected: Vec<_> = (5..=12).map(|i| rid(i as u64)).collect();

        assert_eq!(rids, expected);
    }

    #[test]
    fn delete_and_rebalance() {
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut tree = DiskBPlusTree::new(3, bp);

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
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut tree = DiskBPlusTree::new(3, bp);

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
        let path = format!("/tmp/db_{}.db", rand::random::<u64>());
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open(&path).unwrap(),
        ))));

        let mut tree = DiskBPlusTree::new(4, bp);

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

    #[test]
    fn disk_btree_insert_lookup() {
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open("/tmp/btree.db").unwrap(),
        ))));

        let mut tree = DiskBPlusTree::new(4, bp.clone());

        tree.insert(
            IndexKey::Int64(10),
            RowId {
                page_id: PageId(1),
                slot_id: 0,
            },
        );
        tree.insert(
            IndexKey::Int64(20),
            RowId {
                page_id: PageId(1),
                slot_id: 1,
            },
        );

        let r = tree.search(&IndexKey::Int64(10));
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn disk_btree_basic() {
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open("/tmp/db.db").unwrap(),
        ))));

        let mut tree = DiskBPlusTree::new(4, bp);

        tree.insert(
            IndexKey::Int64(10),
            RowId {
                page_id: PageId(1),
                slot_id: 1,
            },
        );
        tree.insert(
            IndexKey::Int64(20),
            RowId {
                page_id: PageId(1),
                slot_id: 2,
            },
        );
        tree.insert(
            IndexKey::Int64(30),
            RowId {
                page_id: PageId(1),
                slot_id: 3,
            },
        );

        assert_eq!(tree.get(&IndexKey::Int64(20)).len(), 1);
    }

    #[test]
    fn delete_rebalance_disk_tree() {
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open("/tmp/db.db").unwrap(),
        ))));

        let mut tree = DiskBPlusTree::new(4, bp);

        for i in 0..20 {
            tree.insert(
                IndexKey::Int64(i),
                RowId {
                    page_id: PageId(1),
                    slot_id: i as u16,
                },
            );
        }

        for i in 0..20 {
            tree.delete(
                &IndexKey::Int64(i),
                RowId {
                    page_id: PageId(1),
                    slot_id: i as u16,
                },
            );
        }

        for i in 0..20 {
            assert!(tree.search(&IndexKey::Int64(i)).is_empty());
        }
    }

    #[test]
    fn insert_split_disk_tree() {
        let bp = Arc::new(Mutex::new(BufferPool::new(Box::new(
            FilePageManager::open("/tmp/db.db").unwrap(),
        ))));

        let mut tree = DiskBPlusTree::new(4, bp);

        for i in 0..50 {
            tree.insert(
                IndexKey::Int64(i),
                RowId {
                    page_id: PageId(1),
                    slot_id: i as u16,
                },
            );
        }

        for i in 0..50 {
            assert!(!tree.search(&IndexKey::Int64(i)).is_empty());
        }
    }
}
