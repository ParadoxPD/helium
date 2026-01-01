#[cfg(test)]
mod tests {
    use crate::{
        buffer::frame::PAGE_SIZE,
        common::value::Value,
        storage::page::{Page, PageId, RowPage},
    };

    #[test]
    fn page_respects_capacity() {
        let mut page = RowPage::new(PageId(1), 2);

        assert!(page.insert(vec![Value::Int64(1)]).is_some());
        assert!(page.insert(vec![Value::Int64(2)]).is_some());

        assert!(page.is_full());
        assert!(page.insert(vec![Value::Int64(3)]).is_none());
    }

    #[test]
    fn page_capacity_and_count() {
        let mut page = RowPage::new(PageId(0), 2);

        assert_eq!(page.capacity(), 2);
        assert_eq!(page.num_rows(), 0);
        assert!(!page.is_full());

        page.insert(vec![Value::Int64(1)]).unwrap();
        assert_eq!(page.num_rows(), 1);
        assert!(!page.is_full());

        page.insert(vec![Value::Int64(2)]).unwrap();
        assert_eq!(page.num_rows(), 2);
        assert!(page.is_full());
    }

    #[test]
    fn get_row_returns_correct_slot() {
        let mut page = RowPage::new(PageId(0), 1);

        let rid = page.insert(vec![Value::Int64(42)]).unwrap();
        let row = page.get_row(rid.slot_id).unwrap();

        assert_eq!(row.values[0], Value::Int64(42));
    }

    #[test]
    fn row_page_roundtrip() {
        let mut page = RowPage::new(PageId(1), 10);

        let r1 = page.insert(vec![Value::Int64(10)]).unwrap();
        let r2 = page.insert(vec![Value::Int64(20)]).unwrap();

        page.delete(r1.slot_id);

        let data = page.to_data();
        let page2 = RowPage::from_data(data);

        assert!(page2.get(r1.slot_id).is_none());
        assert_eq!(page2.get(r2.slot_id).unwrap().values[0], Value::Int64(20));
    }

    #[test]
    fn row_page_disk_roundtrip() {
        let mut page = RowPage::new(PageId(1), 10);

        let r1 = page.insert(vec![Value::Int64(10)]).unwrap();
        let r2 = page.insert(vec![Value::Int64(20)]).unwrap();
        page.delete(r1.slot_id);

        let mut buf = [0u8; PAGE_SIZE];
        page.write_bytes(&mut buf);

        let page2 = RowPage::from_bytes(PageId(1), &buf);

        assert!(page2.get(r1.slot_id).is_none());
        assert_eq!(page2.get(r2.slot_id).unwrap().values[0], Value::Int64(20));
    }
    use crate::storage::page_manager::{FilePageManager, PageManager};

    #[test]
    fn page_manager_roundtrip() {
        let path = "/tmp/helium_test.db";
        std::fs::remove_file(path).ok();

        let mut pm = FilePageManager::open(path).unwrap();

        let pid = pm.allocate_page();
        {
            let page = pm.fetch_page(pid);
            page.data[0] = 42;
            page.dirty = true;
        }

        pm.flush_all();

        let mut pm2 = FilePageManager::open(path).unwrap();
        let page = pm2.fetch_page(pid);
        assert_eq!(page.data[0], 42);
    }
    use std::sync::{Arc, Mutex};

    use crate::{
        buffer::buffer_pool::BufferPool,
        common::value::Value,
        storage::{
            page::{RowId, StorageRow},
            page_manager::FilePageManager,
            table::{HeapTable, Table},
        },
    };

    #[test]
    fn heap_table_grows_pages() {
        let pm = Box::new(FilePageManager::open("/tmp/heap.db").unwrap());
        let bp = Arc::new(Mutex::new(BufferPool::new(pm)));
        let mut t = HeapTable::new("t".into(), vec!["id".into()], 2, bp);

        t.insert(vec![Value::Int64(1)]);
        t.insert(vec![Value::Int64(2)]);
        t.insert(vec![Value::Int64(3)]);

        assert_eq!(t.pages.lock().unwrap().len(), 2);
    }

    #[test]
    fn heap_cursor_scans_all_rows() {
        let pm = Box::new(FilePageManager::open("/tmp/heap.db").unwrap());
        let bp = Arc::new(Mutex::new(BufferPool::new(pm)));
        let mut t = HeapTable::new("t".into(), vec!["id".into()], 2, bp);

        t.insert(vec![Value::Int64(1)]);
        t.insert(vec![Value::Int64(2)]);
        t.insert(vec![Value::Int64(3)]);

        let table: Arc<dyn Table> = Arc::new(t);
        let mut c = table.scan();
        let mut ids = Vec::new();

        while let Some(row) = c.next() {
            ids.push(row.values[0].clone());
        }

        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn insert_returns_stable_row_ids() {
        let pm = Box::new(FilePageManager::open("/tmp/heap.db").unwrap());
        let bp = Arc::new(Mutex::new(BufferPool::new(pm)));
        let mut table = HeapTable::new("users".into(), vec!["id".into()], 2, bp);

        let r1 = table.insert(vec![Value::Int64(1)]);
        let r2 = table.insert(vec![Value::Int64(2)]);

        assert_ne!(r1, r2);
        assert_eq!(r1.page_id, r2.page_id);
        assert_ne!(r1.slot_id, r2.slot_id);
    }

    #[test]
    fn insert_allocates_new_page_when_full() {
        let pm = Box::new(FilePageManager::open("/tmp/heap.db").unwrap());
        let bp = Arc::new(Mutex::new(BufferPool::new(pm)));
        let mut table = HeapTable::new("users".into(), vec!["id".into()], 1, bp);

        let r1 = table.insert(vec![Value::Int64(1)]);
        let r2 = table.insert(vec![Value::Int64(2)]);

        assert_ne!(r1.page_id, r2.page_id);
    }

    #[test]
    fn delete_hides_row_but_preserves_slot() {
        let pm = Box::new(FilePageManager::open("/tmp/heap.db").unwrap());
        let bp = Arc::new(Mutex::new(BufferPool::new(pm)));
        let mut table = HeapTable::new("users".into(), vec!["id".into()], 2, bp);

        let r1 = table.insert(vec![Value::Int64(1)]);
        let r2 = table.insert(vec![Value::Int64(2)]);

        assert!(table.delete(r1));

        let table: Arc<dyn Table> = Arc::new(table);
        let mut cursor = table.scan();

        let mut seen = Vec::new();
        while let Some(row) = cursor.next() {
            seen.push(row);
        }

        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].values[0], Value::Int64(2));
    }

    #[test]
    fn update_overwrites_row_in_place() {
        let pm = Box::new(FilePageManager::open("/tmp/heap.db").unwrap());
        let bp = Arc::new(Mutex::new(BufferPool::new(pm)));
        let mut table = HeapTable::new("users".into(), vec!["age".into()], 2, bp);

        let rid = table.insert(vec![Value::Int64(20)]);

        assert!(table.update(rid, vec![Value::Int64(21)]));

        let mut cursor = Arc::new(table).scan();
        let r = cursor.next().unwrap();

        assert_eq!(r.values[0], Value::Int64(21));
        assert_eq!(r.rid, rid);
    }

    #[test]
    fn update_fails_on_deleted_row() {
        let pm = Box::new(FilePageManager::open("/tmp/heap.db").unwrap());
        let bp = Arc::new(Mutex::new(BufferPool::new(pm)));
        let mut table = HeapTable::new("users".into(), vec!["x".into()], 1, bp);

        let rid = table.insert(vec![Value::Int64(1)]);
        table.delete(rid);

        assert!(!table.update(rid, vec![Value::Int64(2)]));
    }

    #[test]
    fn update_preserves_slot_id() {
        let pm = Box::new(FilePageManager::open("/tmp/heap.db").unwrap());
        let bp = Arc::new(Mutex::new(BufferPool::new(pm)));
        let mut table = HeapTable::new("t".into(), vec!["x".into()], 1, bp);

        let rid = table.insert(vec![Value::Int64(1)]);
        table.update(rid, vec![Value::Int64(99)]);

        let mut cursor = Arc::new(table).scan();
        let r = cursor.next().unwrap();

        assert_eq!(r.rid.slot_id, rid.slot_id);
    }

    #[test]
    fn heap_table_disk_roundtrip() {
        let pm = Box::new(FilePageManager::open("/tmp/heap.db").unwrap());
        let bp = Arc::new(Mutex::new(BufferPool::new(pm)));

        let mut table = HeapTable::new("t".into(), vec!["x".into()], 4, bp);

        let r1 = table.insert(vec![Value::Int64(1)]);
        let r2 = table.insert(vec![Value::Int64(2)]);

        table.delete(r1);

        let v = table.fetch(r2);
        assert_eq!(v.values[0], Value::Int64(2));
    }

    #[test]
    fn buffer_pool_caches_pages() {
        let pm = Box::new(FilePageManager::open("/tmp/bp.db").unwrap());
        let mut bp = BufferPool::new(pm);

        let pid = bp.pm.allocate_page();

        {
            let page = bp.fetch_page(pid);
            page.data[0] = 99;
            bp.unpin_page(pid, true);
        }

        bp.flush_all();

        let page = bp.fetch_page(pid);
        assert_eq!(page.data[0], 99);
        bp.unpin_page(pid, false);
    }
}
