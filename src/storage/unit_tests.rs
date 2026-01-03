#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        buffer::{buffer_pool::BufferPool, frame::PAGE_SIZE},
        common::{schema::Schema, value::Value},
        storage::{
            page::{Page, PageId, RowPage},
            page_manager::{FilePageManager, PageManager},
            table::HeapTable,
        },
    };

    /* ============================================================
     * RowPage tests
     * ============================================================
     */

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
        let row = page.get(rid.slot_id).unwrap();

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

    /* ============================================================
     * PageManager tests
     * ============================================================
     */

    #[test]
    fn page_manager_roundtrip() {
        let path = "/tmp/helium_pm_test.db";
        std::fs::remove_file(path).ok();

        let mut pm = FilePageManager::open(&path.into()).unwrap();

        let pid = pm.allocate_page();
        {
            let page = pm.fetch_page(pid);
            page.data[0] = 42;
            page.dirty = true;
        }

        pm.flush_all();

        let mut pm2 = FilePageManager::open(&path.into()).unwrap();
        let page = pm2.fetch_page(pid);
        assert_eq!(page.data[0], 42);
    }

    /* ============================================================
     * HeapTable tests
     * ============================================================
     */

    fn test_heap(cap: usize) -> HeapTable {
        let pm = Box::new(FilePageManager::open(&"/tmp/heap_test.db".into()).unwrap());
        let bp = Arc::new(Mutex::new(BufferPool::new(pm)));
        HeapTable::new("t".into(), Schema::new(vec!["x".into()]), cap, bp)
    }

    #[test]
    fn heap_table_grows_pages() {
        let mut t = test_heap(2);

        t.insert(vec![Value::Int64(1)]);
        t.insert(vec![Value::Int64(2)]);
        t.insert(vec![Value::Int64(3)]);

        assert_eq!(t.pages.lock().unwrap().len(), 2);
    }

    #[test]
    fn heap_cursor_scans_all_rows() {
        let mut t = test_heap(2);

        t.insert(vec![Value::Int64(1)]);
        t.insert(vec![Value::Int64(2)]);
        t.insert(vec![Value::Int64(3)]);

        let mut cursor = Arc::new(t).scan();
        let mut vals = Vec::new();

        while let Some((_rid, row)) = cursor.next() {
            vals.push(row.values[0].clone());
        }

        assert_eq!(vals.len(), 3);
    }

    #[test]
    fn insert_returns_stable_row_ids() {
        let mut table = test_heap(2);

        let r1 = table.insert(vec![Value::Int64(1)]);
        let r2 = table.insert(vec![Value::Int64(2)]);

        assert_ne!(r1, r2);
        assert_eq!(r1.page_id, r2.page_id);
        assert_ne!(r1.slot_id, r2.slot_id);
    }

    #[test]
    fn insert_allocates_new_page_when_full() {
        let mut table = test_heap(1);

        let r1 = table.insert(vec![Value::Int64(1)]);
        let r2 = table.insert(vec![Value::Int64(2)]);

        assert_ne!(r1.page_id, r2.page_id);
    }

    #[test]
    fn delete_hides_row_but_preserves_slot() {
        let mut table = test_heap(2);

        let r1 = table.insert(vec![Value::Int64(1)]);
        let _r2 = table.insert(vec![Value::Int64(2)]);

        assert!(table.delete(r1));

        let mut cursor = Arc::new(table).scan();
        let mut rows = Vec::new();

        while let Some(row) = cursor.next() {
            rows.push(row);
        }
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1.values[0], Value::Int64(2));
    }

    #[test]
    fn delete_and_insert_preserves_slot_reuse() {
        let mut table = test_heap(1);

        let r1 = table.insert(vec![Value::Int64(1)]);
        table.delete(r1);

        let r2 = table.insert(vec![Value::Int64(2)]);

        assert_eq!(r1.page_id, r2.page_id);
        assert_eq!(r1.slot_id, r2.slot_id);
    }

    #[test]
    fn heap_table_disk_roundtrip() {
        let mut table = test_heap(4);

        let r1 = table.insert(vec![Value::Int64(1)]);
        let r2 = table.insert(vec![Value::Int64(2)]);
        table.delete(r1);

        let row = table.fetch(r2);
        assert_eq!(row.values[0], Value::Int64(2));
    }

    /* ============================================================
     * BufferPool tests
     * ============================================================
     */

    #[test]
    fn buffer_pool_caches_pages() {
        let pm = Box::new(FilePageManager::open(&"/tmp/bp_test.db".into()).unwrap());
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
