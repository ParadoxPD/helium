use std::sync::{Arc, Mutex};

use crate::{
    storage::{
        buffer::pool::BufferPoolHandle,
        heap::storage_row::StorageRow,
        page::row_page::RowPage, // keep your existing RowPage
        page::{PageId, row_id::RowId},
    },
    types::value::Value,
};

pub struct HeapTable {
    pages: Mutex<Vec<PageId>>,
    page_capacity: usize,
    bp: BufferPoolHandle,
}

impl HeapTable {
    pub fn new(page_capacity: usize, bp: BufferPoolHandle) -> Self {
        let pid;
        {
            let mut bp = bp.lock().unwrap();
            pid = bp.pm.allocate_page();

            let page = RowPage::new(pid, page_capacity);
            let frame = bp.fetch_page(pid);
            page.write_bytes(&mut frame.data);
            bp.unpin_page(pid, true);
        }

        Self {
            pages: Mutex::new(vec![pid]),
            page_capacity,
            bp,
        }
    }

    /// Insert a single physical row.
    pub fn insert(&self, values: Vec<Value>) -> RowId {
        let last_pid = {
            let pages = self.pages.lock().unwrap();
            *pages.last().unwrap()
        };

        // Try last page
        {
            let mut bp = self.bp.lock().unwrap();
            let frame = bp.fetch_page(last_pid);
            let mut page = RowPage::from_bytes(last_pid, &frame.data);

            if let Some(rid) = page.insert(values.clone()) {
                page.write_bytes(&mut frame.data);
                bp.unpin_page(last_pid, true);
                return rid;
            }

            bp.unpin_page(last_pid, false);
        }

        // Allocate new page
        let mut bp = self.bp.lock().unwrap();
        let pid = bp.pm.allocate_page();

        let mut page = RowPage::new(pid, self.page_capacity);
        let rid = page.insert(values).unwrap();

        let frame = bp.fetch_page(pid);
        page.write_bytes(&mut frame.data);
        bp.unpin_page(pid, true);

        self.pages.lock().unwrap().push(pid);
        rid
    }

    pub fn delete(&self, rid: RowId) {
        let mut bp = self.bp.lock().unwrap();
        let frame = bp.fetch_page(rid.page_id);
        let mut page = RowPage::from_bytes(rid.page_id, &frame.data);

        let ok = page.delete(rid.slot);
        assert!(ok, "invalid RowId");

        page.write_bytes(&mut frame.data);
        bp.unpin_page(rid.page_id, true);
    }

    pub fn fetch(&self, rid: RowId) -> StorageRow {
        let mut bp = self.bp.lock().unwrap();
        let frame = bp.fetch_page(rid.page_id);
        let page = RowPage::from_bytes(rid.page_id, &frame.data);

        let row = page.get(rid.slot).expect("invalid RowId").clone();

        bp.unpin_page(rid.page_id, false);
        row
    }

    pub fn scan(&self) -> HeapCursor {
        HeapCursor::new(self)
    }
}
