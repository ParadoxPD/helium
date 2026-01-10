use std::sync::{Arc, Mutex};

use crate::{
    storage::{
        buffer::pool::BufferPoolHandle,
        errors::StorageResult,
        heap::heap_cursor::HeapCursor,
        page::{page_id::PageId, row::StorageRow, row_id::RowId, row_page::RowPage}, // keep your existing RowPage
    },
    types::value::Value,
};

pub struct HeapTable {
    pub(crate) pages: Mutex<Vec<PageId>>,
    page_capacity: usize,
    pub(crate) bp: BufferPoolHandle,
}

impl HeapTable {
    pub fn new(page_capacity: usize, bp: BufferPoolHandle) -> StorageResult<Self> {
        let pid;
        {
            let mut bp = bp.lock().unwrap();
            pid = bp.pm.allocate_page();

            let page = RowPage::new(pid, page_capacity);
            let frame = bp.fetch_page(pid)?;
            page.write_bytes(&mut frame.data);
            bp.unpin_page(pid, true);
        }

        Ok(Self {
            pages: Mutex::new(vec![pid]),
            page_capacity,
            bp,
        })
    }

    /// Insert a single physical row.
    pub fn insert(&self, values: Vec<Value>) -> StorageResult<RowId> {
        let last_pid = {
            let pages = self.pages.lock().unwrap();
            *pages.last().unwrap()
        };

        // Try last page
        {
            let mut bp = self.bp.lock().unwrap();
            let frame = bp.fetch_page(last_pid)?;
            let mut page = RowPage::from_bytes(last_pid, &frame.data)?;

            if let Ok(rid) = page.insert(values.clone()) {
                page.write_bytes(&mut frame.data);
                bp.unpin_page(last_pid, true);
                return Ok(rid);
            }

            bp.unpin_page(last_pid, false);
        }

        // Allocate new page
        let mut bp = self.bp.lock().unwrap();
        let pid = bp.pm.allocate_page();

        let mut page = RowPage::new(pid, self.page_capacity);
        let rid = page.insert(values)?;

        let frame = bp.fetch_page(pid)?;
        page.write_bytes(&mut frame.data);
        bp.unpin_page(pid, true);

        self.pages.lock().unwrap().push(pid);
        Ok(rid)
    }

    pub fn delete(&self, rid: RowId) -> StorageResult<()> {
        let mut bp = self.bp.lock().unwrap();
        let frame = bp.fetch_page(rid.page_id)?;
        let mut page = RowPage::from_bytes(rid.page_id, &frame.data)?;

        let ok = page.delete(rid.slot_id);

        page.write_bytes(&mut frame.data);
        bp.unpin_page(rid.page_id, true);
        Ok(())
    }

    pub fn fetch(&self, rid: RowId) -> StorageResult<StorageRow> {
        let mut bp = self.bp.lock().unwrap();
        let frame = bp.fetch_page(rid.page_id)?;
        let page = RowPage::from_bytes(rid.page_id, &frame.data)?;

        let row = page.get(rid.slot_id)?.clone();

        bp.unpin_page(rid.page_id, false);
        Ok(row)
    }

    pub fn scan(&self) -> HeapCursor<'_> {
        HeapCursor::new(self)
    }
}
