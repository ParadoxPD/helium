use std::sync::{Arc, Mutex};

use crate::{
    catalog::ids::TableId,
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
    pub(crate) table_id: TableId,
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
            bp.unpin_page(pid, true)?;
        }

        Ok(Self {
            table_id: TableId(0), // Will be set properly when opened
            pages: Mutex::new(vec![pid]),
            page_capacity,
            bp,
        })
    }

    // ADD THIS METHOD
    pub fn open(table_id: TableId, bp: BufferPoolHandle) -> StorageResult<Self> {
        // For now, create a new heap since we don't have persistence yet
        // In Phase 2, this will load pages from catalog metadata
        let mut heap = Self::new(100, bp)?;
        heap.table_id = table_id;
        Ok(heap)
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
