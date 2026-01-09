use crate::storage::{
    heap::heap_table::HeapTable, heap::storage_row::StorageRow, page::row_id::RowId,
};

pub struct HeapCursor<'a> {
    table: &'a HeapTable,
    page_idx: usize,
    slot_idx: u16,
}

impl<'a> HeapCursor<'a> {
    pub fn new(table: &'a HeapTable) -> Self {
        Self {
            table,
            page_idx: 0,
            slot_idx: 0,
        }
    }
}

impl<'a> Iterator for HeapCursor<'a> {
    type Item = (RowId, StorageRow);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let pages = self.table.pages.lock().unwrap();
            if self.page_idx >= pages.len() {
                return None;
            }

            let pid = pages[self.page_idx];
            drop(pages);

            let mut bp = self.table.bp.lock().unwrap();
            let frame = bp.fetch_page(pid);
            let page = RowPage::from_bytes(pid, &frame.data);
            bp.unpin_page(pid, false);

            while self.slot_idx < page.capacity() as u16 {
                let slot = self.slot_idx;
                self.slot_idx += 1;

                if let Some(row) = page.get(slot) {
                    return Some((RowId { page_id: pid, slot }, row.clone()));
                }
            }

            self.page_idx += 1;
            self.slot_idx = 0;
        }
    }
}
