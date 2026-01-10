use crate::storage::{
    errors::{StorageError, StorageResult},
    heap::heap_table::HeapTable,
    page::{page_id::PageId, row::StorageRow, row_id::RowId, row_page::RowPage},
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
            let frame = bp.fetch_page(pid).unwrap();
            let page = match RowPage::from_bytes(pid, &frame.data) {
                Ok(p) => p,
                Err(_) => {
                    self.page_idx += 1;
                    self.slot_idx = 0;
                    continue; // skip corrupted page
                }
            };
            bp.unpin_page(pid, false);

            while (self.slot_idx as usize) < page.slots_len() {
                let slot_id = self.slot_idx;
                self.slot_idx += 1;

                if let Ok(row) = page.get(slot_id) {
                    return Some((
                        RowId {
                            page_id: pid,
                            slot_id,
                        },
                        row.clone(),
                    ));
                }
            }

            self.page_idx += 1;
            self.slot_idx = 0;
        }
    }
}
