use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffer_pool::BufferPoolHandle,
    common::{schema::Schema, value::Value},
    storage::{
        btree::node::Index,
        page::{HeapPageHandle, Page, PageId, RowId, RowPage, StorageRow},
    },
};

pub trait Table: Send + Sync {
    fn scan(self: Arc<Self>) -> Box<dyn TableCursor>;
    fn schema(&self) -> &Schema;
    fn fetch(&self, rid: RowId) -> StorageRow;
    fn get_index(&self, column: &str) -> Option<Arc<dyn Index>>;
    fn delete(&self, rid: RowId) -> bool;
    fn update(&self, rid: RowId, values: Vec<Value>) -> bool;
    fn insert(&self, values: Vec<Value>) -> RowId;
}

pub trait TableCursor {
    fn next(&mut self) -> Option<StorageRow>;
}

pub struct HeapTable {
    name: String,
    schema: Schema,
    pages: Mutex<Vec<PageId>>,
    page_capacity: usize,
    bp: BufferPoolHandle,
}

pub struct HeapTableCursor {
    table: Arc<HeapTable>,
    page_idx: usize,
    slot_idx: u16,
}

impl HeapTable {
    pub fn new(name: String, schema: Schema, page_capacity: usize, bp: BufferPoolHandle) -> Self {
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
            name,
            schema,
            pages: Mutex::new(vec![pid]),
            page_capacity,
            bp,
        }
    }
    pub fn insert_rows(&mut self, rows: Vec<Vec<Value>>) {
        for row in rows {
            self.insert(row);
        }
    }
}

impl Table for HeapTable {
    fn scan(self: Arc<Self>) -> Box<dyn TableCursor> {
        Box::new(HeapTableCursor::new(self))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn fetch(&self, rid: RowId) -> StorageRow {
        let mut bp = self.bp.lock().unwrap();
        let frame = bp.fetch_page(rid.page_id);
        let page = RowPage::from_bytes(rid.page_id, &frame.data);

        let row = page.get(rid.slot_id).expect("invalid RowId").clone();

        bp.unpin_page(rid.page_id, false);
        row
    }

    fn get_index(&self, _column: &str) -> Option<Arc<dyn Index>> {
        None
    }

    fn insert(&self, values: Vec<Value>) -> RowId {
        let last_pid = {
            let pages = self.pages.lock().unwrap();
            *pages.last().unwrap()
        };

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

    fn update(&self, rid: RowId, values: Vec<Value>) -> bool {
        let mut bp = self.bp.lock().unwrap();

        let frame = bp.fetch_page(rid.page_id);
        let mut page = RowPage::from_bytes(rid.page_id, &frame.data);

        let ok = page.update(rid.slot_id, values);
        if ok {
            page.write_bytes(&mut frame.data);
            bp.unpin_page(rid.page_id, true);
        } else {
            bp.unpin_page(rid.page_id, false);
        }

        ok
    }

    fn delete(&self, rid: RowId) -> bool {
        let mut bp = self.bp.lock().unwrap();

        let frame = bp.fetch_page(rid.page_id);
        let mut page = RowPage::from_bytes(rid.page_id, &frame.data);

        let ok = page.delete(rid.slot_id);
        if ok {
            page.write_bytes(&mut frame.data);
            bp.unpin_page(rid.page_id, true);
        } else {
            bp.unpin_page(rid.page_id, false);
        }

        ok
    }
}

impl HeapTableCursor {
    pub fn new(table: Arc<HeapTable>) -> Self {
        Self {
            table,
            page_idx: 0,
            slot_idx: 0,
        }
    }
}

impl TableCursor for HeapTableCursor {
    fn next(&mut self) -> Option<StorageRow> {
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

                if let Some(mut row) = page.get(slot).cloned() {
                    row.rid = RowId {
                        page_id: pid,
                        slot_id: slot,
                    };
                    return Some(row);
                }
            }

            self.page_idx += 1;
            self.slot_idx = 0;
        }
    }
}
