use std::sync::Arc;

use crate::{
    buffer::buffer_pool::BufferPoolHandle,
    common::{schema::Schema, value::Value},
    exec::operator::Row,
    storage::{
        btree::node::Index,
        page::{HeapPageHandle, Page, PageId, RowId, RowPage, RowSlot, StorageRow},
        page_manager::PageManagerHandle,
    },
};

pub trait Table: Send + Sync {
    fn scan(self: Arc<Self>) -> Box<dyn TableCursor>;
    fn schema(&self) -> &Schema;
    fn fetch(&self, rid: RowId) -> StorageRow;
    fn get_index(&self, column: &str) -> Option<Arc<dyn Index>>;
}

pub trait TableCursor {
    fn next(&mut self) -> Option<StorageRow>;
}

pub struct HeapTable {
    name: String,
    schema: Schema,
    pages: Vec<PageId>,
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
            pages: vec![pid],
            page_capacity,
            bp,
        }
    }

    pub fn insert(&mut self, values: Vec<Value>) -> RowId {
        let last_pid = *self.pages.last().unwrap();

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

        self.pages.push(pid);
        rid
    }

    pub fn update(&mut self, rid: RowId, values: Vec<Value>) -> bool {
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

    pub fn delete(&mut self, rid: RowId) -> bool {
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
            if self.page_idx >= self.table.pages.len() {
                return None;
            }

            let pid = self.table.pages[self.page_idx];

            let mut bp = self.table.bp.lock().unwrap();
            let frame = bp.fetch_page(pid);
            let page = RowPage::from_bytes(pid, &frame.data);

            while self.slot_idx < page.capacity() as u16 {
                let slot = self.slot_idx;
                self.slot_idx += 1;

                if let Some(row) = page.get(slot) {
                    let result = row.clone();
                    bp.unpin_page(pid, false);
                    return Some(result);
                }
            }

            bp.unpin_page(pid, false);

            self.page_idx += 1;
            self.slot_idx = 0;
        }
    }
}

#[cfg(test)]
mod tests {
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

        assert_eq!(t.pages.len(), 2);
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
