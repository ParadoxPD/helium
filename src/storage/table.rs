use std::sync::Arc;

use crate::{
    common::schema::Schema,
    exec::operator::Row,
    storage::page::{Page, PageId, RowId, RowPage, StorageRow},
};

pub trait Table: Send + Sync {
    fn scan(self: Arc<Self>) -> Box<dyn TableCursor>;
    fn schema(&self) -> &Schema;
}

pub trait TableCursor {
    fn next(&mut self) -> Option<StorageRow>;
}

pub struct HeapTable {
    name: String,
    schema: Schema,
    pages: Vec<RowPage>,
    page_capacity: usize,
}

pub struct HeapTableCursor {
    table: Arc<HeapTable>,
    page_idx: usize,
    row_idx: usize,
}

impl HeapTable {
    pub fn new(name: String, schema: Vec<String>, page_capacity: usize) -> Self {
        Self {
            name,
            schema,
            pages: vec![RowPage::new(PageId(0), page_capacity)],
            page_capacity,
        }
    }

    pub fn insert(&mut self, row: StorageRow) -> RowId {
        // try last page
        if let Some(last) = self.pages.last_mut() {
            if let Some(rid) = last.insert(row.clone()) {
                return rid;
            }
        }

        // allocate new page
        let new_page_id = PageId(self.pages.len() as u64);
        let mut page = RowPage::new(new_page_id, self.page_capacity);

        let rid = page.insert(row).expect("new page must have space");

        self.pages.push(page);
        rid
    }

    pub fn update(&mut self, rid: RowId, new_row: StorageRow) -> bool {
        if let Some(page) = self.pages.get_mut(rid.page_id.0 as usize) {
            return page.update(rid.slot_id, new_row);
        }
        false
    }

    pub fn delete(&mut self, rid: RowId) -> bool {
        let page = &mut self.pages[rid.page_id.0 as usize];
        if let Some(slot) = page.slots.get_mut(rid.slot_id as usize) {
            *slot = None.unwrap();
            return true;
        }
        false
    }
}

impl Table for HeapTable {
    fn scan(self: Arc<Self>) -> Box<dyn TableCursor> {
        Box::new(HeapTableCursor::new(self))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl HeapTableCursor {
    pub fn new(table: Arc<HeapTable>) -> Self {
        Self {
            table,
            page_idx: 0,
            row_idx: 0,
        }
    }
}

impl TableCursor for HeapTableCursor {
    fn next(&mut self) -> Option<StorageRow> {
        while self.page_idx < self.table.pages.len() {
            let page = &self.table.pages[self.page_idx];

            if let Some(row) = page.get(self.row_idx as u16) {
                let rid = RowId {
                    page_id: page.id(),
                    slot_id: self.row_idx as u16,
                };

                self.row_idx += 1;

                return Some(StorageRow {
                    rid,
                    values: row.values.clone(),
                });
            }

            self.page_idx += 1;
            self.row_idx = 0;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        common::value::Value,
        storage::{
            page::{RowId, StorageRow},
            table::{HeapTable, Table},
        },
    };

    fn srow(values: Vec<Value>) -> StorageRow {
        StorageRow {
            rid: RowId {
                page_id: crate::storage::page::PageId(0),
                slot_id: 0,
            },
            values,
        }
    }

    #[test]
    fn heap_table_grows_pages() {
        let mut t = HeapTable::new("t".into(), vec!["id".into()], 2);

        t.insert(srow(vec![Value::Int64(1)]));
        t.insert(srow(vec![Value::Int64(2)]));
        t.insert(srow(vec![Value::Int64(3)]));

        assert_eq!(t.pages.len(), 2);
    }

    #[test]
    fn heap_cursor_scans_all_rows() {
        let mut t = HeapTable::new("t".into(), vec!["id".into()], 2);

        t.insert(srow(vec![Value::Int64(1)]));
        t.insert(srow(vec![Value::Int64(2)]));
        t.insert(srow(vec![Value::Int64(3)]));

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
        let mut table = HeapTable::new("users".into(), vec!["id".into()], 2);

        let r1 = table.insert(srow(vec![Value::Int64(1)]));
        let r2 = table.insert(srow(vec![Value::Int64(2)]));

        assert_ne!(r1, r2);
        assert_eq!(r1.page_id, r2.page_id);
        assert_ne!(r1.slot_id, r2.slot_id);
    }

    #[test]
    fn insert_allocates_new_page_when_full() {
        let mut table = HeapTable::new("users".into(), vec!["id".into()], 1);

        let r1 = table.insert(srow(vec![Value::Int64(1)]));
        let r2 = table.insert(srow(vec![Value::Int64(2)]));

        assert_ne!(r1.page_id, r2.page_id);
    }

    #[test]
    fn delete_hides_row_but_preserves_slot() {
        let mut table = HeapTable::new("users".into(), vec!["id".into()], 2);

        let r1 = table.insert(srow(vec![Value::Int64(1)]));
        let r2 = table.insert(srow(vec![Value::Int64(2)]));

        assert!(table.delete(r1));

        let mut seen = Vec::new();
        for page in &table.pages {
            for slot in &page.slots {
                if let Some(row) = &slot.row {
                    seen.push(row.clone());
                }
            }
        }

        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].values[0], Value::Int64(2));
    }

    #[test]
    fn update_overwrites_row_in_place() {
        let mut table = HeapTable::new("users".into(), vec!["age".into()], 2);

        let rid = table.insert(srow(vec![Value::Int64(20)]));

        assert!(table.update(rid, srow(vec![Value::Int64(21)])));

        let mut cursor = Arc::new(table).scan();
        let r = cursor.next().unwrap();

        assert_eq!(r.values[0], Value::Int64(21));
        assert_eq!(r.rid, rid);
    }

    #[test]
    fn update_fails_on_deleted_row() {
        let mut table = HeapTable::new("users".into(), vec!["x".into()], 1);

        let rid = table.insert(srow(vec![Value::Int64(1)]));
        table.delete(rid);

        assert!(!table.update(rid, srow(vec![Value::Int64(2)])));
    }

    #[test]
    fn update_preserves_slot_id() {
        let mut table = HeapTable::new("t".into(), vec!["x".into()], 1);

        let rid = table.insert(srow(vec![Value::Int64(1)]));
        table.update(rid, srow(vec![Value::Int64(99)]));

        let mut cursor = Arc::new(table).scan();
        let r = cursor.next().unwrap();

        assert_eq!(r.rid.slot_id, rid.slot_id);
    }
}
