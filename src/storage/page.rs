use std::collections::HashMap;

use crate::{common::value::Value, exec::operator::Row};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageId(pub u64);

pub trait Page {
    fn id(&self) -> PageId;
    fn num_rows(&self) -> usize;
    fn capacity(&self) -> usize;
    fn is_full(&self) -> bool;
    fn get_row(&self, slot_id: u16) -> Option<&StorageRow>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowId {
    pub page_id: PageId,
    pub slot_id: u16,
}

#[derive(Debug, Clone)]
pub struct StorageRow {
    pub rid: RowId,
    pub values: Vec<Value>,
}

impl StorageRow {
    pub fn new(values: Vec<Value>) -> Self {
        Self {
            rid: RowId {
                page_id: PageId(0),
                slot_id: 0,
            },
            values: values,
        }
    }
}

pub struct RowSlot {
    pub row: Option<StorageRow>,
    pub used: bool,
}

impl RowSlot {
    pub fn empty() -> Self {
        Self {
            row: None,
            used: false,
        }
    }
}

pub struct RowPage {
    pub id: PageId,
    pub slots: Vec<RowSlot>,
    capacity: usize,
}

impl RowPage {
    pub fn new(id: PageId, capacity: usize) -> Self {
        Self {
            id,
            slots: (0..capacity).map(|_| RowSlot::empty()).collect(),
            capacity,
        }
    }
    pub fn insert(&mut self, values: Vec<Value>) -> Option<RowId> {
        if self.is_full() {
            return None;
        }

        let slot_id = self.slots.len() as u16;
        let rid = RowId {
            page_id: self.id,
            slot_id,
        };

        self.slots.push(RowSlot {
            used: true,
            row: Some(StorageRow { rid, values }),
        });

        Some(rid)
    }

    pub fn delete(&mut self, slot_id: u16) -> bool {
        let slot = match self.slots.get_mut(slot_id as usize) {
            Some(s) => s,
            None => return false,
        };

        if !slot.used {
            return false;
        }

        slot.used = false;
        slot.row = None;
        true
    }
    pub fn update(&mut self, slot_id: u16, values: Vec<Value>) -> bool {
        let slot = match self.slots.get_mut(slot_id as usize) {
            Some(s) => s,
            None => return false,
        };

        if !slot.used {
            return false;
        }

        let rid = slot.row.as_ref().unwrap().rid;
        slot.row = Some(StorageRow { rid, values });
        true
    }

    pub fn get(&self, slot_id: u16) -> Option<&StorageRow> {
        self.slots
            .get(slot_id as usize)
            .and_then(|s| s.row.as_ref())
    }

    pub fn iter(&self) -> impl Iterator<Item = (RowId, &StorageRow)> {
        self.slots.iter().enumerate().filter_map(move |(i, slot)| {
            slot.row.as_ref().map(|row| {
                (
                    RowId {
                        page_id: self.id,
                        slot_id: i as u16,
                    },
                    row,
                )
            })
        })
    }

    pub fn push(&mut self, row: StorageRow) -> Result<RowId, ()> {
        if self.is_full() {
            return Err(());
        }

        let idx = self.slots.len();
        self.slots.push(RowSlot {
            row: Some(row),
            used: true,
        });

        Ok(RowId {
            page_id: self.id,
            slot_id: idx as u16,
        })
    }
}

impl Page for RowPage {
    fn id(&self) -> PageId {
        self.id
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn num_rows(&self) -> usize {
        self.slots.iter().filter(|s| s.row.is_some()).count()
    }

    fn is_full(&self) -> bool {
        self.num_rows() == self.capacity()
    }

    fn get_row(&self, slot_id: u16) -> Option<&StorageRow> {
        self.slots.get(slot_id as usize).unwrap().row.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        common::value::Value,
        storage::page::{Page, PageId, RowId, RowPage, StorageRow},
    };

    fn srow(values: Vec<Value>) -> StorageRow {
        StorageRow {
            rid: RowId {
                page_id: PageId(0),
                slot_id: 0,
            },
            values,
        }
    }

    #[test]
    fn page_respects_capacity() {
        let mut page = RowPage::new(PageId(1), 2);

        assert!(page.push(srow(vec![Value::Int64(1)])).is_ok());
        assert!(page.push(srow(vec![Value::Int64(2)])).is_ok());

        assert!(page.is_full());
        assert!(page.push(srow(vec![Value::Int64(3)])).is_err());
    }

    #[test]
    fn page_capacity_and_count() {
        let mut page = RowPage::new(PageId(0), 2);

        assert_eq!(page.capacity(), 2);
        assert_eq!(page.num_rows(), 0);
        assert!(!page.is_full());

        page.push(srow(vec![Value::Int64(1)])).unwrap();
        assert_eq!(page.num_rows(), 1);

        page.push(srow(vec![Value::Int64(2)])).unwrap();
        assert!(page.is_full());
    }

    #[test]
    fn get_row_returns_correct_slot() {
        let mut page = RowPage::new(PageId(0), 1);

        let rid = page.push(srow(vec![Value::Int64(42)])).unwrap();
        let row = page.get_row(rid.slot_id).unwrap();

        assert_eq!(row.values[0], Value::Int64(42));
    }
}
