use crate::storage::buffer::frame::PAGE_SIZE;
use crate::storage::page::row_id::RowId;
use crate::types::value::Value;

use super::row::StorageRow;
use super::{page_id::PageId, traits::Page};

pub const HEADER_SIZE: usize = 8;
pub const SLOT_SIZE: usize = 4;

#[derive(Debug, Clone)]
pub struct Slot {
    pub offset: u32,
    pub used: bool,
}

pub struct RowPage {
    id: PageId,
    slots: Vec<Slot>,
    rows: Vec<StorageRow>,
    free_slots: Vec<u16>,
    capacity: usize,
}

impl RowPage {
    pub fn new(id: PageId, capacity: usize) -> Self {
        Self {
            id,
            slots: Vec::with_capacity(capacity),
            rows: Vec::with_capacity(capacity),
            free_slots: Vec::new(),
            capacity,
        }
    }

    pub fn get(&self, slot_id: u16) -> Option<&StorageRow> {
        let slot = self.slots.get(slot_id as usize)?;

        if !slot.used {
            return None;
        }

        self.rows.get(slot.offset as usize)
    }

    pub fn from_bytes(id: PageId, buf: &[u8]) -> Self {
        // ---- header ----
        let slot_count = u16::from_le_bytes([buf[0], buf[1]]) as usize;
        let row_count = u16::from_le_bytes([buf[2], buf[3]]) as usize;
        let capacity = u16::from_le_bytes([buf[4], buf[5]]) as usize;

        // ---- slots ----
        let mut slots = Vec::with_capacity(slot_count);
        let mut offset = HEADER_SIZE;

        for _ in 0..slot_count {
            let off = u16::from_le_bytes([buf[offset], buf[offset + 1]]);
            let used = buf[offset + 2] != 0;

            slots.push(Slot {
                offset: off as u32,
                used,
            });

            offset += SLOT_SIZE;
        }

        // ---- rows ----
        let mut rows = Vec::with_capacity(row_count);
        let mut row_ptr = HEADER_SIZE + SLOT_SIZE * slot_count;

        for _ in 0..row_count {
            let val_count = u16::from_le_bytes([buf[row_ptr], buf[row_ptr + 1]]) as usize;
            row_ptr += 2;

            let mut values = Vec::with_capacity(val_count);
            let mut slice = &buf[row_ptr..];

            for _ in 0..val_count {
                let v = Value::deserialize(&mut slice);
                values.push(v);
            }

            let consumed = buf[row_ptr..].len() - slice.len();
            row_ptr += consumed;

            rows.push(StorageRow { values });
        }

        // ---- free slots ----
        let free_slots = slots
            .iter()
            .enumerate()
            .filter_map(|(i, s)| if !s.used { Some(i as u16) } else { None })
            .collect();

        Self {
            id,
            slots,
            rows,
            free_slots,
            capacity,
        }
    }

    pub fn slots_len(&self) -> usize {
        self.slots.len()
    }

    pub fn insert(&mut self, values: Vec<Value>) -> Option<RowId> {
        if self.num_rows() == self.capacity {
            return None;
        }

        let slot_id = if let Some(free) = self.free_slots.pop() {
            let slot = &mut self.slots[free as usize];
            slot.used = true;
            slot.offset = self.rows.len() as u32;
            free
        } else {
            let slot_id = self.slots.len() as u16;
            self.slots.push(Slot {
                offset: self.rows.len() as u32,
                used: true,
            });
            slot_id
        };

        self.rows.push(StorageRow { values });

        Some(RowId {
            page_id: self.id,
            slot_id,
        })
    }

    pub fn delete(&mut self, slot_id: u16) -> bool {
        let slot = match self.slots.get_mut(slot_id as usize) {
            Some(s) if s.used => s,
            _ => return false,
        };

        slot.used = false;
        self.free_slots.push(slot_id);

        //#[cfg(debug_assertions)]
        //self.assert_consistent();

        true
    }

    pub fn write_bytes(&self, buf: &mut [u8]) {
        assert!(buf.len() >= PAGE_SIZE);

        buf.fill(0);

        let slot_count = self.slots.len() as u16;
        let row_count = self.rows.len() as u16;
        let capacity = self.capacity as u16;

        // ---- header ----
        buf[0..2].copy_from_slice(&slot_count.to_le_bytes());
        buf[2..4].copy_from_slice(&row_count.to_le_bytes());
        buf[4..6].copy_from_slice(&capacity.to_le_bytes());
        // buf[6..8] reserved

        // ---- slots ----
        let mut offset = HEADER_SIZE;
        for slot in &self.slots {
            let off = slot.offset as u16;
            buf[offset..offset + 2].copy_from_slice(&off.to_le_bytes());
            buf[offset + 2] = if slot.used { 1 } else { 0 };
            buf[offset + 3] = 0; // padding
            offset += SLOT_SIZE;
        }

        // ---- rows ----
        let mut row_ptr = HEADER_SIZE + SLOT_SIZE * self.slots.len();

        for row in &self.rows {
            let values = &row.values;

            buf[row_ptr..row_ptr + 2].copy_from_slice(&(values.len() as u16).to_le_bytes());
            row_ptr += 2;

            for v in values {
                let mut tmp = Vec::new();
                v.serialize(&mut tmp);
                buf[row_ptr..row_ptr + tmp.len()].copy_from_slice(&tmp);
                row_ptr += tmp.len();
            }
        }

        debug_assert!(row_ptr <= PAGE_SIZE);
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
        self.slots.iter().filter(|s| s.used).count()
    }

    fn is_full(&self) -> bool {
        self.num_rows() == self.capacity
    }

    fn get_row(&self, slot_id: u16) -> Option<&StorageRow> {
        self.get(slot_id)
    }
}
