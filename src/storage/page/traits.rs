use super::page_id::PageId;
use super::row::StorageRow;

pub trait Page {
    fn id(&self) -> PageId;
    fn capacity(&self) -> usize;
    fn num_rows(&self) -> usize;
    fn is_full(&self) -> bool;
    fn get_row(&self, slot_id: u16) -> Option<&StorageRow>;
}
