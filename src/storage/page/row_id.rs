use crate::storage::page::page_id::PageId;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RowId {
    pub page_id: PageId,
    pub slot_id: u16,
}
