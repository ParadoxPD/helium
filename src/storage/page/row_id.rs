#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RowId {
    pub page_id: u32,
    pub slot: u16,
}
