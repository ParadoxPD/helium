use crate::storage::page::PageId;

pub struct BufferFrame {
    pub page: PageFrame,
    pub pin_count: usize,
}

pub const PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone)]
pub struct PageFrame {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
    pub dirty: bool,
}
