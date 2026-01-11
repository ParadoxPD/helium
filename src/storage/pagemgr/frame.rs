use crate::storage::{buffer::frame::PAGE_SIZE, page::page_id::PageId};

#[derive(Debug)]
pub struct PageFrame {
    pub id: PageId,
    pub data: [u8; PAGE_SIZE],
    pub dirty: bool,
}
