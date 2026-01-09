use std::sync::{Arc, Mutex};

use super::frame::PageFrame;
use crate::storage::page::page_id::PageId;

pub trait PageManager: Send + Sync {
    fn allocate_page(&mut self) -> PageId;
    fn fetch_page(&mut self, id: PageId) -> &mut PageFrame;
    fn flush_page(&mut self, id: PageId);
    fn flush_all(&mut self);
}

pub type PageManagerHandle = Arc<Mutex<dyn PageManager>>;
