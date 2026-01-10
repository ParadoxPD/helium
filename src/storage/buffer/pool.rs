use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::storage::{
    buffer::frame::{BufferFrame, PageFrame},
    errors::{StorageError, StorageResult},
    page::page_id::PageId,
    pagemgr::manager::PageManager,
};

pub type BufferPoolHandle = Arc<Mutex<BufferPool>>;

pub struct BufferPool {
    pub pm: Box<dyn PageManager>,
    frames: HashMap<PageId, BufferFrame>,
}

impl BufferPool {
    pub fn new(pm: Box<dyn PageManager>) -> Self {
        Self {
            pm,
            frames: HashMap::new(),
        }
    }

    pub fn fetch_page(&mut self, pid: PageId) -> StorageResult<&mut PageFrame> {
        if !self.frames.contains_key(&pid) {
            let pm_page = self.pm.fetch_page(pid)?;

            let page = PageFrame {
                id: pm_page.id,
                data: pm_page.data,
                dirty: false,
            };

            self.frames.insert(pid, BufferFrame { page, pin_count: 0 });
        }

        let frame = self
            .frames
            .get_mut(&pid)
            .ok_or(StorageError::PageNotFound { page_id: pid.0 })?;

        frame.pin_count += 1;
        Ok(&mut frame.page)
    }

    pub fn unpin_page(&mut self, pid: PageId, dirty: bool) -> StorageResult<()> {
        let frame = self
            .frames
            .get_mut(&pid)
            .ok_or(StorageError::PageNotFound { page_id: pid.0 })?;

        if dirty {
            frame.page.dirty = true;
        }

        if frame.pin_count == 0 {
            return Err(StorageError::CorruptedPage {
                page_id: pid.0,
                reason: "unbalanced unpin".into(),
            });
        }
        frame.pin_count -= 1;
        Ok(())
    }

    pub fn flush_all(&mut self) -> StorageResult<()> {
        for frame in self.frames.values_mut() {
            if frame.page.dirty {
                let pm_page = self.pm.fetch_page(frame.page.id)?;
                pm_page.data.copy_from_slice(&frame.page.data);
                self.pm.flush_page(frame.page.id);
                frame.page.dirty = false;
            }
        }
        Ok(())
    }
}
