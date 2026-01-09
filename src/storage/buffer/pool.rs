use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::storage::{
    buffer::frame::{BufferFrame, PageFrame},
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

    pub fn fetch_page(&mut self, pid: PageId) -> &mut PageFrame {
        let frame = self.frames.entry(pid).or_insert_with(|| {
            let page = {
                let pm_page = self.pm.fetch_page(pid);
                PageFrame {
                    id: pm_page.id,
                    data: pm_page.data,
                    dirty: false,
                }
            };

            BufferFrame { page, pin_count: 0 }
        });

        frame.pin_count += 1;
        &mut frame.page
    }

    pub fn unpin_page(&mut self, pid: PageId, dirty: bool) {
        let frame = self.frames.get_mut(&pid).expect("unpinning unknown page");

        if dirty {
            frame.page.dirty = true;
        }

        assert!(frame.pin_count > 0);
        frame.pin_count -= 1;
    }

    pub fn flush_all(&mut self) {
        for frame in self.frames.values_mut() {
            if frame.page.dirty {
                let pm_page = self.pm.fetch_page(frame.page.id);
                pm_page.data.copy_from_slice(&frame.page.data);
                self.pm.flush_page(frame.page.id);
                frame.page.dirty = false;
            }
        }
    }
}
