use crate::buffer::frame::{PAGE_SIZE, PageFrame};
use crate::storage::page::PageId;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

pub trait PageManager: Send + Sync {
    fn allocate_page(&mut self) -> PageId;
    fn fetch_page(&mut self, id: PageId) -> &mut PageFrame;
    fn flush_page(&mut self, id: PageId);
    fn flush_all(&mut self);
}

pub type PageManagerHandle = Arc<Mutex<dyn PageManager>>;

pub struct FilePageManager {
    file: File,
    pages: HashMap<PageId, PageFrame>,
    next_page_id: u64,
}

impl FilePageManager {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let size = file.metadata()?.len();
        let next_page_id = size / PAGE_SIZE as u64;

        Ok(Self {
            file,
            pages: HashMap::new(),
            next_page_id,
        })
    }
}

impl PageManager for FilePageManager {
    fn allocate_page(&mut self) -> PageId {
        let id = PageId(self.next_page_id);
        self.next_page_id += 1;

        let frame = PageFrame {
            id,
            data: [0u8; PAGE_SIZE],
            dirty: true,
        };

        self.pages.insert(id, frame);
        id
    }

    fn fetch_page(&mut self, id: PageId) -> &mut PageFrame {
        if !self.pages.contains_key(&id) {
            let mut frame = PageFrame {
                id,
                data: [0u8; PAGE_SIZE],
                dirty: false,
            };

            let offset = id.0 * PAGE_SIZE as u64;
            self.file.seek(SeekFrom::Start(offset)).unwrap();
            self.file.read_exact(&mut frame.data).unwrap_or(());

            self.pages.insert(id, frame);
        }

        self.pages.get_mut(&id).unwrap()
    }

    fn flush_page(&mut self, id: PageId) {
        if let Some(frame) = self.pages.get_mut(&id) {
            if frame.dirty {
                let offset = id.0 * PAGE_SIZE as u64;
                self.file.seek(SeekFrom::Start(offset)).unwrap();
                self.file.write_all(&frame.data).unwrap();
                frame.dirty = false;
            }
        }
    }

    fn flush_all(&mut self) {
        let ids: Vec<PageId> = self.pages.keys().cloned().collect();
        for id in ids {
            self.flush_page(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::page_manager::{FilePageManager, PageManager};

    #[test]
    fn page_manager_roundtrip() {
        let path = "/tmp/helium_test.db";
        std::fs::remove_file(path).ok();

        let mut pm = FilePageManager::open(path).unwrap();

        let pid = pm.allocate_page();
        {
            let page = pm.fetch_page(pid);
            page.data[0] = 42;
            page.dirty = true;
        }

        pm.flush_all();

        let mut pm2 = FilePageManager::open(path).unwrap();
        let page = pm2.fetch_page(pid);
        assert_eq!(page.data[0], 42);
    }
}
