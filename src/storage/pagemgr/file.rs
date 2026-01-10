use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use crate::storage::{
    buffer::frame::PAGE_SIZE,
    errors::{StorageError, StorageResult},
    page::page_id::PageId,
    pagemgr::{frame::PageFrame, manager::PageManager},
};

#[derive(Debug)]
pub struct FilePageManager {
    file: File,
    pages: HashMap<PageId, PageFrame>,
    next_page_id: u64,
}

impl FilePageManager {
    pub fn open(path: &PathBuf) -> std::io::Result<Self> {
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

    fn fetch_page(&mut self, id: PageId) -> StorageResult<&mut PageFrame> {
        if !self.pages.contains_key(&id) {
            let mut frame = PageFrame {
                id,
                data: [0u8; PAGE_SIZE],
                dirty: false,
            };

            let offset = id.0 * PAGE_SIZE as u64;
            self.file
                .seek(SeekFrom::Start(offset))
                .map_err(|e| StorageError::Io {
                    message: e.to_string(),
                })?;
            self.file
                .read_exact(&mut frame.data)
                .map_err(|e| StorageError::Io {
                    message: e.to_string(),
                })?;

            self.pages.insert(id, frame);
        }

        Ok(self.pages.get_mut(&id).unwrap())
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
