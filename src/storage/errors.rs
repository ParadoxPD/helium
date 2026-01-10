use std::fmt;

#[derive(Debug)]
pub enum StorageError {
    PageNotFound { page_id: u64 },

    InvalidRowId { page_id: u64, slot_id: u16 },

    PageFull { page_id: u64 },

    CorruptedPage { page_id: u64, reason: String },

    IndexViolation { index_name: String, reason: String },

    Io { message: String },
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::PageNotFound { page_id } => {
                write!(f, "storage error: page {} not found", page_id)
            }

            StorageError::InvalidRowId { page_id, slot_id } => {
                write!(
                    f,
                    "storage error: invalid row id (page {}, slot {})",
                    page_id, slot_id
                )
            }

            StorageError::PageFull { page_id } => {
                write!(f, "storage error: page {} is full", page_id)
            }

            StorageError::CorruptedPage { page_id, reason } => {
                write!(f, "storage error: corrupted page {} ({})", page_id, reason)
            }

            StorageError::IndexViolation { index_name, reason } => {
                write!(
                    f,
                    "storage error: index '{}' violated ({})",
                    index_name, reason
                )
            }

            StorageError::Io { message } => {
                write!(f, "storage IO error: {}", message)
            }
        }
    }
}

impl std::error::Error for StorageError {}
