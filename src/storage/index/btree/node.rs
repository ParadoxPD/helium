use crate::storage::errors::{StorageError, StorageResult};
use crate::storage::index::btree::key::IndexKey;
use crate::storage::page::page_id::PageId;
use crate::storage::page::row_id::RowId;

pub enum BTreeNode {
    Internal {
        keys: Vec<IndexKey>,
        children: Vec<PageId>, // page ids
    },
    Leaf {
        keys: Vec<IndexKey>,
        values: Vec<Vec<RowId>>, // one-to-many
        next: Option<PageId>,
    },
}

impl BTreeNode {
    pub fn write_bytes(&self, buf: &mut [u8], page_id: PageId) -> StorageResult<()> {
        buf.fill(0);
        let mut out = Vec::new();

        match self {
            BTreeNode::Leaf { keys, values, next } => {
                if keys.len() != values.len() {
                    return Err(StorageError::IndexCorrupted {
                        page_id: page_id.0,
                        reason: "leaf keys/values length mismatch".into(),
                    });
                }

                out.push(0); // leaf tag
                out.extend_from_slice(&(keys.len() as u16).to_le_bytes());
                out.extend_from_slice(&0u16.to_le_bytes()); // reserved
                out.extend_from_slice(&next.map(|p| p.0).unwrap_or(u64::MAX).to_le_bytes());

                for k in keys {
                    k.serialize(&mut out);
                }

                for list in values {
                    out.extend_from_slice(&(list.len() as u16).to_le_bytes());
                    for rid in list {
                        out.extend_from_slice(&rid.page_id.0.to_le_bytes());
                        out.extend_from_slice(&rid.slot_id.to_le_bytes());
                    }
                }
            }

            BTreeNode::Internal { keys, children } => {
                if children.len() != keys.len() + 1 {
                    return Err(StorageError::IndexCorrupted {
                        page_id: page_id.0,
                        reason: "internal keys/children invariant violated".into(),
                    });
                }

                out.push(1); // internal tag
                out.extend_from_slice(&(keys.len() as u16).to_le_bytes());
                out.extend_from_slice(&0u16.to_le_bytes()); // reserved
                out.extend_from_slice(&0u64.to_le_bytes()); // unused

                for k in keys {
                    k.serialize(&mut out);
                }

                for c in children {
                    out.extend_from_slice(&c.0.to_le_bytes());
                }
            }
        }

        if out.len() > buf.len() {
            return Err(StorageError::IndexCorrupted {
                page_id: page_id.0,
                reason: "BTreeNode serialization overflow".into(),
            });
        }

        buf[..out.len()].copy_from_slice(&out);
        Ok(())
    }

    pub fn from_bytes(buf: &[u8], page_id: PageId) -> StorageResult<BTreeNode> {
        let mut input = buf;

        let node_type = *input.get(0).ok_or(StorageError::IndexCorrupted {
            page_id: page_id.0,
            reason: "unexpected EOF reading node type".into(),
        })?;
        input = &input[1..];

        let key_count = u16::from_le_bytes(
            input
                .get(..2)
                .ok_or(StorageError::IndexCorrupted {
                    page_id: page_id.0,
                    reason: "unexpected EOF reading key_count".into(),
                })?
                .try_into()
                .unwrap(),
        ) as usize;

        input = input
            .get(4..) // skip key_count + reserved
            .ok_or(StorageError::IndexCorrupted {
                page_id: page_id.0,
                reason: "unexpected EOF skipping header".into(),
            })?;

        let next_raw = u64::from_le_bytes(
            input
                .get(..8)
                .ok_or(StorageError::IndexCorrupted {
                    page_id: page_id.0,
                    reason: "unexpected EOF reading next pointer".into(),
                })?
                .try_into()
                .unwrap(),
        );
        input = &input[8..];

        let mut keys = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            keys.push(IndexKey::deserialize(&mut input, page_id.0)?);
        }

        match node_type {
            // ---------- LEAF ----------
            0 => {
                let mut values = Vec::with_capacity(key_count);

                for _ in 0..key_count {
                    let cnt = u16::from_le_bytes(
                        input
                            .get(..2)
                            .ok_or(StorageError::IndexCorrupted {
                                page_id: page_id.0,
                                reason: "unexpected EOF reading rid count".into(),
                            })?
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    input = &input[2..];

                    let mut list = Vec::with_capacity(cnt);
                    for _ in 0..cnt {
                        let pid = u64::from_le_bytes(
                            input
                                .get(..8)
                                .ok_or(StorageError::IndexCorrupted {
                                    page_id: page_id.0,
                                    reason: "unexpected EOF reading RowId.page_id".into(),
                                })?
                                .try_into()
                                .unwrap(),
                        );

                        let sid = u16::from_le_bytes(
                            input
                                .get(8..10)
                                .ok_or(StorageError::IndexCorrupted {
                                    page_id: page_id.0,
                                    reason: "unexpected EOF reading RowId.slot_id".into(),
                                })?
                                .try_into()
                                .unwrap(),
                        );

                        input = &input[10..];

                        list.push(RowId {
                            page_id: PageId(pid),
                            slot_id: sid,
                        });
                    }
                    values.push(list);
                }

                Ok(BTreeNode::Leaf {
                    keys,
                    values,
                    next: if next_raw == u64::MAX {
                        None
                    } else {
                        Some(PageId(next_raw))
                    },
                })
            }

            // ---------- INTERNAL ----------
            1 => {
                let mut children = Vec::with_capacity(key_count + 1);
                for _ in 0..key_count + 1 {
                    let pid = u64::from_le_bytes(
                        input
                            .get(..8)
                            .ok_or(StorageError::IndexCorrupted {
                                page_id: page_id.0,
                                reason: "unexpected EOF reading child pointer".into(),
                            })?
                            .try_into()
                            .unwrap(),
                    );
                    input = &input[8..];
                    children.push(PageId(pid));
                }

                Ok(BTreeNode::Internal { keys, children })
            }

            _ => Err(StorageError::IndexCorrupted {
                page_id: page_id.0,
                reason: format!("invalid B+Tree node type {}", node_type),
            }),
        }
    }
}
