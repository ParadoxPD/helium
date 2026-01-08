use crate::storage::{
    btree::node::IndexKey,
    page::{PageId, RowId},
};

pub enum DiskNode {
    Leaf {
        keys: Vec<IndexKey>,
        rids: Vec<Vec<RowId>>,
        next: Option<PageId>,
    },
    Internal {
        keys: Vec<IndexKey>,
        children: Vec<PageId>,
    },
}

impl DiskNode {
    pub fn write_bytes(&self, buf: &mut [u8]) {
        buf.fill(0);
        let mut out = Vec::new();

        match self {
            DiskNode::Leaf { keys, rids, next } => {
                out.push(0);
                out.extend_from_slice(&(keys.len() as u16).to_le_bytes());
                out.extend_from_slice(&0u16.to_le_bytes());
                out.extend_from_slice(&next.map(|p| p.0).unwrap_or(0).to_le_bytes());

                for k in keys {
                    k.serialize(&mut out);
                }

                for list in rids {
                    out.extend_from_slice(&(list.len() as u16).to_le_bytes());
                    for rid in list {
                        out.extend_from_slice(&rid.page_id.0.to_le_bytes());
                        out.extend_from_slice(&rid.slot_id.to_le_bytes());
                    }
                }
            }

            DiskNode::Internal { keys, children } => {
                out.push(1);
                out.extend_from_slice(&(keys.len() as u16).to_le_bytes());
                out.extend_from_slice(&0u16.to_le_bytes());
                out.extend_from_slice(&0u64.to_le_bytes());

                for k in keys {
                    k.serialize(&mut out);
                }

                for c in children {
                    out.extend_from_slice(&c.0.to_le_bytes());
                }
            }
        }

        buf[..out.len()].copy_from_slice(&out);
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        let mut input = buf;

        let node_type = input[0];
        input = &input[1..];

        let key_count = u16::from_le_bytes(input[..2].try_into().unwrap()) as usize;
        input = &input[4..]; // skip key_count + reserved

        let next = u64::from_le_bytes(input[..8].try_into().unwrap());
        input = &input[8..];

        let mut keys = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            keys.push(IndexKey::deserialize(&mut input));
        }

        match node_type {
            0 => {
                let mut rids = Vec::with_capacity(key_count);
                for _ in 0..key_count {
                    let cnt = u16::from_le_bytes(input[..2].try_into().unwrap()) as usize;
                    input = &input[2..];
                    let mut list = Vec::with_capacity(cnt);
                    for _ in 0..cnt {
                        let pid = u64::from_le_bytes(input[..8].try_into().unwrap());
                        let sid = u16::from_le_bytes(input[8..10].try_into().unwrap());
                        input = &input[10..];
                        list.push(RowId {
                            page_id: PageId(pid),
                            slot_id: sid,
                        });
                    }
                    rids.push(list);
                }

                DiskNode::Leaf {
                    keys,
                    rids,
                    next: if next == 0 { None } else { Some(PageId(next)) },
                }
            }

            1 => {
                let mut children = Vec::with_capacity(key_count + 1);
                for _ in 0..key_count + 1 {
                    let pid = u64::from_le_bytes(input[..8].try_into().unwrap());
                    input = &input[8..];
                    children.push(PageId(pid));
                }

                DiskNode::Internal { keys, children }
            }

            _ => panic!("invalid B+Tree node type"),
        }
    }
}
