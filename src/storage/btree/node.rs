use crate::{
    common::value::Value,
    storage::{
        btree::{
            internal::{DiskInternalNode, InternalNode},
            leaf::{DiskLeafNode, LeafNode},
        },
        page::{PageId, RowId},
    },
};

pub trait Index: Send + Sync {
    fn insert(&mut self, key: IndexKey, rid: RowId);
    fn delete(&mut self, key: &IndexKey, rid: RowId);
    fn get(&self, key: &IndexKey) -> Vec<RowId>;
    fn range(&self, from: &IndexKey, to: &IndexKey) -> Vec<RowId>;
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum IndexKey {
    Int64(i64),
    String(String),
}

impl IndexKey {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            IndexKey::Int64(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }

            IndexKey::String(s) => {
                buf.push(2);
                let bytes = s.as_bytes();
                let len = bytes.len() as u16;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }

    pub fn deserialize(buf: &mut &[u8]) -> Self {
        assert!(!buf.is_empty(), "buffer underflow in IndexKey::deserialize");

        let tag = buf[0];
        *buf = &buf[1..];

        match tag {
            1 => {
                let (num, rest) = buf.split_at(8);
                *buf = rest;
                IndexKey::Int64(i64::from_le_bytes(num.try_into().unwrap()))
            }

            2 => {
                let (len_bytes, rest) = buf.split_at(2);
                let len = u16::from_le_bytes(len_bytes.try_into().unwrap()) as usize;

                let (str_bytes, rest2) = rest.split_at(len);
                *buf = rest2;

                let s = String::from_utf8(str_bytes.to_vec()).expect("invalid UTF-8 in IndexKey");
                IndexKey::String(s)
            }

            _ => panic!("unknown IndexKey tag {}", tag),
        }
    }
}

impl TryFrom<&Value> for IndexKey {
    type Error = ();

    fn try_from(v: &Value) -> Result<Self, ()> {
        match v {
            Value::Int64(i) => Ok(IndexKey::Int64(*i)),
            Value::String(s) => Ok(IndexKey::String(s.clone())),
            _ => Err(()),
        }
    }
}

pub type NodeId = PageId;

#[derive(Clone)]
pub enum BPlusNode {
    Internal(InternalNode),
    Leaf(LeafNode),
}
#[derive(Clone)]
pub enum DiskBPlusNode {
    Internal(DiskInternalNode),
    Leaf(DiskLeafNode),
}

#[cfg(test)]
mod tests {
    use crate::storage::btree::node::IndexKey;

    #[test]
    fn index_key_roundtrip() {
        let keys = vec![
            IndexKey::Int64(42),
            IndexKey::Int64(-7),
            IndexKey::String("abc".into()),
            IndexKey::String("longer_index_key".into()),
        ];

        for k in keys {
            let mut buf = Vec::new();
            k.serialize(&mut buf);

            let mut slice = buf.as_slice();
            let k2 = IndexKey::deserialize(&mut slice);

            assert_eq!(k, k2);
            assert!(slice.is_empty());
        }
    }
}
