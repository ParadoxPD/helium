use crate::types::value::Value;

/// A total-orderable key usable by B+Tree.
/// NO NULLs allowed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKey {
    Int(i64),
    Bool(bool),
    String(String),
}

impl TryFrom<&Value> for IndexKey {
    type Error = &'static str;

    fn try_from(v: &Value) -> Result<Self, Self::Error> {
        match v {
            Value::Int64(v) => Ok(IndexKey::Int(*v)),
            Value::Boolean(v) => Ok(IndexKey::Bool(*v)),
            Value::String(v) => Ok(IndexKey::String(v.clone())),
            Value::Null => Err("NULL cannot be indexed"),
            _ => Err("unsupported value type for index"),
        }
    }
}

impl IndexKey {
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            IndexKey::Int(v) => {
                buf.push(0);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            IndexKey::Bool(v) => {
                buf.push(1);
                buf.push(*v as u8);
            }
            IndexKey::String(s) => {
                buf.push(2);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }

    pub fn deserialize(input: &mut &[u8]) -> Self {
        let tag = input[0];
        *input = &input[1..];

        match tag {
            0 => {
                let (n, rest) = input.split_at(8);
                *input = rest;
                IndexKey::Int(i64::from_le_bytes(n.try_into().unwrap()))
            }
            1 => {
                let v = input[0] != 0;
                *input = &input[1..];
                IndexKey::Bool(v)
            }
            2 => {
                let (len, rest) = input.split_at(4);
                let len = u32::from_le_bytes(len.try_into().unwrap()) as usize;
                let (s, rest2) = rest.split_at(len);
                *input = rest2;
                IndexKey::String(String::from_utf8(s.to_vec()).unwrap())
            }
            _ => panic!("invalid IndexKey tag"),
        }
    }
}
