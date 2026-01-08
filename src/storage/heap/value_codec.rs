use crate::types::value::Value;

pub struct ValueCodec;

impl ValueCodec {
    pub fn serialize(value: &Value, buf: &mut Vec<u8>) {
        match value {
            Value::Null => buf.push(0),
            Value::Int64(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Value::Float64(v) => {
                buf.push(2);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Value::Bool(v) => {
                buf.push(3);
                buf.push(if *v { 1 } else { 0 });
            }
            Value::String(s) => {
                buf.push(4);
                let bytes = s.as_bytes();
                let len = bytes.len() as u32;
                buf.extend_from_slice(&len.to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }
    }

    pub fn deserialize(buf: &mut &[u8]) -> Value {
        let tag = buf[0];
        *buf = &buf[1..];

        match tag {
            0 => Value::Null,
            1 => {
                let (num, rest) = buf.split_at(8);
                *buf = rest;
                Value::Int64(i64::from_le_bytes(num.try_into().unwrap()))
            }
            2 => {
                let (num, rest) = buf.split_at(8);
                *buf = rest;
                Value::Float64(f64::from_le_bytes(num.try_into().unwrap()))
            }
            3 => {
                let v = buf[0] != 0;
                *buf = &buf[1..];
                Value::Bool(v)
            }
            4 => {
                let (len_bytes, rest) = buf.split_at(4);
                let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
                let (str_bytes, rest2) = rest.split_at(len);
                *buf = rest2;
                Value::String(String::from_utf8(str_bytes.to_vec()).unwrap())
            }
            _ => panic!("unknown Value tag {}", tag),
        }
    }
}
