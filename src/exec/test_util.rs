use crate::common::value::Value;
use crate::exec::operator::Row;

pub fn qrow(alias: &str, cols: &[(&str, Value)]) -> Row {
    let mut r = Row::new();
    for (c, v) in cols {
        r.insert(format!("{c}"), v.clone());
    }
    r
}
