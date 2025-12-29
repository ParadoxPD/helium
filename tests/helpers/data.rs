use helium::common::value::Value;
use helium::exec::operator::Row;

#[allow(dead_code)]
pub fn users() -> Vec<Row> {
    vec![
        row(&[
            ("id", Value::Int64(1)),
            ("name", Value::String("Alice".into())),
            ("age", Value::Int64(30)),
            ("active", Value::Bool(true)),
        ]),
        row(&[
            ("id", Value::Int64(2)),
            ("name", Value::String("Bob".into())),
            ("age", Value::Int64(15)),
            ("active", Value::Bool(false)),
        ]),
    ]
}

#[allow(dead_code)]
pub fn orders() -> Vec<Row> {
    vec![
        row(&[
            ("id", Value::Int64(1)),
            ("user_id", Value::Int64(1)),
            ("amount", Value::Int64(200)),
        ]),
        row(&[
            ("id", Value::Int64(2)),
            ("user_id", Value::Int64(1)),
            ("amount", Value::Int64(20)),
        ]),
    ]
}

fn row(kvs: &[(&str, Value)]) -> Row {
    let mut r = Row::new();
    for (k, v) in kvs {
        r.insert(k.to_string(), v.clone());
    }
    r
}
