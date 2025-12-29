use std::io::{self, Write};

use helium::api::db::{Database, QueryResult};
use helium::common::value::Value;
use helium::exec::operator::Row;

fn main() {
    let mut db = Database::new();

    db.insert_table(
        "users",
        vec!["name".into(), "age".into(), "score".into()],
        vec![
            row(&[
                ("name", Value::String("Alice".into())),
                ("age", Value::Int64(30)),
                ("score", Value::Int64(80)),
            ]),
            row(&[
                ("name", Value::String("Bob".into())),
                ("age", Value::Int64(15)),
                ("score", Value::Int64(90)),
            ]),
            row(&[
                ("name", Value::String("Carol".into())),
                ("age", Value::Int64(40)),
                ("score", Value::Int64(40)),
            ]),
        ],
    );

    println!("Helium DB CLI");
    println!("Type SQL and press enter. Ctrl+C to exit");

    loop {
        print!("helium> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        if io::stdin().read_line(&mut input).is_err() {
            break;
        }

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        let result = db.query(input);

        match result {
            QueryResult::Explain(_) => {}
            QueryResult::Rows(rows) => {
                for row in rows {
                    println!("{row:?}");
                }
            }
        }
    }
}

fn row(pairs: &[(&str, Value)]) -> Row {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}
