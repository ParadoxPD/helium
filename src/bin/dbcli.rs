use std::io::{self, Write};

use helium::api::db::{Database, QueryResult};
use helium::common::value::Value;
use helium::exec::operator::Row;

fn main() {
    let mut db = Database::new("/tmp/test.db".to_string());

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
            Ok(result) => match result {
                QueryResult::Explain(_) => {}
                QueryResult::Rows(rows) => {
                    for row in rows {
                        println!("{row:?}");
                    }
                }
                other => println!("{:?}", other),
            },
            Err(error) => {
                println!("{:?}", error);
            }
        }
    }
}
