use std::io::{self, Write};

use helium::api::db::{Database, QueryResult};
use helium::debugger::{DebugLevel, set_debug_level};

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let debug_level = args
        .iter()
        .find(|arg| arg.starts_with("--debug="))
        .and_then(|arg| arg.strip_prefix("--debug="))
        .and_then(|level| level.parse::<u8>().ok())
        .map(DebugLevel::from_u8)
        .unwrap_or(DebugLevel::Off);

    set_debug_level(debug_level);

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
