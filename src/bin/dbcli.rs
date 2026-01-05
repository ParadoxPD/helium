use helium::{
    api::db::{Database, QueryResult},
    debugger::{DebugLevel, set_debug_level},
};
use rustyline::{DefaultEditor, Editor, Result, error::ReadlineError};
use std::process;

fn main() -> Result<()> {
    // ---------- Debug flags ----------
    let args: Vec<String> = std::env::args().collect();
    let debug_level = args
        .iter()
        .find(|arg| arg.starts_with("--debug="))
        .and_then(|arg| arg.strip_prefix("--debug="))
        .and_then(|v| v.parse::<u8>().ok())
        .map(DebugLevel::from_u8)
        .unwrap_or(DebugLevel::Off);

    set_debug_level(debug_level);

    // ---------- Database ----------
    let mut db = Database::new("/tmp/test.db".to_string());

    println!("Helium DB CLI");
    println!("Type SQL. End with ';'.");
    println!("Commands: .exit");
    println!("---------------------");

    // ---------- Line editor ----------
    let mut rl = DefaultEditor::new()?;

    let mut buffer = String::new();

    Ok(loop {
        let prompt = if buffer.is_empty() {
            "helium> "
        } else {
            "....> "
        };

        match rl.readline(prompt) {
            Ok(line) => {
                let line = line.trim();

                // ---------- Meta commands ----------
                if buffer.is_empty() && line.starts_with('.') {
                    if handle_meta_command(line) {
                        continue;
                    } else {
                        break;
                    }
                }

                buffer.push_str(line);
                buffer.push(' ');

                if !line.ends_with(';') {
                    continue;
                }

                rl.add_history_entry(buffer.clone()).ok();

                // ---------- Execute SQL ----------
                match db.query(&buffer) {
                    Ok(QueryResult::Rows(rows)) => {
                        for row in rows {
                            println!("{row:?}");
                        }
                    }
                    Ok(QueryResult::Explain(plan)) => {
                        println!("{plan}");
                    }
                    Ok(QueryResult::Ok) => {}
                    Err(err) => {
                        eprintln!("Error: {err}");
                    }
                }

                buffer.clear();
            }

            Err(ReadlineError::Interrupted) => {
                println!("^C");
                buffer.clear();
            }

            Err(ReadlineError::Eof) => {
                println!();
                break;
            }

            Err(err) => {
                eprintln!("Read error: {err}");
                break;
            }
        }
    })
}

fn handle_meta_command(cmd: &str) -> bool {
    match cmd {
        ".exit" | ".quit" => {
            process::exit(0);
        }
        ".help" => {
            println!("Available commands:");
            println!("  .exit     Exit CLI");
            println!("  .help     Show this help");
            true
        }
        _ => {
            println!("Unknown command: {cmd}");
            true
        }
    }
}

