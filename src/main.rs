extern crate simple_db;
use std::io::{self, Write};

fn main() {
    let mut table = simple_db::Table::init();
    loop {
        print!("db > ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input)
            .expect("DbError reading input\n");

        let input = input.trim();
        if input.starts_with(".exit") {
            break;
        }
        if input.starts_with(".") {
            match simple_db::meta_command(&input) {
                Ok(_) => continue,
                Err(err) => {
                    println!("{}", err);
                    continue;
                },
            }
        }
        else {
            match simple_db::statement_command(&input, &mut table) {
                Ok(_) => println!("Executed."),
                Err(err) => {
                    println!("{}", err);
                    continue;
                }
            }
        }
    }
}
