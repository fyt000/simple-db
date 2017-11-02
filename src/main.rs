use std::io::{self, Write};
use std::fmt;
use std::error;

#[derive(Debug)]
enum DbError {
    MetaUnrecognized,
    StatementUnrecognized,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DbError::MetaUnrecognized => write!(f, "Meta command unrecognized"),
            DbError::StatementUnrecognized => write!(f, "Statement unrecognized"),
        }
    }
}

impl error::Error for DbError {
    fn description(&self) -> &str {
        match *self {
            DbError::MetaUnrecognized => "Unrecognized",
            DbError::StatementUnrecognized => "Unrecognized",
        }
    }
    fn cause(&self) -> Option<&error::Error> {
        match *self {
            _ => None,
        }
    }
}

fn meta_command(_input : &str) -> Result<(), DbError> {
    Err(DbError::MetaUnrecognized)
}

fn statement_command(input : &str) -> Result<(), DbError> {
    if input.starts_with("select") {
        println!("This is where we would do a select");
    } else if input == "insert" {
        println!("This is where we would do a insert");
    } else {
        return Err(DbError::StatementUnrecognized);
    }
    Ok(())
} 

fn main() {
    loop {
        print!("db > ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input)
            .expect("DbError reading input\n");

        let input = input.to_lowercase();
        let input = input.trim();
        if input.starts_with(".exit") {
            break;
        }
        if input.starts_with(".") {
            match meta_command(&input) {
                Ok(_) => continue,
                Err(err) => {
                    println!("{}", err);
                    continue;
                },
            }
        }
        else {
            match statement_command(&input) {
                Ok(_) => println!("Executed."),
                Err(err) => {
                    println!("{}", err);
                    continue;
                }
            }
        }
    }
}
