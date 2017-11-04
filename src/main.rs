use std::io::{self, Write};
use std::fmt;
use std::error;
use std::str;

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

#[derive(Debug)]
struct Row {
    id: u32,
    user_id: String,
    email: String,
}

const ROWSIZE: usize = 255 + 255 + 32 + 2;

impl Row {
    fn de_serialize(data : &[u8]) -> Row {
        let mut id : u32 = 0;
        id = id ^ (data[0] as u32);
        id = id ^ ((data[1] as u32) << 8);
        id = id ^ ((data[2] as u32) << 16);
        id = id ^ ((data[3] as u32) << 24);
        let user_id_len : usize = data[4] as usize;
        let email_len : usize = data[5] as usize;
        let user_id = str::from_utf8(&data[6..6+user_id_len]).unwrap();
        let email = str::from_utf8(&data[6+user_id_len..
                                         6+user_id_len+email_len]).unwrap();
        Row { 
            id, 
            user_id : user_id.to_string(), 
            email : email.to_string(), 
        }
    }

    // return a copy for now, will read a bit more
    // return Result.. error if any of string exceeds 255
    fn serialize(&self) -> [u8; ROWSIZE] {
        let mut data : [u8; ROWSIZE] = [0; ROWSIZE];
        data[0] = self.id as u8;
        data[1] = (self.id >> 8) as u8;
        data[2] = (self.id >> 16) as u8;
        data[3] = (self.id >> 24) as u8;
        let user_id_len = self.user_id.len();
        data[4] = user_id_len as u8;
        let email_len = self.email.len();
        data[5] = email_len as u8;
        data[6..6+user_id_len].copy_from_slice(self.user_id.as_bytes());
        data[6+user_id_len..6+user_id_len+email_len]
            .copy_from_slice(self.email.as_bytes());
        data
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

        // let row = Row {
            // id:3,
            // user_id:String::from("hello"),
            // email:String::from("world"),
        // };

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
