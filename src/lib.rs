use std::fmt;
use std::error;
use std::str;
use std::io::Write;

#[derive(Debug)]
pub enum DbError {
    MetaUnrecognized,
    StatementUnrecognized,
    StatementSyntaxError,
    TableFull,
    ParsingError(std::num::ParseIntError),
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DbError::MetaUnrecognized => write!(f, "Meta command unrecognized"),
            DbError::StatementUnrecognized => write!(f, "Statement unrecognized"),
            DbError::StatementSyntaxError => write!(f, "Statement has syntax error"),
            DbError::TableFull => write!(f, "Table is full"),
            DbError::ParsingError(ref err) => err.fmt(f),
        }
    }
}

impl error::Error for DbError {
    fn description(&self) -> &str {
        match *self {
            DbError::MetaUnrecognized => "Unrecognized",
            DbError::StatementUnrecognized => "Unrecognized",
            DbError::StatementSyntaxError => "Syntax Error",
            DbError::TableFull => "Table full",
            DbError::ParsingError(ref err) => err.description(),
        }
    }
    fn cause(&self) -> Option<&error::Error> {
        match *self {
            DbError::ParsingError(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::num::ParseIntError> for DbError {
    fn from(err: std::num::ParseIntError) -> DbError {
        DbError::ParsingError(err)
    }
}

const USERID_SIZE: usize = 32;
const EMAIL_SIZE: usize = 255;
const ROW_SIZE: usize = EMAIL_SIZE + USERID_SIZE + 4 + 2;
const PAGE_SIZE: usize = 4096;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_PAGES: usize = 100;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;


#[derive(Debug)]
struct Row {
    id: u32,
    user_id: String,
    email: String,
}

impl Row {
    fn deserialize(data : &[u8]) -> Row {
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

    fn serialize(&self, data : &mut [u8]) -> () {
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
    }
}

pub struct Table {
    pages: Vec<Vec<u8>>,
    num_rows : usize,
}

impl Table {
    pub fn init() -> Table {
        Table { pages: Vec::with_capacity(TABLE_MAX_PAGES), num_rows: 0}
    }
    fn add_row(&mut self, row : &Row) -> Result<(), DbError> {
        {
            let num_rows = self.num_rows;
            let row_data = try!(self.get_row(num_rows));
            row.serialize(row_data);
        }
        self.num_rows += 1;
        Ok(())
    }
    fn get_row(&mut self, row_num : usize) -> Result<&mut [u8], DbError> {
        let page_num = row_num / ROWS_PER_PAGE;
        if self.pages.len() >= TABLE_MAX_PAGES {
            return Err(DbError::TableFull);
        }
        while self.pages.len() <= page_num {
            self.pages.push(vec![0; PAGE_SIZE]);
        }
        let row_offset : usize = row_num % ROWS_PER_PAGE;
        let byte_offset : usize = row_offset * ROW_SIZE;
        return Ok(&mut self.pages[page_num][byte_offset..byte_offset+ROW_SIZE]);
    }
}


pub fn meta_command(_input : &str) -> Result<(), DbError> {
    Err(DbError::MetaUnrecognized)
}

pub fn statement_command(input : &str, table : &mut Table, 
                         writer : &mut Write) -> Result<(), DbError> {
    if input.starts_with("select") {
        for i in 0..table.num_rows {
            let r = Row::deserialize(&try!(table.get_row(i)));
            writer.write_fmt(format_args!("({}, {}, {})\n", 
                                          r.id, r.user_id, r.email)).unwrap();
        }
        writer.flush().unwrap();
    } else if input.starts_with("insert") {
        if table.num_rows >= TABLE_MAX_ROWS {
            return Err(DbError::TableFull);
        }
        let params : Vec<&str> = input.split_whitespace().collect();
        if params.len() != 4 {
            return Err(DbError::StatementSyntaxError);
        }
        let id = try!(params[1].parse::<u32>());
        if params[2].len() > USERID_SIZE || params[3].len() > EMAIL_SIZE {
            return Err(DbError::StatementSyntaxError);
        }
        let row = Row {
            id,
            user_id : String::from(params[2]),
            email : String::from(params[3]),
        };
        try!(table.add_row(&row));
    } else {
        return Err(DbError::StatementUnrecognized);
    }
    Ok(())
} 


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let mut table = Table::init();
        let mut buf : Vec<u8> = vec![];
        statement_command("insert 1 user1 person1@example.com", &mut table, &mut buf).unwrap();
        statement_command("select", &mut table, &mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), String::from("(1, user1, person1@example.com)\n"));
    }
}