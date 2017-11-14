extern crate tempdir;

use std::fmt;
use std::error;
use std::str;
use std::io::Write;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::PathBuf;


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
            DbError::StatementSyntaxError => 
                write!(f, "Statement has syntax error"),
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

const USERID_SIZE: usize = 31;
const EMAIL_SIZE: usize = 254;
// Store size of email/id instead of null terminating
// This means we need 2 extra bytes for serialization,
// and we still need a paging table of some sort to actually
// make this dynamic sizing useful...
// To sync with the tutorial, I am going to use 31 and 254
// as the userid and email size instead of 32 and 255
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

struct Pager {
    file : File,
    file_length : u64,
    pages: Vec<Vec<u8>>,
}

// do I need a drop for Pager so file gets dropped?
impl Pager {
    fn open(filename : PathBuf) -> Pager {
        let file = OpenOptions::new().read(true)
                                     .write(true)
                                     .create(true)
                                     .open(filename)
                                     .expect("Cannot open persistent file");
        let meta = file.metadata().expect("Cannot open file metadata");
        let mut pager = Pager {
            file,
            file_length : meta.len(),
            pages: Vec::with_capacity(TABLE_MAX_PAGES),
        };
        for _i in 0..TABLE_MAX_PAGES {
            // vec![] should be of capacity 0
            pager.pages.push(vec![]);
        }
        pager
    }

    fn get(&mut self, page_num : usize) -> &mut [u8] {
        if page_num > TABLE_MAX_PAGES {
            panic!("Tried to fetch page number out of bounds. {} > {}\n", 
                   page_num, TABLE_MAX_PAGES);
        }
        if self.pages[page_num].len() == 0 {
            self.pages[page_num] = vec![0; PAGE_SIZE];
            let mut num_pages : u64 = self.file_length / PAGE_SIZE as u64;
            if self.file_length % PAGE_SIZE as u64 != 0 {
                num_pages += 1;
            }
            if (page_num as u64) < num_pages {
                let start_offset = (page_num * PAGE_SIZE) as u64;  
                self.file.seek(SeekFrom::Start(start_offset))
                    .expect("Unable to read page from file");
                // if this is the last page, and not full
                // then we can only read whatever we have
                let mut size = PAGE_SIZE;
                if self.file_length < start_offset + (size as u64) {
                    size = (self.file_length - start_offset) as usize;
                }
                self.file.read_exact(&mut self.pages[page_num][..size])
                    .expect("Unable to read page from file");
            }
        }
        return &mut self.pages[page_num][..]; 
    }

    fn flush(&mut self, page_num : usize, size : usize) {
        if self.pages[page_num].len() == 0 {
            return;
        }
        self.file.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
            .expect("Cannot write to file");
        self.file.write_all(&self.pages[page_num][..size])
            .expect("Cannot write to file");
    }

}

pub struct Table {
    pager : Pager,
    num_rows : usize,
}

impl Table {
    pub fn db_open(filename : PathBuf) -> Table {
        let pager = Pager::open(filename);
        // the tutorial is wrong
        // let num_rows = pager.file_length / ROW_SIZE as u64;
        let file_length = pager.file_length as usize; //well..
        let pages = file_length / PAGE_SIZE;
        let additional = (file_length - (pages * PAGE_SIZE)) / ROW_SIZE;
        let num_rows = (additional + pages * ROWS_PER_PAGE);
        Table {
            pager,
            num_rows : num_rows as usize, 
        } 
    }

    fn add_row(&mut self, row : &Row) -> Result<(), DbError> {
        {
            let num_rows = self.num_rows;
            let row_data = self.get_row(num_rows)?;
            row.serialize(row_data);
        }
        self.num_rows += 1;
        Ok(())
    }
    fn get_row(&mut self, row_num : usize) -> Result<&mut [u8], DbError> {
        let page_num = row_num / ROWS_PER_PAGE;
        if page_num >= TABLE_MAX_PAGES {
            return Err(DbError::TableFull);
        }
        let row_offset : usize = row_num % ROWS_PER_PAGE;
        let byte_offset : usize = row_offset * ROW_SIZE;
        return Ok(&mut self.pager.get(page_num)[byte_offset..byte_offset+ROW_SIZE]);
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        let full_pages = self.num_rows / ROWS_PER_PAGE;
        for i in 0..full_pages {
            self.pager.flush(i, PAGE_SIZE);
        }
        let additional_rows = self.num_rows % ROWS_PER_PAGE;
        if additional_rows > 0 {
            self.pager.flush(full_pages, additional_rows * ROW_SIZE);
        }
    }
}


pub fn meta_command(_input : &str) -> Result<(), DbError> {
    Err(DbError::MetaUnrecognized)
}

pub fn statement_command(input : &str, table : &mut Table, 
                         writer : &mut Write) -> Result<(), DbError> {
    if input.starts_with("select") {
        for i in 0..table.num_rows {
            let r = Row::deserialize(&(table.get_row(i)?));
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
        let id = params[1].parse::<u32>()?;
        if params[2].len() > USERID_SIZE || params[3].len() > EMAIL_SIZE {
            return Err(DbError::StatementSyntaxError);
        }
        let row = Row {
            id,
            user_id : String::from(params[2]),
            email : String::from(params[3]),
        };
        table.add_row(&row)?;
    } else {
        return Err(DbError::StatementUnrecognized);
    }
    Ok(())
} 


#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time};
    use tempdir::TempDir;
    #[test]
    fn it_works() {
        let tmp_dir = TempDir::new("simple-db").unwrap();
        let file_path = tmp_dir.path().join("test1.db");
        let mut table = Table::db_open(file_path);
        let mut buf : Vec<u8> = vec![];
        statement_command("insert 1 user1 person1@example.com", 
                          &mut table, &mut buf).unwrap();
        statement_command("select", &mut table, &mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), 
                   String::from("(1, user1, person1@example.com)\n"));
    }

    #[test]
    fn table_max() {
        let tmp_dir = TempDir::new("simple-db").unwrap();
        let file_path = tmp_dir.path().join("test1.db");
        let mut table = Table::db_open(file_path);
        for i in 0..1400 {
            let mut buf : Vec<u8> = vec![];
            let insert_str = format!("insert {} user{} person{}@example.com", 
                                     i, i, i );
            statement_command(&insert_str, &mut table, &mut buf).unwrap();
        }
        let mut buf : Vec<u8> = vec![];
        statement_command("select", &mut table, &mut buf).unwrap(); 
        let mut idx = 0;
        let whole_str = String::from_utf8(buf).unwrap();
        let lines = whole_str.lines();
        for rec in lines {
            assert_eq!(rec, format!("({}, user{}, person{}@example.com)", 
                                    idx, idx, idx));
            idx += 1;
        }
        assert_eq!(idx, 1400);
    }

    #[test]
    #[should_panic(expected = "Table is full")]
    fn table_full() {
        let tmp_dir = TempDir::new("simple-db").unwrap();
        let file_path = tmp_dir.path().join("test1.db");
        let mut table = Table::db_open(file_path);
        for _i in 0..1401 {
            let mut buf : Vec<u8> = vec![];
            match statement_command("insert 1 user1 person1@example.com", 
                                    &mut table, &mut buf) {
                Ok(_) => (),
                Err(DbError::TableFull) => panic!("Table is full"),
                _ => panic!("incorrect panic"),
            }
        }
    }

    #[test]
    fn long_name() {
        let tmp_dir = TempDir::new("simple-db").unwrap();
        let file_path = tmp_dir.path().join("test1.db");
        let mut table = Table::db_open(file_path);
        let mut buf : Vec<u8> = vec![];
        let long_user = "a".repeat(31);
        let long_email = "a".repeat(254);
        let long_insert = format!("insert 1 {} {}", long_user, long_email);
        statement_command(long_insert.as_str(), &mut table, &mut buf).unwrap();
        statement_command("select", &mut table, &mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), 
                   format!("(1, {}, {})\n", long_user, long_email));
    }

    #[test]
    #[should_panic(expected = "uint parse error")]
    fn uint_parse() {
        let tmp_dir = TempDir::new("simple-db").unwrap();
        let file_path = tmp_dir.path().join("test1.db");
        let mut table = Table::db_open(file_path);
        let mut buf : Vec<u8> = vec![];
        match statement_command("insert -1 x x", &mut table, &mut buf) {
            Ok(_) => (),
            Err(DbError::ParsingError(_)) => panic!("uint parse error"),
            _ => panic!("incorrect panic"),
        }
    }

    #[test]
    fn table_max_persist() {
        let tmp_dir = TempDir::new("simple-db").unwrap();
        for total_lines in 0..1400 {
            let path1 = tmp_dir.path().join(format!("test{}.db",total_lines));
            let path2 = path1.clone();
            {
                let mut table = Table::db_open(path1);
                for i in 0..total_lines {
                    let mut buf : Vec<u8> = vec![];
                    let insert_str = format!("insert {} user{} person{}@example.com", 
                                            i, i, i );
                    statement_command(&insert_str, &mut table, &mut buf).unwrap();
                }
            }
            {
                let mut table = Table::db_open(path2);
                let mut buf : Vec<u8> = vec![];
                statement_command("select", &mut table, &mut buf).unwrap(); 
                let mut idx = 0;
                let whole_str = String::from_utf8(buf).unwrap();
                let lines = whole_str.lines();
                for rec in lines {
                    assert_eq!(rec, format!("({}, user{}, person{}@example.com)", 
                                            idx, idx, idx));
                    idx += 1;
                }
                assert_eq!(idx, total_lines);
            }

        }

    }
}