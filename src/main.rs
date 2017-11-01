use std::io::{self, Write};


fn main() {
    loop {
        print!("db > ");
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin().read_line(&mut input)
            .expect("Error reading input\n");

        match input.trim() {
            ".exit" => break,
            other => println!("Unrecognized command '{}'.", other),
        }
    }
}
