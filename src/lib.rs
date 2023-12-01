use checksum::crc32::Crc32;
use std::fmt;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use uuid::Uuid;

#[derive(Debug)]
pub enum Operation {
    Insert,
    Update,
    Delete,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operation::Insert => write!(f, "Insert"),
            Operation::Update => write!(f, "Update"),
            Operation::Delete => write!(f, "Delete"),
        }
    }
}

pub struct WalEntry {
    pub key: String,
    pub value: String,
    pub timestamp: i64,
    pub transaction_id: String,
    pub operation: Operation,
    pub checksum: u32,
}

impl fmt::Display for WalEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}-{}-{:?}-{}-{}-{}\n",
            self.transaction_id,
            self.timestamp,
            self.operation,
            self.key,
            self.value,
            self.checksum
        )
    }
}

impl WalEntry {
    pub fn new(operation: Operation, key: String, value: String) -> WalEntry {
        let transaction_id = Uuid::new_v4().to_string();
        let timestamp = chrono::Utc::now().timestamp();
        let data = format!(
            "{}|{}|{}|{}|{}",
            key, value, timestamp, transaction_id, operation
        );
        let checksum = Crc32::new().checksum(data.as_bytes());

        WalEntry {
            key,
            value,
            timestamp,
            transaction_id,
            operation,
            checksum,
        }
    }
}

pub fn write_to_file(logs: &[WalEntry], path: String) {
    let mut file = match OpenOptions::new().append(true).create(true).open(path) {
        Ok(file) => file,
        Err(error) => panic!("Problem opening the data file: {:?}", error),
    };
    for log in logs.iter() {
        let buf = log.to_string();
        if let Err(error) = file.write_all(buf.as_bytes()) {
            eprintln!("Error writing to file: {:?}", error);
        }
    }
}

pub fn read_from_file(path: String) -> Result<Vec<WalEntry>, &'static str> {
    let logs: Vec<WalEntry> = Vec::new();
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) => panic!("Problem opening the data file: {:?}", error),
    };
    let mut reader = BufReader::new(file);
    let mut log_string = String::new();
    while reader.read_line(&mut log_string).unwrap() > 0 {
        print!("{}", log_string);
        log_string.clear();
    }
    Ok(logs)
}
