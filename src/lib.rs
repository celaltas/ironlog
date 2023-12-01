use bincode;
use bincode::Error;
use checksum::crc32::Crc32;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Deserialize, Serialize)]
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

    fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        bincode::serialize(self)
    }

    fn from_bytes(bytes: &[u8]) -> Result<WalEntry, Error> {
        bincode::deserialize(bytes)
    }
}

pub fn write_to_file(logs: &[WalEntry], path: String) {
    let mut file = match OpenOptions::new().append(true).create(true).open(path) {
        Ok(file) => file,
        Err(error) => panic!("Problem opening the data file: {:?}", error),
    };
    for log in logs.iter() {
        let serialized_entry = match log.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => panic!("Problem serializing entry: {:?}", error),
        };
        if let Err(error) = file.write_all(&serialized_entry) {
            eprintln!("Error writing to file: {:?}", error);
        }
    }
}

pub fn read_from_file(path: String) -> Result<Vec<WalEntry>, &'static str> {
    let mut logs: Vec<WalEntry> = Vec::new();
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) => panic!("Problem opening the data file: {:?}", error),
    };
    let mut reader = BufReader::new(file);
    let mut deserialized_entry: Vec<u8> = Vec::new();
    let _ = reader.read_until(b'\n', &mut deserialized_entry);
    let entry = match WalEntry::from_bytes(&deserialized_entry) {
        Ok(entry) => entry,
        Err(error) => panic!("Problem deserializing entry: {:?}", error),
    };
    logs.push(entry);
    Ok(logs)
}
