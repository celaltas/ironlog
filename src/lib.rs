use bincode;
use bincode::Error;
use checksum::crc32::Crc32;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use uuid::Uuid;
pub mod flush;
use flush::Flusher;
use std::fs;

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
        writeln!(
            f,
            "{}-{}-{:?}-{}-{}-{}",
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

pub fn write_to_file(logs: &[WalEntry], path: &str, max_log_size: u64) -> Result<(), io::Error> {
    let mut file = OpenOptions::new().append(true).create(true).open(path)?;

    for log in logs {
        let serialized_entry = match log.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => {
                eprintln!("Error serializing entry: {:?}", error);
                continue;
            }
        };

        if let Some(file_size) = get_file_size(&file) {
            if file_size + serialized_entry.len() as u64 > max_log_size {
                rotate_log_file(&mut file, path)?;
            }
        }

        if let Err(error) = file.write_all(&serialized_entry) {
            eprintln!("Error writing to file: {:?}", error);
        } else if let Err(error) = file.write_all(b"\n") {
            eprintln!("Error writing newline to file: {:?}", error);
        }
    }

    Ok(())
}

pub fn read_from_file(path: &str) -> Result<Vec<WalEntry>, &'static str> {
    let mut file =
        File::open(path).unwrap_or_else(|err| panic!("Problem opening the data file: {:?}", err));
    let mut serialized_logs: Vec<u8> = Vec::new();

    match file.read_to_end(&mut serialized_logs) {
        Ok(_) => (),
        Err(_) => return Err("Error reading file"),
    };

    let lines = serialized_logs
        .split(|b| b == &0xA)
        .map(|line| line.strip_suffix(&[0xD]).unwrap_or(line));

    let mut a: Vec<WalEntry> = Vec::new();
    for line in lines {
        match WalEntry::from_bytes(line) {
            Ok(log) => a.push(log),
            Err(_) => {
                eprintln!("Error parsing log entry");
                continue;
            }
        }
    }
    Ok(a)
}

pub fn flush_all_logs(wal_folder: &str, flusher: &mut impl Flusher) {
    match get_all_log_files(wal_folder) {
        Ok(paths) => {
            for path in paths {
                if let Some(p) = path.to_str() {
                    match read_from_file(p) {
                        Ok(res) => apply_changes(res, flusher),
                        Err(err) => println!("Error occured: {}", err),
                    };
                }
            }
        }
        Err(err) => println!("Error occured when logs reading..."),
    };
}

pub fn apply_changes(entries: Vec<WalEntry>, flusher: &mut impl Flusher) {
    for entry in entries {
        if verify_checksum(&entry) {
            let transaction = entry.transaction_id.clone();
            match flusher.flush(entry) {
                Ok(_) => println!("Flushing log {} is succesfully", transaction),
                Err(err) => println!("Flushing log {} is failed, err", err),
            };
        } else {
            println!("The log {} checksum not verified", entry.transaction_id)
        }
    }
}

fn get_file_size(file: &File) -> Option<u64> {
    match file.metadata() {
        Ok(m) => Some(m.len()),
        Err(_) => None,
    }
}

fn rotate_log_file(file: &mut File, path: &str) -> Result<(), io::Error> {
    let last_number = get_last_number_of_wal(path);
    let new_path = format!("wal-{:04}.bin", last_number);
    *file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(new_path)?;
    Ok(())
}

pub fn get_number_of_wal() -> u32 {
    0
}

pub fn get_last_number_of_wal(path: &str) -> u32 {
    let wal_number = &path[4..8];
    let last_number: u32 = wal_number.parse().unwrap();
    last_number + 1
}

fn verify_checksum(entry: &WalEntry) -> bool {
    true
}

fn get_all_log_files(path: &str) -> Result<Vec<PathBuf>, io::Error> {
    let mut files: Vec<PathBuf> = vec![];
    for entry in fs::read_dir(path)? {
        let dir = entry?;
        files.push(dir.path())
    }
    Ok(files)
}
