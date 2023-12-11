use bincode::{self, Error};
use checksum::crc32::Crc32;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use uuid::Uuid;

pub mod flush;
use flush::Flusher;

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

#[derive(Deserialize, Serialize, Debug)]
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

pub fn write_to_file(logs: &[WalEntry], path: &Path, max_log_size: u64) -> Result<(), io::Error> {
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

pub fn flush_all_logs(wal_folder: &Path, flusher: &mut impl Flusher) {
    match get_all_log_files(wal_folder) {
        Ok(paths) => {
            for path in paths.iter() {
                if let Some(p) = path.to_str() {
                    match read_from_file(p) {
                        Ok(res) => apply_changes(res, flusher),
                        Err(err) => println!("Error occured: {}", err),
                    };
                }
            }
        }
        Err(_) => println!("Error occured when logs reading..."),
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

fn rotate_log_file(file: &mut File, path: &Path) -> Result<(), io::Error> {
    let parent = path.parent().unwrap_or(Path::new("."));
    let last_number = get_next_number_of_wal(path);
    let new_path = format!("{}/wal-{:04}.bin", parent.to_str().unwrap(), last_number);
    *file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(new_path)?;
    Ok(())
}

pub fn get_initial_number_of_wal(path: &Path) -> u32 {
    let mut initial_number: u32 = 0;
    match get_all_log_files(path) {
        Ok(logs) => {
            if logs.as_os_str().is_empty() {
                return 0;
            }
            for log in logs.iter() {
                if let Some(file_name) = log.to_str() {
                    if let Some(num) = get_wal_sequence(Path::new(file_name)) {
                        if initial_number < num {
                            initial_number = num;
                        }
                    }
                };
            }
            initial_number + 1
        }
        Err(_) => 0
    }
}

pub fn get_next_number_of_wal(path: &Path) -> u32 {
    if is_wal_file(path) {
        let file_name = path.file_name().unwrap();
        if let Some(wal_number) = file_name.to_str().map(|name| &name[4..8]) {
            return wal_number.parse().unwrap_or(0) + 1;
        }
    }
    0
}

fn verify_checksum(_entry: &WalEntry) -> bool {
    true
}

fn is_wal_file(file_path: &Path) -> bool {
    let prefix = "wal-";
    let suffix = ".bin";

    if let Some(file_name) = file_path.file_name() {
        if let Some(file_name_str) = file_name.to_str() {
            if file_name_str.starts_with(prefix) && file_name_str.ends_with(suffix) {
                let middle = &file_name_str[prefix.len()..(file_name_str.len() - suffix.len())];
                if middle.chars().all(|c| c.is_numeric()) && middle.len() == 4 {
                    return true;
                }
            }
        }
    }
    false
}

fn get_wal_sequence(path: &Path) -> Option<u32> {
    let prefix = "wal-";
    let suffix = ".bin";

    if is_wal_file(path) {
        let file_name = path.file_name()?.to_str()?;
        let middle = &file_name[prefix.len()..(file_name.len() - suffix.len())];
        middle.parse().ok()
    } else {
        None
    }
}

fn get_all_log_files(path: &Path) -> Result<PathBuf, io::Error> {
    let mut paths = PathBuf::new();
    for entry in fs::read_dir(path)? {
        let dir = entry?;
        if let Some(extension) = dir.path().extension() {
            if extension == "bin" {
                paths.push(dir.path());
            }
        }
    }
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::read_to_string,
        path::{Path, PathBuf},
    };
    use tempfile::tempdir;

    use crate::*;

    #[test]
    fn test_get_all_log_files() {
        let paths: PathBuf = ["./wal-0000.bin"].iter().collect();
        let path = Path::new(".");
        let log_file_paths = get_all_log_files(path).unwrap();
        assert_eq!(paths, log_file_paths)
    }

    #[test]
    fn test_is_wal_file() {
        let path1 = Path::new("wal-0001.bin");
        let path2 = Path::new("./");
        let path3 = Path::new("/etc/passwd");

        assert_eq!(true, is_wal_file(path1));
        assert_eq!(false, is_wal_file(path2));
        assert_eq!(false, is_wal_file(path3));
    }

    #[test]
    fn test_get_next_number_of_wal() {
        let path1 = Path::new("wal-0000.bin");
        let path2 = Path::new("wal-0123.bin");
        let path3 = Path::new("./");
        let path4 = Path::new("/etc/passwd");

        assert_eq!(1, get_next_number_of_wal(path1));
        assert_eq!(124, get_next_number_of_wal(path2));
        assert_eq!(0, get_next_number_of_wal(path3));
        assert_eq!(0, get_next_number_of_wal(path4));
    }

    #[test]
    fn test_rotate_log_file() {
        let tmp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = tmp_dir.path().join("wal-0000.bin");
        let mut tmp_file = File::create(&file_path).expect("Failed to create temp file");
        writeln!(tmp_file, "Brian was here. Briefly.").expect("Failed to write temp dir");
        rotate_log_file(&mut tmp_file, &file_path).expect("Rotation failed");
        let new_file_path = tmp_dir.path().join("wal-0001.bin");
        assert_eq!(new_file_path.exists(), true);
        let content_tmp_file = read_to_string(&file_path).expect("failed to read file");
        let rotated_tmp_file = read_to_string(&new_file_path).expect("failed to read file");
        assert_eq!("Brian was here. Briefly.\n", content_tmp_file);
        assert_eq!("", rotated_tmp_file);
    }

    #[test]
    fn test_get_file_size() {
        let tmp_dir = tempdir().expect("Failed to create temp dir");
        let file_path = tmp_dir.path().join("test_file.txt");
        let mut tmp_file = File::create(&file_path).expect("Failed to create temp file");
        let data = "hello world";
        write!(tmp_file, "{}", data).expect("Failed to write temp file");
        assert_eq!(get_file_size(&tmp_file).unwrap(), data.len() as u64)
    }

    #[test]
    fn test_get_initial_number_of_wal() {
        let tmp_dir = tempdir().expect("Failed to create temp dir");
        let init_number = get_initial_number_of_wal(&tmp_dir.path());
        assert_eq!(init_number, 0);

        let file_path = tmp_dir.path().join("wal-0000.bin");
        let _ = File::create(&file_path).expect("Failed to create temp file");

        let init_number = get_initial_number_of_wal(&tmp_dir.path());
        assert_eq!(init_number, 1);

        let file_path = tmp_dir.path().join("wal-0123.bin");
        let _ = File::create(&file_path).expect("Failed to create temp file");

        let init_number = get_initial_number_of_wal(&tmp_dir.path());
        assert_eq!(init_number, 124);
    }

    
    
}
