
use std::io::Result;
use crate::WalEntry;


pub trait Flusher {
    fn flush(&mut self, entry: WalEntry) -> Result<()>;
}
