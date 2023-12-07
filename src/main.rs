use ironlog::{read_from_file, write_to_file, WalEntry, get_number_of_wal, flush_all_logs};
use eventual::Timer;
mod config;
mod flush;
use std::time::Duration;


struct Postgresql{}
impl ironlog::flush::Flusher for Postgresql {
    fn flush(&mut self, entry: WalEntry) -> std::io::Result<()> {
        println!("{} is flushed", entry);
        Ok(())
    }
}



fn main() {
    let mut logs: Vec<WalEntry> = Vec::new();
    let number_of_wal = get_number_of_wal();
    let path = format!("wal-{:04}.bin", number_of_wal);
    logs.push(WalEntry::new(
        ironlog::Operation::Insert,
        String::from("name"),
        String::from("bahoz"),
    ));
    logs.push(WalEntry::new(
        ironlog::Operation::Insert,
        String::from("age"),
        String::from("22"),
    ));
    logs.push(WalEntry::new(
        ironlog::Operation::Insert,
        String::from("gender"),
        String::from("male"),
    ));
    logs.push(WalEntry::new(
        ironlog::Operation::Insert,
        String::from("hobby"),
        String::from("coding"),
    ));
    logs.push(WalEntry::new(
        ironlog::Operation::Insert,
        String::from("hobby"),
        String::from("reading"),
    ));

    write_to_file(&logs, &path,1350);
    let logs = match read_from_file(&path){
        Ok(res) => res,
        Err(e) => panic!("{}", e),
    
    };

    logs.iter().for_each(|x| print!("{}", x));




    let mut postgres = Postgresql{};
    let config = config::WalConfig::new(Duration::new(30, 0), 1024);
    let timer = Timer::new();
    let ticks = timer.interval_ms(config.flush_interval.as_millis() as u32).iter();

    for _ in ticks {
        flush_all_logs(".", &mut postgres)
    }



}
