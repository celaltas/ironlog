use ironlog::{read_from_file, write_to_file, WalEntry};

fn main() {
    let mut logs: Vec<WalEntry> = Vec::new();
    let path = String::from("test.txt");
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

    write_to_file(&logs, path.clone());
    let logs = match read_from_file(path.clone()){
        Ok(res) => res,
        Err(e) => panic!("{}", e),
    
    };

    logs.iter().for_each(|x| println!("{}", x));



}
