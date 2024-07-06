use flate2::read::GzDecoder;
use std::io;
use std::io::prelude::*;
use std::fs;
use std::collections::HashMap;
extern crate chrono;
use chrono::Utc;
use std::time::{SystemTime};

const CAPACITY: usize = 10240;


fn main() {

    let second: u64 = 600;
    let search_str = String::from("Long Duration");
    let dirs = String::from("/mnt/ARCH1/BACKUP/BWKS/");
    let mut scores:HashMap<String, i32> = HashMap::new();
    let now = Utc::now();
    let dirtime = now.format("%Y%m%d").to_string();
    let current_dir = dirs.to_owned() + &dirtime;
    let duration_since_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let timestamp_nanos = duration_since_epoch.as_nanos(); // u128


    for entry in fs::read_dir(&current_dir).unwrap() {
        let entry = entry;
        let path = entry.expect("REASON").path();        
        let metadata = fs::metadata(&path).unwrap(); 
    
    if metadata.is_file(){
        let mtime = metadata.modified().expect("modifed metadata error").elapsed().expect("elapsed metadata metadata error").as_secs();
        

    if mtime < second {
        let in_fh = std::fs::File::open(&path).unwrap();
        let in_gz = GzDecoder::new(in_fh);
        let in_buf = io::BufReader::with_capacity(CAPACITY, in_gz);

    for line in in_buf.lines() {
            let line = line.unwrap();
            
        if line.contains(&search_str) {   
            
            let Some((a, _))  = line.split_once(',') else { todo!() };
            let record_id = a.to_string();

                scores.entry(record_id).and_modify(|count| *count += 1).or_insert(1);
            
            }


    }
        }
       
    }

}

    if scores.is_empty(){
        println!("longduration,stream=bwks,id=0 count=0 {}", timestamp_nanos);
        return;

  }

    for (string, num) in &scores {
            println!("longduration,stream=bwks,id={} count={}i {}", string, num, timestamp_nanos);
      }


}
