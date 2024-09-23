use std::io;
use std::io::prelude::*;
use std::fs;
use std::fs::{File, OpenOptions};
use std::net::TcpStream;
use std::env;
use std::collections::HashMap;
use std::path::Path;
use std::time::SystemTime;
extern crate chrono;
use chrono::{Duration, Local, Datelike, Weekday};
use now::DateTimeNow;
use ssh2::Session;
use flate2::read::GzDecoder;


const CAPACITY: usize = 10240;



fn main() {

    let _guard = sentry::init(("https://sentry.ru/795", sentry::ClientOptions {
        release: sentry::release_name!(),
        ..Default::default()
    }));

    sentry::capture_message("Im start!", sentry::Level::Info);


    let second: u64 = 604800;
    let search_str = String::from("Long Duration");
    let dirs = String::from("/mnt/ARCH1/BACKUP/BWKS/");
    let file = "/tmp/bwks.csv";
    let file_upload = "/www/bwks_mon/bwks.csv";
    let host_port_upl = "127.0.0.1:22";
    let mut scores_cdr:HashMap<String, i32> = HashMap::new();
    let now = Local::now();
    let duration_since_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let timestamp_nanos = duration_since_epoch.as_nanos(); // u128
    let week_start = now.beginning_of_week();  
    
    let current_day = Local::now();
    let current_day_num:i64 = current_day.weekday().number_from_monday().into();
    let week_day = current_day.weekday();
    let fri = Weekday::Fri;

if week_day != fri {
    println!("longduration,stream=bwks,id=0i count=0i {}", timestamp_nanos);
    return;
}

    let mut date_vector: Vec<String> = Vec::new();

    let user = env::var("A_USER").unwrap();
    let pass = env::var("A_PASS").unwrap();

    //let user = "";
    //let pass = "";


if Path::new(file).exists() {
        fs::remove_file(file).unwrap();
      }
    let _ = File::create(file);
    
for n in 0..current_day_num {
    let wsplus = week_start + Duration::days(n);
    let wsplus =  wsplus.format("%Y%m%d");
    let wsplus =  wsplus.to_string();
        date_vector.push(wsplus); 
    }
    
  
    
    for element in date_vector { 
        let current_dir = dirs.to_owned() + &element;

    for entry in fs::read_dir(&current_dir).unwrap() {
        let entry = entry;
        let path = entry.expect("path error").path();        
        let metadata = fs::metadata(&path).unwrap(); 
    
    if metadata.is_file(){
        let mtime = metadata.modified().expect("modifed metadata error").elapsed().expect("elapsed metadata error").as_secs();
        

    if mtime < second {
        let in_fh = std::fs::File::open(&path).unwrap();
        let in_gz = GzDecoder::new(in_fh);
        let in_buf = io::BufReader::with_capacity(CAPACITY, in_gz);
        let fname = &path.file_name().expect("error file name").to_str().unwrap();
        let mut concat_string = String::new();
        let mut count  = 1;


    for line in in_buf.lines() {
            let line = line.unwrap();


        if line.contains(&search_str) {   
            for part in line.split(",").filter(|&x| !x.is_empty()) {
                
                if count == 1 || count == 7 || count == 9 || count == 10{

                if count == 1{
                       concat_string = fname.to_string() + ";" + part;                  
                }
                else{
                       concat_string = concat_string.to_owned() + ";" + part;
                }
                
                
                }
                
            if count == 10 {

                scores_cdr.entry(concat_string.clone()).and_modify(|count| *count += 1).or_insert(1);
            }
                
                count += 1;
        }
    
            }

            

               
    }

        }
       
    }

}
    }


    if scores_cdr.is_empty(){
        println!("longduration,stream=bwks,id=0i count=0i {}", timestamp_nanos);
        return;

  }
  else{
        println!("longduration,stream=bwks,id=0i count=1i {}", timestamp_nanos);
  }

  for (string, num) in &scores_cdr {
            
            let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(file)
            .unwrap();
    
        if let Err(e) = writeln!(file, "{}", string.to_owned() + ";" + &num.to_string()) {
            eprintln!("Couldn't write to file: {}", e);
        }
      }
 
      //upload to sftp

      let tcp = TcpStream::connect(host_port_upl).unwrap();
       let mut sess = Session::new().unwrap();
              sess.set_tcp_stream(tcp);
              sess.set_compress(true);
              sess.timeout();
              sess.set_timeout(3000);
              sess.handshake().unwrap();
              sess.userauth_password(&user, &pass).unwrap();
      let sftp = sess.sftp().unwrap();
      let mut local_file = File::open(file).expect("no file found");
      let mut buffer:Vec<u8> = Vec::new();
      let _ :u64 = local_file.read_to_end(&mut buffer).unwrap().try_into().unwrap();
            sftp.create(&Path::new(file_upload))
          .unwrap()
          .write_all(&buffer)
          .unwrap();



}
