use rocksdb::{DB,ColumnFamilyDescriptor,Options};
use rust_rocksdb_example::{IdGeneratorOptions,IdInstance};
use std::{time::{SystemTime, UNIX_EPOCH,Duration}, ops::Add, sync::{Arc, Mutex}};
use chrono::{offset::Local, TimeZone}; 
use chrono::{DateTime,Utc}; 
use tokio::*;

fn main() {

    let options = IdGeneratorOptions::new().machine_id(1).node_id(1);
    let _ = IdInstance::init(options);

    {
        let id =IdInstance::next_id();
        println!("{id}");
        println!("{}",IdInstance::format(id));
    }
    
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    let task1=  rt.spawn(async {
        println!("1 {}", IdInstance::next_id());
        println!("1 {}", IdInstance::next_id());
        println!("1 {}", IdInstance::next_id());
        println!("1 {}", IdInstance::next_id());
    });
    let task2= rt.spawn(async {
        println!("2 {}", IdInstance::next_id());
        println!("2 {}", IdInstance::next_id());
        println!("2 {}", IdInstance::next_id());
        println!("2 {}", IdInstance::next_id());
    });
    rt.handle();
  
    



    let path = "_path_for_rocksdb_storage_with_cfs";
    let mut cf_opts = Options::default();
    cf_opts.set_max_write_buffer_number(16);
    let cf = ColumnFamilyDescriptor::new("cf1", cf_opts);
   
   



    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    {
       let db = DB::open_cf_descriptors(&db_opts, path, vec![cf]).unwrap();
     
       let cf1=  db.cf_handle("cf1").unwrap();
       
       let key = 1234_i64.to_ne_bytes();
       db.put_cf(&cf1,&key, "value").unwrap();

       db.put_cf(&cf1,b"my key", b"my value2").unwrap();
       match db.get_pinned_cf(&cf1,b"my key") {
           Ok(Some(value)) => {
            println!("retrieved value {:?}", String::from_utf8(value.as_ref().to_vec()).unwrap());
            drop(value);
           },
           Ok(None) => println!("value not found"),
           Err(e) => println!("operational problem encountered: {}", e),
       }

       println!("key={} value{}",i64::from_be_bytes(key),String::from_utf8(db.get_pinned_cf(&cf1,&key).unwrap().unwrap().as_ref().to_vec()).unwrap());


       db.delete_cf(&cf1,b"my key").unwrap();
       db.delete_cf(&cf1,&key).unwrap();
    }


    let _ = DB::destroy(&Options::default(), path);
}

