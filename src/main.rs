use chrono::Local;
use rocksdb::{Direction, IteratorMode, Options, ReadOptions, DB};

use rust_rocksdb_example::storage::{rocksdb::RocksDB,Storage,SupportedDatabase};
use serde::{Deserialize, Serialize};


fn main() {
    let serverid = 1;
    let path = "_path_for_rocksdb_storage_with_cfs";

    let dbstore = RocksDB::open(path);
    dbstore.init();
    {
        let db = dbstore.db.clone();
        let cf2 = db.cf_handle("cf2").unwrap();
        db.put_cf(&cf2, b"test", "test111").unwrap();
        let id= RocksDB::get_key();
        println!("{id}");
        let id= RocksDB::get_key();
        println!("{id}");
        let id= RocksDB::get_key();
        println!("{id}");
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let db1 = dbstore.db.clone();
        let otps1 = dbstore.read_opts.clone();
        rt.block_on(async {
            println!("tokio started");
            let mut tasks = Vec::new();
            let time = tokio::time::Instant::now();
            for i in 0..100 {
                let db2 = db1.clone();
                let otps2= otps1.clone();
                let i = i;
                let task= tokio::spawn(async move {
                    let cf= db2.cf_handle("cf2").unwrap();
                    let _= db2.get_cf_opt(&cf,b"test",&otps2).unwrap();
                    // let text = String::from_utf8(value.unwrap()).unwrap();
                    // println!("{} value={}",i,text);
                });
                tasks.push(task);
            }
            for task in tasks{
                task.await.unwrap();
            }
            //tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            //10000  54ms
            //1000 6ms
            //100 1ms
            println!("tokio end 耗时 {}",time.elapsed().as_millis());
            //为什么不行?
            //let dbs:SupportedDatabase = SupportedDatabase::RocksDB(dbstore.clone());
            //let value = spawn_readers(&dbs.clone()).await;
            for i in 0..10{
                // 使用引用，而不是拷贝 -> &dbstore.clone()
                let value = spawn_readers(&dbstore).await;
                println!("{} value : {}",i,value);
            }
            
           
        });

        
        
        //测试多线程Id生成

        //    let  rt = tokio::runtime::Runtime::new().unwrap();
        //    for _ in 0..100 {
        //        rt.spawn(async {
        //             println!("{}",ProcessUniqueId::new());
        //        });
        //    }
        //    rt.handle();
        //   let mut value = IdValue{ yy: String::from("20220511"),step:1};
        //   println!("{:?}",value);
        //   let buf = rmp_serde::to_vec(&value).unwrap();
        //     value.step=2;
        //   println!("{:?}",buf);
        //   let value2 = rmp_serde::from_slice::<IdValue>(&buf).unwrap();
        //   println!("{:?}",value2);

        let cf1 = db.cf_handle("cf1").unwrap();
        db.put_cf(&cf1, b"20220511_abc", "20220511_1").unwrap();
        db.put_cf(&cf1, b"20220511_abd", "20220511_2").unwrap();
        db.put_cf(&cf1, b"20220512_abc", "20220512_3").unwrap();


        let cf2 = db.cf_handle("cf2").unwrap();
        db.put_cf(&cf2, b"1", "20220511_1").unwrap();
        db.put_cf(&cf2, b"2", "20220511_2").unwrap();
        db.put_cf(&cf2, b"3", "20220512_3").unwrap();

        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(false);

        let perfix = b"20220511";
        let end_prefix = get_end_prefix(perfix);
        println!(
            "{}",
            String::from_utf8(end_prefix.clone().unwrap().to_vec()).unwrap()
        );
        if let Some(end_prefix) = end_prefix {
            read_opts.set_iterate_upper_bound(end_prefix);
        }
        let iter = db.iterator_cf_opt(
            &cf1,
            read_opts,
            IteratorMode::From(perfix, Direction::Forward),
        );
        for (key, value) in iter {
            println!(
                "Saw {} {:?}",
                String::from_utf8(key.to_vec()).unwrap(),
                String::from_utf8(value.to_vec()).unwrap()
            );
        }

        let mut read_opts = ReadOptions::default();
        read_opts.set_verify_checksums(false);
        let perfix = b"20220511_abc";
        let end_prefix = get_end_prefix(perfix);
        println!(
            "{}",
            String::from_utf8(end_prefix.clone().unwrap().to_vec()).unwrap()
        );
        if let Some(end_prefix) = end_prefix {
            read_opts.set_iterate_upper_bound(end_prefix);
        }
        let iter = db.iterator_cf_opt(
            &cf1,
            read_opts,
            IteratorMode::From(perfix, Direction::Forward),
        );
        for (key, value) in iter {
            println!(
                "Saw {} {:?}",
                String::from_utf8(key.to_vec()).unwrap(),
                String::from_utf8(value.to_vec()).unwrap()
            );
        }

        db.drop_cf("cf1").unwrap();
        db.drop_cf("cf2").unwrap();
    }
    println!("end");
    dbstore.flush();
    drop(dbstore);
    println!("drop end");
    //清除所有数据
    //let _ = DB::destroy(&Options::default(), path);
}

pub fn get_end_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end_range = prefix.to_vec();
    while let Some(0xff) = end_range.last() {
        end_range.pop();
    }
    if let Some(byte) = end_range.last_mut() {
        *byte += 1;
        Some(end_range)
    } else {
        None
    }
}

async fn spawn_readers (db: &impl Storage) -> String {
    db.get_key("cf2","test").await
}