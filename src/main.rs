use chrono::Local;
use rocksdb::{Direction, IteratorMode, Options, ReadOptions, DB};

use rust_rocksdb_example::storage::rocksdb::{DBStore, id_refresh};
use serde::{Deserialize, Serialize};

fn main() {
    let serverid = 1;
    let path = "_path_for_rocksdb_storage_with_cfs";

    let dbstore = DBStore::open(path);
    dbstore.init();
    {
        let db = dbstore.db.clone();

        let id= DBStore::get_key();
        println!("{id}");
        let id= DBStore::get_key();
        println!("{id}");
        let id= DBStore::get_key();
        println!("{id}");
        let id= DBStore::get_key();
        println!("{id}");
        let id= DBStore::get_key();
        println!("{id}");
        let id= DBStore::get_key();
        println!("{id}");
        let id= DBStore::get_key();
        println!("{id}");
       
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
    dbstore.flush();
    drop(dbstore);
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
