use rocksdb::{DB,ColumnFamilyDescriptor,Options};
use std::{time::{SystemTime, UNIX_EPOCH, Duration}, ops::Add,};
use std::hint::spin_loop;


fn main() {
    let path = "_path_for_rocksdb_storage_with_cfs";

    let time = SystemTime::now();
    let bb= time
        .duration_since(UNIX_EPOCH)
        .expect("Time went mackward")
        .as_millis() as i64;
    println!("{bb}");

    let time = time.add(Duration::from_secs(365*24*60*60));
    let bb= time
    .duration_since(UNIX_EPOCH)
    .expect("Time went mackward")
    .as_millis() as i64;
    println!("{bb}");
    let time = time.add(Duration::from_secs(1*24*60*60));
    let bb= time
    .duration_since(UNIX_EPOCH)
    .expect("Time went mackward")
    .as_millis() as i64;
    println!("{bb}");
    let time = time.add(Duration::from_secs(100*365*24*60*60));
    let bb= time
    .duration_since(UNIX_EPOCH)
    .expect("Time went mackward")
    .as_millis() as i64;
    println!("{bb}");
    let mut id_generator = snowflake::SnowflakeIdGenerator::with_epoch(1, 1, UNIX_EPOCH);
    let id= id_generator.lazy_generate();
    println!("{id}");
    let discord_epoch = UNIX_EPOCH + Duration::from_millis(1420070400000);
    let mut id_generator = snowflake::SnowflakeIdGenerator::with_epoch(1, 1, discord_epoch);
    let id= id_generator.lazy_generate();
    println!("{id}");
    
    let mut id_generator2 = SnowflakeIdGenerator2::new(1,1);
    let id= id_generator2.lazy_generate();
    println!("{id}");
    let id= id_generator2.lazy_generate();
    println!("{id}");
    let id= id_generator2.lazy_generate();
    println!("{id}");
    let id= id_generator2.lazy_generate();
    println!("{id}");
    let id= id_generator2.lazy_generate();
    println!("{id}");
    let id= id_generator2.lazy_generate();
    println!("{id}");
    
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



/// The `SnowflakeIdGenerator` type is snowflake algorithm wrapper.
#[derive(Copy, Clone, Debug)]
pub struct SnowflakeIdGenerator2 {
    /// epoch used by the snowflake algorithm.
    epoch: SystemTime,

    /// last_time_millis, last time generate id is used times millis.
    last_time_millis: i64,

    /// machine_id, is use to supplement id machine or sectionalization attribute.
    pub machine_id: i32,

    /// node_id, is use to supplement id machine-node attribute.
    pub node_id: i32,

    /// auto-increment record.
    idx: u16,
}

impl SnowflakeIdGenerator2 {
    /// Constructs a new `SnowflakeIdGenerator` using the UNIX epoch.
    /// Please make sure that machine_id and node_id is small than 32(2^5);
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake::SnowflakeIdGenerator2;
    ///
    /// let id_generator = SnowflakeIdGenerator2::new(1, 1);
    /// ```
    pub fn new(machine_id: i32, node_id: i32) -> SnowflakeIdGenerator2 {
        Self::with_epoch(machine_id, node_id, UNIX_EPOCH)
    }

    /// Constructs a new `SnowflakeIdGenerator` using the specified epoch.
    /// Please make sure that machine_id and node_id is small than 32(2^5);
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::{Duration, UNIX_EPOCH};
    /// use snowflake::SnowflakeIdGenerator;
    ///
    /// // 1 January 2015 00:00:00
    /// let discord_epoch = UNIX_EPOCH + Duration::from_millis(1420070400000);
    /// let id_generator = SnowflakeIdGenerator::with_epoch(1, 1, discord_epoch);
    /// ```
    pub fn with_epoch(machine_id: i32, node_id: i32, epoch: SystemTime) -> SnowflakeIdGenerator2 {
        //TODO:limit the maximum of input args machine_id and node_id
        let last_time_millis = get_time_millis(epoch);

        SnowflakeIdGenerator2 {
            epoch,
            last_time_millis,
            machine_id,
            node_id,
            idx: 0,
        }
    }

    /// The real_time_generate keep id generate time is eq call method time.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake::SnowflakeIdGenerator;
    ///
    /// let mut id_generator = SnowflakeIdGenerator::new(1, 1);
    /// id_generator.real_time_generate();
    /// ```
    pub fn real_time_generate(&mut self) -> i64 {
        self.idx = (self.idx + 1) % 4096;

        let mut now_millis = get_time_millis(self.epoch);

        // supplement code for 'clock is moving backwards situation'.

        // If the milliseconds of the current clock are equal to
        // the number of milliseconds of the most recently generated id,
        // then check if enough 4096 are generated,
        // if enough then busy wait until the next millisecond.
        if now_millis == self.last_time_millis {
            if self.idx == 0 {
                now_millis = biding_time_conditions(self.last_time_millis, self.epoch);
                self.last_time_millis = now_millis;
            }
        } else {
            self.last_time_millis = now_millis;
            self.idx = 0;
        }

        // last_time_millis is 64 bits，left shift 22 bit，store 42 bits ， machine_id left shift 17 bits，
        // node_id left shift 12 bits ,idx complementing bits.
        self.last_time_millis << 22
            | ((self.machine_id << 17) as i64)
            | ((self.node_id << 12) as i64)
            | (self.idx as i64)
    }

    /// The basic guarantee time punctuality.
    ///
    /// Basic guarantee time punctuality.
    /// sometimes one millis can't use up 4096 ID, the property of the ID isn't real-time.
    /// But setting time after every 4096 calls.
    /// # Examples
    ///
    /// ```
    /// use snowflake::SnowflakeIdGenerator;
    ///
    /// let mut id_generator = SnowflakeIdGenerator::new(1, 1);
    /// id_generator.generate();
    /// ```
    pub fn generate(&mut self) -> i64 {
        self.idx = (self.idx + 1) % 4096;

        // Maintenance `last_time_millis` for every 4096 ids generated.
        if self.idx == 0 {
            let mut now_millis = get_time_millis(self.epoch);

            if now_millis == self.last_time_millis {
                now_millis = biding_time_conditions(self.last_time_millis, self.epoch);
            }

            self.last_time_millis = now_millis;
        }

        //last_time_millis is 64 bits，left shift 22 bit，store 42 bits ， machine_id left shift 17 bits，
        //node_id left shift 12 bits ,idx complementing bits.
        self.last_time_millis << 22
            | ((self.machine_id << 17) as i64)
            | ((self.node_id << 12) as i64)
            | (self.idx as i64)
    }

    /// The lazy generate.
    ///
    /// Lazy generate.
    /// Just start time record last_time_millis it consume every millis ID.
    /// Maybe faster than standing time.
    /// # Examples
    ///
    /// ```
    /// use snowflake::SnowflakeIdGenerator;
    ///
    /// let mut id_generator = SnowflakeIdGenerator::new(1, 1);
    /// id_generator.lazy_generate();
    /// ```
    pub fn lazy_generate(&mut self) -> i64 {
        self.idx = (self.idx + 1) % 4096;

        if self.idx == 0 {
            self.last_time_millis += 1;
        }

        self.last_time_millis << 22
            | ((self.machine_id << 17) as i64)
            | ((self.node_id << 12) as i64)
            | (self.idx as i64)
    }
}

#[inline(always)]
/// Get the latest milliseconds of the clock.
pub fn get_time_millis(epoch: SystemTime) -> i64 {
    220509121313888 as i64
}

#[inline(always)]
// Constantly refreshing the latest milliseconds by busy waiting.
fn biding_time_conditions(last_time_millis: i64, epoch: SystemTime) -> i64 {
    let mut latest_time_millis: i64;
    loop {
        latest_time_millis = get_time_millis(epoch);
        if latest_time_millis > last_time_millis {
            return latest_time_millis;
        }
        spin_loop();
    }
}