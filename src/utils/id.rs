use std::hint::spin_loop;
use std::time::{SystemTime, UNIX_EPOCH};
use once_cell::sync::OnceCell;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;

/// 请确保machine_id和node_id小于32
pub struct IdGeneratorOptions{
    pub epoch: Option<SystemTime>,
    /// machine_id, is use to supplement id machine or sectionalization attribute.
    pub machine_id: i32,
    /// node_id, is use to supplement id machine-node attribute.
    pub node_id: i32,
}

impl IdGeneratorOptions {
    pub fn new() -> Self {
        IdGeneratorOptions {
            epoch: None,
            machine_id: 1,
            node_id: 1,
        }
    }
    /// 机器Id  [1..31]
    pub fn machine_id(mut self, machine_id: i32) -> Self {
        self.machine_id = machine_id;
        self
    }
    /// 生成的开始时间， 少于当前时间，保持不变
    pub fn epoch(mut self, epoch: SystemTime) -> Self {
        self.epoch = Some(epoch);
        self
    }
    /// 节点Id  [1..31]
    pub fn node_id(mut self, node_id: i32) -> Self {
        self.node_id = node_id;
        self
    }
}

/// Constructs a new `IdInstance` using the UNIX epoch.
/// Please make sure that machine_id and node_id is small than 32(2^5);
///
/// # Examples
///
/// ```
/// let options = IdGeneratorOptions::new().machine_id(1).node_id(1);
/// let _ = IdInstance::init(options);
/// let id = IdInstance::next_id();
/// ```
pub struct IdInstance;

impl IdInstance {
    /// Initialize the instance
    pub fn init(option: IdGeneratorOptions) {
        IdInstance::get_instance().lock().init(option)
    }

    /// Get a unique id
    pub fn next_id() -> i64 {
        IdInstance::get_instance().lock().lazy_generate()
    }

    fn get_instance() -> &'static Mutex<SnowflakeIdGenerator> {
        static INSTANCE: OnceCell<Mutex<SnowflakeIdGenerator>> = OnceCell::new();
     
        INSTANCE.get_or_init(|| Mutex::new(SnowflakeIdGenerator::default()))
    }
}

/// Instance of multiple generators contained in a vector
/// Constructs a new `IdInstance` using the UNIX epoch.
/// Please make sure that machine_id and node_id is small than 32(2^5);
///
/// # Examples
///
/// ```
/// let _ = IdInstance::init(1,1,None);
/// let id = IdInstance::next_id();
/// ```
pub struct IdVecInstance;

impl IdVecInstance {
    /// Initialize the instance
    ///
    /// Every time you call this function will drop all the previous generators in the instance.
    pub fn init(mut options: Vec<IdGeneratorOptions>)  {
        let mut instances = IdVecInstance::get_instance().write();
        instances.clear();
        for option in options.drain(..) {
            let mut instance = SnowflakeIdGenerator::default();
            instance.init(option);
            instances.push(Arc::new(Mutex::new(instance)));
        }
    }
    /// Get a unique id
    pub fn next_id(index: usize) -> i64 {
        // Because this step matters the speed a lot,
        // so we won't check the index and let it panic
        let reader = {
            let r = IdVecInstance::get_instance().read();
            Arc::clone(&r[index])
        };
        let id = reader.lock().lazy_generate();
        id
    }

    fn get_instance() -> &'static RwLock<Vec<Arc<Mutex<SnowflakeIdGenerator>>>> {
        static INSTANCE: OnceCell<RwLock<Vec<Arc<Mutex<SnowflakeIdGenerator>>>>> = OnceCell::new();
        INSTANCE.get_or_init(|| RwLock::new(Vec::new()))
    }
}




/// The `SnowflakeIdGenerator` type is snowflake algorithm wrapper.
#[derive(Debug, Clone, Copy)]
 struct SnowflakeIdGenerator {
    /// epoch used by the snowflake algorithm.
    epoch: SystemTime,

    /// last_time_millis, last time generate id is used times millis.
    last_time_millis: i64,

    /// machine_id, is use to supplement id machine or sectionalization attribute.
    machine_id: i32,

    /// node_id, is use to supplement id machine-node attribute.
    node_id: i32,

    /// auto-increment record.
    idx: u16,
}


impl Default for SnowflakeIdGenerator {
    fn default() -> Self {
        let last_time_millis = get_time_millis(UNIX_EPOCH);
        SnowflakeIdGenerator {
            epoch: UNIX_EPOCH,
            last_time_millis: last_time_millis,
            machine_id: 1,
            node_id: 1,
            idx: 0,
        }
    }
}


impl SnowflakeIdGenerator {
    
    fn init(&mut self,option: IdGeneratorOptions){
        if option.epoch.is_some(){
            self.last_time_millis= get_time_millis(option.epoch.unwrap());
        }
        self.machine_id = option.machine_id;
        self.node_id = option.node_id;
    }

    fn real_time_generate(&mut self) -> i64 {
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

    
    fn generate(&mut self) -> i64 {
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

    
    fn lazy_generate(&mut self) -> i64 {
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
fn get_time_millis(epoch: SystemTime) -> i64 {
    SystemTime::now()
        .duration_since(epoch)
        .expect("Time went mackward")
        .as_millis() as i64
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