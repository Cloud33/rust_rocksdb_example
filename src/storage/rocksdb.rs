
use rocksdb::{DB,ColumnFamilyDescriptor,Options,ReadOptions,WriteOptions,BlockBasedIndexType, BlockBasedOptions,Cache,DBCompressionType,FlushOptions,Error};
use chrono::{Local, prelude::*};
use crate::utils::snowflake::ProcessUniqueId;
use std::{sync::{atomic::{AtomicUsize,Ordering}, Arc}, thread::sleep};
use async_trait::async_trait;
use super::Storage;

//
//https://doc.rust-lang.org/stable/std/mem/struct.ManuallyDrop.html

//CF
const CONFIG_CF: &str = "config";
// const HEADERS_CF: &str = "headers";
// const TXID_CF: &str = "txid";
// const FUNDING_CF: &str = "funding";
// const SPENDING_CF: &str = "spending";
// //CF List
// const COLUMN_FAMILIES: &[&str] = &[CONFIG_CF, HEADERS_CF, TXID_CF, FUNDING_CF, SPENDING_CF];

// key
const  ID_KEY: &[u8] = b"id";
// const CONFIG_KEY: &str = "C";
// const TIP_KEY: &[u8] = b"T";



// const KB: usize = 1_024;
// const MB: usize = 1_024 * KB;

// /// 缓冲区大小
// const DB_DEFAULT_COLUMN_MEMORY_BUDGET_MB: usize = 128;

// /// The default memory budget in MiB.
// const DB_DEFAULT_MEMORY_BUDGET_MB: usize = 512;
// //pub struct 
/// id 前缀 年份 yyyy
static ID_PERFIX_YY:AtomicUsize = AtomicUsize::new(0);
/// id 前缀 重启次数  1~100
static ID_PERFIX_STEP:AtomicUsize = AtomicUsize::new(0);
/// id 前缀 字面值
static ID_PERFIX:AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct RocksDB {
    pub db: Arc<DB>,
    pub user_write_opts: Arc<WriteOptions>,
    pub write_opts: Arc<WriteOptions>,
    pub sync_write_opts: Arc<WriteOptions>,
    pub read_opts: Arc<ReadOptions>,
}



impl RocksDB  {
    // 打开数据库
    pub fn open(path: &str) ->Self{
        let block_opts = generate_block_based_options();

        let cf_opts = generate_cf_options(&block_opts);
        let cf1 = ColumnFamilyDescriptor::new("cf1", cf_opts);

        let cf_opts = generate_cf_options(&block_opts);
        let cf2 = ColumnFamilyDescriptor::new("cf2", cf_opts);

        let cf_opts = generate_cf_options(&block_opts);
        let cf3 = ColumnFamilyDescriptor::new(CONFIG_CF, cf_opts);

        let user_write_opts = generate_user_write_options();
        let write_opts = generate_write_options();
        let sync_write_opts = generate_sync_write_options();
        let read_opts = generate_read_options();
        let db_opts = generate_options();
        let db = DB::open_cf_descriptors(&db_opts, path, vec![cf1,cf2,cf3]).unwrap();
        println!("db open end");
        Self { db: Arc::new(db),
            user_write_opts:Arc::new(user_write_opts),
            write_opts:Arc::new(write_opts), 
            sync_write_opts:Arc::new(sync_write_opts),
            read_opts:Arc::new(read_opts),}
    }
    // 初始化
    pub fn init(&self){
        self.id_handle()
    }

    pub fn flush(&self) -> Result<(), Error>{
        let flush_opts = generate_flush_options();
        let result= &self.db.flush_opt(&flush_opts);
        result.clone()
    }
    // 获取 key
    pub fn get_key() -> String {
        format!("{}_{}",ID_PERFIX.load(Ordering::Relaxed),ProcessUniqueId::new())
    }
    // id处理
    fn id_handle(&self){
        // 获取配置表引用
        let config_cf = self.db.cf_handle(CONFIG_CF).unwrap();
        //获取当前系统日期 yyyyMMdd
        let yy = Local::now().format("%Y%m%d").to_string().parse::<usize>().unwrap();
        //获取配置表中Id的值
        let value = self.db.get_pinned_cf(&config_cf,ID_KEY)
        .map(|x| x.map(|v| rmp_serde::from_slice::<IdValue>(&v.as_ref().to_vec()).unwrap())).unwrap();
      
        let value = match value {
            Some(mut v) => {
                //如果有值，判断是否是当前日期，如果是，step+1 代表今天是重启过
                if v.yy == yy {
                    println!("get id value: {:?}",v);
                    v.step = v.step + 1;
                }
                else{
                    //如果不是，设置等于今天日期，step = 0
                    v.yy = yy;
                    v.step = 0;
                }
                v
            },
            None => IdValue { yy: yy, step: 0},
        };
        //如果step 大于 99，代表今天已经重启100以上，不能再次启动
        if value.step > 99{
            print!("The number of restarts in the current day is greater than 100 and cannot be started");
        }
        println!("id value:  {:?}", value);
        //更新Id值
        self.db.put_cf_opt(&config_cf, ID_KEY, rmp_serde::to_vec(&value).unwrap(),&self.sync_write_opts).unwrap();
        //设置运行时的yy
        ID_PERFIX_YY.store(value.yy,Ordering::Relaxed); //Relaxed够了，不需要非常严格
        //设置运行时的step
        ID_PERFIX_STEP.store(value.step,Ordering::Relaxed);//Relaxed够了，不需要非常严格
        //设置运行时字面值
        let perfix = get_id_perfix();
        ID_PERFIX.store(perfix,Ordering::Relaxed);
        //启动Id刷新服务
        self.id_refresh();
    }
    // id 刷新服务
    fn id_refresh(&self){
        let db = self.db.clone();
        let sync_write_opts = self.sync_write_opts.clone();
        let  rt = tokio::runtime::Runtime::new().unwrap();
        rt.spawn(async move {
            loop {
                //获取当前时间
                let current_date = Local::now();
                println!("current_date:{}",current_date.format("%Y-%m-%d %H:%M:%S"));
                //获取当前时间+1天 明天
                let future_date= current_date + chrono::Duration::days(1);
                //获取 明天零时
                let end_time =Local.ymd(future_date.year(), future_date.month(), future_date.day()).and_hms_milli(0, 0, 0, 0);
                println!("end_time:{}",end_time.format("%Y-%m-%d %H:%M:%S"));
                //获取当前时间到明天零时的时间差
                let duration  = end_time - current_date;
                println!("duration:{}",duration.num_seconds());
                let current_date = current_date+ duration;
                println!("current_date:{}",current_date.format("%Y-%m-%d %H:%M:%S"));
                //回收没有使用的值
                drop(current_date);
                drop(future_date);
                //等待明天零时，任务被唤醒
                tokio::time::sleep(duration.to_std().unwrap()).await;
                // 开始执行刷新操作
                // 获取运行时年份
                let yy = ID_PERFIX_YY.load(Ordering::Relaxed);
                //获取运行时 重启次数
                let step = ID_PERFIX_STEP.load(Ordering::Relaxed);
                let config_cf = db.cf_handle(CONFIG_CF).unwrap();
                
                let value = db.get_pinned_cf(&config_cf,ID_KEY)
                .map(|x| x.map(|v| 
                    rmp_serde::from_slice::<IdValue>(&v.as_ref().to_vec()).unwrap())).unwrap().unwrap();
                //检查数据是否正确
                assert_eq!(yy,value.yy);
                assert_eq!(step,value.step);
                //设置新的数据
                let id_value = IdValue { yy: yy+1, step: 0};
                //更新到存储
                db.put_cf_opt(&config_cf, ID_KEY, rmp_serde::to_vec(&id_value).unwrap(),&sync_write_opts).unwrap();
                //更新运行时
                ID_PERFIX_YY.store(id_value.yy,Ordering::Relaxed);
                ID_PERFIX_STEP.store(id_value.step,Ordering::Relaxed);
                //更新计算值
                let perfix = get_id_perfix();
                ID_PERFIX.store(perfix,Ordering::Relaxed);
            }
        });
        rt.handle();
        //sleep(std::time::Duration::from_secs(1));
    }
}

impl Drop for RocksDB  {
    fn drop(&mut self) {
        println!("closing DB at {}", self.db.path().display());
    }
}

#[async_trait]
impl Storage for RocksDB  {
    async fn get_key(&self, table: &str, key : &str) -> String{
        //todo!()
        let db = self.db.clone();
        let read_opts=self.read_opts.clone();
        let table = table.to_owned();
        let key = key.to_owned();
        let task = tokio::task::spawn(async move {
            let cf = db.cf_handle(table.as_str()).unwrap();
            let value= db.get_cf_opt(&cf,key,&read_opts).unwrap();
            String::from_utf8(value.unwrap()).unwrap()
        });
        task.await.unwrap()
    }
}

// 获取Id前缀
fn get_id_perfix() -> usize {
    let id_value = IdValue { yy:ID_PERFIX_YY.load(Ordering::Relaxed),step:ID_PERFIX_STEP.load(Ordering::Relaxed)};
    let perfix = format!("{}",id_value);
    perfix.parse::<usize>().unwrap()
}
// 获取DB配置
fn generate_options() -> Options{
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true); //如果为 true，则将创建打开数据库时不存在的任何列系列。
    db_opts.create_if_missing(true); //如果为 true，则在缺少数据库时将创建数据库。
    db_opts.set_use_fsync(false);// 异步刷盘
    db_opts.set_report_bg_io_stats(true);//在压缩和刷新中测量 IO 统计信息
    db_opts.set_max_open_files(-1);//将 max_open_files 设置为 -1 可始终使所有文件保持打开状态，从而避免代价高昂的表缓存调用。
    db_opts.set_bytes_per_sync(1024 * 1024); //WAL 日志同步设置 1MB
    db_opts.set_keep_log_file_num(1); //指定要保留的信息日志文件的最大数量。 默认值：1000
    db_opts.set_max_background_jobs(4); //设置并发后台作业（压缩和刷新）的最大数量。
    db_opts
}
//获取表配置 ,只有运行时表需要这样的配置，归档不需要
fn generate_cf_options(block_opts: &BlockBasedOptions) -> Options {
    let mut cf_opts = Options::default();
    cf_opts.set_level_compaction_dynamic_level_bytes(true); //允许 RocksDB 为级别选择动态字节基数。打开此功能后，RocksDB 将自动调整每个级别的最大字节数
    cf_opts.set_block_based_table_factory(block_opts);
    cf_opts.optimize_level_style_compaction(128 * 1024 * 1024);//优化关卡风格压缩。 它设置缓冲区大小，以便内存消耗受到限制 ,目前 128 MB
    cf_opts.set_target_file_size_base(64 * 1024 * 1024); //L0-L1目标文件大小 SSD:64 * MB HDD:256 * MB
    cf_opts.set_compression_type(DBCompressionType::Lz4);  //Lz4 快
    cf_opts.set_bottommost_compression_type(DBCompressionType::Zstd); //设置将用于在最底部级别压缩块的最底部压缩算法
    cf_opts.set_bottommost_zstd_max_train_bytes(0, true); //设置在压缩最底部的级别时传递给 zstd 的字典训练器的训练数据的最大大小
    cf_opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(6)); //前缀提取器
    cf_opts.set_memtable_prefix_bloom_ratio(0.2); //将为每个大小为 （上限为 0.25） 的可表创建一个前缀绽放过滤器
    cf_opts.set_memtable_whole_key_filtering(true); //在内存中启用整个键绽放过滤器。请注意，仅当memtable_prefix_bloom_size_ratio不为 0 时，此操作才会生效。启用整个密钥筛选可能会降低点查找的 CPU 使用率。
    cf_opts.set_max_write_buffer_number(4); // 设置内存中建立的最大写入缓冲区数。默认值和最小值为 2，因此当将 1 个写入缓冲区刷新到存储时，新写入操作可以继续写入另一个写入缓冲区。如果max_write_buffer_number > 3，则写入速度将减慢到options.delayed_write_rate如果我们写入允许的最后一个写入缓冲区。
    cf_opts
}
// 获取块的配置
fn generate_block_based_options() -> BlockBasedOptions{
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_format_version(5);  //使用最新的数据版本
    block_opts.set_block_size(16 * 1024);  //block 大小  SSD:16 * KB  HDD: 64 * KB  
    block_opts.set_block_restart_interval(16); //用于密钥增量编码的重新启动点之间的密钥数，默认16
    block_opts.set_index_type(BlockBasedIndexType::HashSearch);  //索引类型 哈希索引 ,哈希索引以加快前缀查找
    //let cache_size = config.memory_budget() / 3; //建议这应该是总内存预算的 1/3 左右
    let cache_size = 128 << 20;
    let cache = Cache::new_lru_cache(cache_size).expect("create cache failed");
    block_opts.set_block_cache(&cache);  //创建所选大小的块缓存，以缓存未压缩的数据。建议这应该是总内存预算的 1/3 左右, 128 MB
  
    block_opts.set_cache_index_and_filter_blocks(true);  //缓存索引
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);// 缓存索引
   
    block_opts.set_hybrid_ribbon_filter(10.0,2);  // 设置混合功能区筛选器策略以减少磁盘读取。在给定级别之前使用“绽放”滤镜，对所有其他级别使用“功能区”滤镜。这会将功能区筛选器节省的内存与布隆筛选器的较低 CPU 使用率相结合。
    block_opts
}
//获取读取配置
fn generate_read_options() -> ReadOptions{
    let mut read_opts = ReadOptions::default();
    read_opts.set_verify_checksums(false);
    read_opts
}
//获取flush配置
fn generate_flush_options() -> FlushOptions{
    let mut flush_opts = FlushOptions::default();
    flush_opts.set_wait(true);
    flush_opts
}

//同步写
fn generate_sync_write_options() -> WriteOptions{
    let mut write_options = WriteOptions::default();
    write_options.set_sync(true);  //同步写，则在将写入视为完成之前，将从操作系统缓冲区缓存中刷新写入操作。如果此标志为真，则写入速度会变慢。
    write_options.set_no_slowdown(false); //如果为 true，并且我们需要等待或休眠以写入请求，则立即失败
    write_options
}
//用户级别写
fn generate_user_write_options () -> WriteOptions {
    let mut write_options = WriteOptions::default();
    write_options.set_sync(false);  //同步写，则在将写入视为完成之前，将从操作系统缓冲区缓存中刷新写入操作。如果此标志为真，则写入速度会变慢。
    write_options.set_no_slowdown(false); //如果为 true，并且我们需要等待或休眠以写入请求，则立即失败
    write_options
}

//正常别写
fn generate_write_options () -> WriteOptions {
    let mut write_options = WriteOptions::default();
    write_options.set_sync(false);  //同步写，则在将写入视为完成之前，将从操作系统缓冲区缓存中刷新写入操作。如果此标志为真，则写入速度会变慢。
    write_options.set_no_slowdown(false); //如果为 true，并且我们需要等待或休眠以写入请求，则立即失败
    write_options
}


#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct IdValue {
    yy: usize,
    step: usize,
}

impl std::fmt::Display for IdValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let step = {
            if self.step <= 9 {
                format!("00{}", self.step)
            } else if self.step >= 10 && self.step <= 99 {
                format!("0{}", self.step)
            } else {
                panic!("The number of restarts in the current day is greater than 100, and the ID cannot be generated!")
            }
        };
        write!(f, "{}{}", self.yy, step)
    }
}
