use rocksdb::{DB,ColumnFamilyDescriptor,Options,ReadOptions,BlockBasedIndexType, BlockBasedOptions,Cache,DBCompressionType,IteratorMode,Direction};
use rust_rocksdb_example::utils::id::*;

fn main() {

    // let options = IdGeneratorOptions::new().machine_id(1).node_id(1);
    // let _ = IdInstance::init(options);
    // let id = IdInstance::next_id();
    // println!("{}",id);
    // let str= IdInstance::format(id);
    // println!("{}",str);
    // let str= IdInstance::format(id);
    // println!("{}",str);


    let path = "_path_for_rocksdb_storage_with_cfs";

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


    let mut cf_opts = Options::default();
    cf_opts.set_level_compaction_dynamic_level_bytes(true); //允许 RocksDB 为级别选择动态字节基数。打开此功能后，RocksDB 将自动调整每个级别的最大字节数
    cf_opts.set_block_based_table_factory(&block_opts);
    cf_opts.optimize_level_style_compaction(128 * 1024 * 1024);//优化关卡风格压缩。 它设置缓冲区大小，以便内存消耗受到限制 ,目前 128 MB
    cf_opts.set_target_file_size_base(64 * 1024 * 1024); //L0-L1目标文件大小 SSD:64 * MB HDD:256 * MB
    cf_opts.set_compression_type(DBCompressionType::Lz4);  //Lz4 快
    cf_opts.set_bottommost_compression_type(DBCompressionType::Zstd); //设置将用于在最底部级别压缩块的最底部压缩算法
    cf_opts.set_bottommost_zstd_max_train_bytes(0, true); //设置在压缩最底部的级别时传递给 zstd 的字典训练器的训练数据的最大大小
    cf_opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(6)); //前缀提取器
    cf_opts.set_memtable_prefix_bloom_ratio(0.2); //将为每个大小为 （上限为 0.25） 的可表创建一个前缀绽放过滤器
    cf_opts.set_memtable_whole_key_filtering(true); //在内存中启用整个键绽放过滤器。请注意，仅当memtable_prefix_bloom_size_ratio不为 0 时，此操作才会生效。启用整个密钥筛选可能会降低点查找的 CPU 使用率。
    cf_opts.set_max_write_buffer_number(4); // 设置内存中建立的最大写入缓冲区数。默认值和最小值为 2，因此当将 1 个写入缓冲区刷新到存储时，新写入操作可以继续写入另一个写入缓冲区。如果max_write_buffer_number > 3，则写入速度将减慢到options.delayed_write_rate如果我们写入允许的最后一个写入缓冲区。
    let cf1 = ColumnFamilyDescriptor::new("cf1", cf_opts);
   
    let mut cf_opts = Options::default();
    cf_opts.set_level_compaction_dynamic_level_bytes(true); //允许 RocksDB 为级别选择动态字节基数。打开此功能后，RocksDB 将自动调整每个级别的最大字节数
    cf_opts.set_block_based_table_factory(&block_opts);
    cf_opts.optimize_level_style_compaction(128 * 1024 * 1024);//优化关卡风格压缩。 它设置缓冲区大小，以便内存消耗受到限制 ,目前 128 MB
    cf_opts.set_target_file_size_base(64 * 1024 * 1024); //L0-L1目标文件大小 SSD:64 * MB HDD:256 * MB
    cf_opts.set_compression_type(DBCompressionType::Lz4);  //Lz4 快
    cf_opts.set_bottommost_compression_type(DBCompressionType::Zstd); //设置将用于在最底部级别压缩块的最底部压缩算法
    cf_opts.set_bottommost_zstd_max_train_bytes(0, true); //设置在压缩最底部的级别时传递给 zstd 的字典训练器的训练数据的最大大小
    cf_opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(6)); //前缀提取器
    cf_opts.set_memtable_prefix_bloom_ratio(0.2); //将为每个大小为 （上限为 0.25） 的可表创建一个前缀绽放过滤器
    cf_opts.set_memtable_whole_key_filtering(true); //在内存中启用整个键绽放过滤器。请注意，仅当memtable_prefix_bloom_size_ratio不为 0 时，此操作才会生效。启用整个密钥筛选可能会降低点查找的 CPU 使用率。
    cf_opts.set_max_write_buffer_number(4); // 设置内存中建立的最大写入缓冲区数。默认值和最小值为 2，因此当将 1 个写入缓冲区刷新到存储时，新写入操作可以继续写入另一个写入缓冲区。如果max_write_buffer_number > 3，则写入速度将减慢到options.delayed_write_rate如果我们写入允许的最后一个写入缓冲区。
    let cf2 = ColumnFamilyDescriptor::new("cf2", cf_opts);

    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true); //如果为 true，则将创建打开数据库时不存在的任何列系列。
    db_opts.create_if_missing(true); //如果为 true，则在缺少数据库时将创建数据库。
    db_opts.set_use_fsync(false);// 异步刷盘
    db_opts.set_report_bg_io_stats(true);//在压缩和刷新中测量 IO 统计信息
    db_opts.set_max_open_files(-1);//将 max_open_files 设置为 -1 可始终使所有文件保持打开状态，从而避免代价高昂的表缓存调用。
    db_opts.set_bytes_per_sync(1024 * 1024); //WAL 日志同步设置 1MB
    db_opts.set_keep_log_file_num(1); //指定要保留的信息日志文件的最大数量。 默认值：1000
    db_opts.set_max_background_jobs(4); //设置并发后台作业（压缩和刷新）的最大数量。
    {
       let db = DB::open_cf_descriptors(&db_opts, path, vec![cf1,cf2]).unwrap();
     
       let cf1=  db.cf_handle("cf1").unwrap();
       db.put_cf(&cf1,b"20220511_abc", "20220511_1").unwrap();
       db.put_cf(&cf1,b"20220511_abd", "20220511_2").unwrap();
       db.put_cf(&cf1,b"20220512_abc", "20220512_3").unwrap();

       let cf2=  db.cf_handle("cf2").unwrap();
       db.put_cf(&cf2,b"20220511_abc", "20220511_1").unwrap();
       db.put_cf(&cf2,b"20220511_abd", "20220511_2").unwrap();
       db.put_cf(&cf2,b"20220512_abc", "20220512_3").unwrap();

       let mut read_opts = ReadOptions::default();
       read_opts.set_verify_checksums(false);
       
       let perfix = b"20220511";
       let end_prefix = get_end_prefix(perfix);
       println!("{}",String::from_utf8(end_prefix.clone().unwrap().to_vec()).unwrap());
       if let Some(end_prefix) = end_prefix {
            read_opts.set_iterate_upper_bound(end_prefix);
       }
       let iter =   db.iterator_cf_opt(&cf1, read_opts,IteratorMode::From(perfix, Direction::Forward));
       for (key, value) in iter {
          println!("Saw {} {:?}", String::from_utf8(key.to_vec()).unwrap(), String::from_utf8(value.to_vec()).unwrap());
       }

       let mut read_opts = ReadOptions::default();
       read_opts.set_verify_checksums(false);
       let perfix = b"20220511_abc";
       let end_prefix = get_end_prefix(perfix);
       println!("{}",String::from_utf8(end_prefix.clone().unwrap().to_vec()).unwrap());
       if let Some(end_prefix) = end_prefix {
            read_opts.set_iterate_upper_bound(end_prefix);
       }
       let iter =   db.iterator_cf_opt(&cf1, read_opts,IteratorMode::From(perfix, Direction::Forward));
       for (key, value) in iter {
          println!("Saw {} {:?}", String::from_utf8(key.to_vec()).unwrap(), String::from_utf8(value.to_vec()).unwrap());
       }
        
       db.drop_cf("cf1").unwrap();
       db.drop_cf("cf2").unwrap();
    }


    let _ = DB::destroy(&Options::default(), path);
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