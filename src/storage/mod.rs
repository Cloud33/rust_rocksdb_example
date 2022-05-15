use async_trait::async_trait;

pub mod rocksdb;

#[async_trait]
pub trait Storage: Send + Sync + 'static {
    async fn get_key(&self,table: &str,key : &str) -> String;
}






#[derive(Clone)]
pub enum SupportedDatabase {
    RocksDB(rocksdb::RocksDB),
    //Sled(sled_impl::Sled),
}