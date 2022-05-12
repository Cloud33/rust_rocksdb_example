

#[macro_use]
extern crate rust_rocksdb_example;

use threadpool::ThreadPool;
use bencher::{benchmark_group, benchmark_main, Bencher};
use rust_rocksdb_example::utils::snowflake::ProcessUniqueId;

use std::sync::mpsc::channel;

fn bencher(bench: &mut Bencher) {
    bench.iter(|| {
        ProcessUniqueId::new();
    })
}

fn bencher_threaded(bench: &mut Bencher) {
    let pool = ThreadPool::new(4);
    bench.iter(|| {
        let (tx, rx) = channel();
        for _ in 0..4 {
            let tx = tx.clone();
            pool.execute(move || {
                for _ in 0..1000 {
                    ProcessUniqueId::new();
                }
                tx.send(()).unwrap();
            });
        }
        rx.iter().take(4).count();
    });
}

benchmark_group!(benches, bencher, bencher_threaded);
benchmark_main!(benches);