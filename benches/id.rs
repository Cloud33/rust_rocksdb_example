

#[macro_use]
extern crate rust_rocksdb_example;

use bencher::{benchmark_group, benchmark_main, Bencher};
use rust_rocksdb_example::utils::id::*;

//17 ns/ite
fn bencher_IdInstance(bench: &mut Bencher) {
    let options = IdGeneratorOptions::new().machine_id(1).node_id(1);
    let _ = IdInstance::init(options);
    bench.iter(|| {
        IdInstance::next_id();
    })
}
//39 ns/iter
fn bencher_IdVecInstance(bench: &mut Bencher) {
    let options = vec![
            IdGeneratorOptions::new().machine_id(1).node_id(1),
            IdGeneratorOptions::new().machine_id(1).node_id(2),
        ];
    let _ = IdVecInstance::init(options);
    bench.iter(|| {
        IdVecInstance::next_id(0)
    })
}

benchmark_group!(benches, bencher_IdInstance, bencher_IdVecInstance);
benchmark_main!(benches);