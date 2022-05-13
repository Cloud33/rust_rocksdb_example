#[macro_use]
extern crate rust_rocksdb_example;

use bencher::{benchmark_group, benchmark_main, Bencher};
use uuid::Uuid;

//35 ns/iter 
fn new_v4(b: &mut Bencher) {
    b.iter(|| Uuid::new_v4());
}

benchmark_group!(benches, new_v4);
benchmark_main!(benches);