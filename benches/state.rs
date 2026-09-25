//! `upsert_unit` is called from the engine's per-frame thread, so its
//! latency matters even while a reader (a gRPC handler) is iterating the
//! same cache concurrently. Benchmark both cases.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use minerva_server::StateCache;
use minerva_server::proto::{Unit, UnitCategory};

const UNIT_COUNT: usize = 1_000;

fn make_unit(i: usize) -> Unit {
    Unit {
        id: format!("unit-{i}"),
        group_id: "group-0".to_string(),
        category: UnitCategory::Infantry as i32,
        r#type: "B_Soldier_F".to_string(),
        state: None,
    }
}

fn upsert_batch(cache: &StateCache) {
    for i in 0..UNIT_COUNT {
        cache.upsert_unit(make_unit(i));
    }
}

fn bench_upsert_unit(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_cache_upsert_unit");

    group.bench_function("alone", |b| {
        b.iter_batched(
            StateCache::new,
            |cache| upsert_batch(black_box(&cache)),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("with_concurrent_reader", |b| {
        b.iter_batched(
            || {
                let cache = Arc::new(StateCache::new());
                let stop = Arc::new(AtomicBool::new(false));
                let reader = thread::spawn({
                    let cache = cache.clone();
                    let stop = stop.clone();
                    move || {
                        while !stop.load(Ordering::Relaxed) {
                            black_box(cache.list_units(None, None));
                        }
                    }
                });
                (cache, stop, reader)
            },
            |(cache, stop, reader)| {
                upsert_batch(black_box(&cache));
                stop.store(true, Ordering::Relaxed);
                let _ = reader.join();
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_upsert_unit);
criterion_main!(benches);
