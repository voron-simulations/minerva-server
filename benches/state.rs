//! `upsert_unit`/`upsert_group_with_units` run from the engine's per-frame
//! thread, so their latency matters even while a reader (a gRPC handler) is
//! iterating the same cache concurrently, and even while subscribers are
//! draining the broadcasts they produce. Benchmark all of that, and use the
//! fan-out numbers to size `BROADCAST_CAPACITY` in src/state.rs: it must
//! absorb one mission "tick" (every group upserting once) without a
//! momentarily-slow subscriber lagging and getting aborted.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use minerva_server::proto::group_service_client::GroupServiceClient;
use minerva_server::proto::{
    Group, Position, SubscribeGroupUpdatesRequest, Unit, UnitCategory, UnitState,
};
use minerva_server::{Command, CommandId, CommandSink, ServerConfig, ServerHandle, StateCache};
use tokio_stream::StreamExt;

const UNIT_COUNT: usize = 1_000;
/// A mission-scale tick: ~50 groups (see docs/in-game-testing.md's test
/// mission), most squad-sized.
const GROUP_COUNT: usize = 50;
const UNITS_PER_GROUP: usize = 12;

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

/// A group's units at a given tick. `moved` flips the reported position, so
/// callers can compare the changed path (always broadcasts) against the
/// unchanged one (skipped by the dedupe in `upsert_group_with_units`).
fn make_group_units(group_id: &str, moved: bool) -> Vec<Unit> {
    (0..UNITS_PER_GROUP)
        .map(|i| Unit {
            id: format!("{group_id}-unit-{i}"),
            group_id: group_id.to_string(),
            category: UnitCategory::Infantry as i32,
            r#type: "B_Soldier_F".to_string(),
            state: Some(UnitState {
                position: Some(Position {
                    x: if moved { 1.0 } else { 0.0 },
                    y: 0.0,
                    z: 0.0,
                }),
                ..Default::default()
            }),
        })
        .collect()
}

fn make_group(id: &str) -> Group {
    Group {
        id: id.to_string(),
        side: 1,
        readiness: None,
        has_task: false,
        waypoints: Vec::new(),
        units: Vec::new(),
    }
}

/// One "tick" of every group in a mission pushing its full unit list, as
/// `fnc_pushGroup.sqf` does once per group per `TICK` -- staggered there,
/// but worst-case for the cache/broadcast path is every group in the same
/// instant, which this simulates.
fn push_one_tick(cache: &StateCache, moved: bool) {
    for g in 0..GROUP_COUNT {
        let id = format!("group-{g}");
        cache.upsert_group_with_units(make_group(&id), make_group_units(&id, moved));
    }
}

fn bench_upsert_group_with_units(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_cache_upsert_group_with_units");

    group.bench_function("changed/one_tick", |b| {
        b.iter_batched(
            StateCache::new,
            |cache| push_one_tick(black_box(&cache), true),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("unchanged/one_tick", |b| {
        b.iter_batched(
            || {
                let cache = StateCache::new();
                push_one_tick(&cache, false);
                cache
            },
            // The dedupe path: same group and units as the setup tick, so
            // every group's joined value is unchanged and nothing broadcasts.
            |cache| push_one_tick(black_box(&cache), false),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

struct NoopSink;
impl CommandSink for NoopSink {
    fn dispatch(&self, _id: CommandId, _command: &Command) {}
}

/// Fans a full tick out to `subscriber_count` concurrent subscribers over
/// real gRPC connections (there's no public in-process hook onto the
/// broadcast channel, and the network path is what actually determines
/// whether `BROADCAST_CAPACITY` is enough), each draining its stream
/// concurrently, to measure the write-lock-held cost of
/// `broadcast::Sender::send` scaling with subscriber count.
fn bench_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_cache_group_updates_fanout");
    let rt = tokio::runtime::Runtime::new().expect("runtime");

    for subscriber_count in [0usize, 8, 64] {
        group.bench_function(format!("subscribers_{subscriber_count}"), |b| {
            b.iter_batched(
                || {
                    rt.block_on(async {
                        let config = ServerConfig {
                            addr: "127.0.0.1:0".parse().expect("addr"),
                            ..ServerConfig::default()
                        };
                        let handle =
                            ServerHandle::spawn(config, Arc::new(NoopSink)).expect("spawn");
                        let endpoint = format!("http://{}", handle.local_addr());
                        let stop = Arc::new(AtomicBool::new(false));
                        let mut drainers = Vec::new();
                        for _ in 0..subscriber_count {
                            let mut client = GroupServiceClient::connect(endpoint.clone())
                                .await
                                .expect("connect");
                            let mut stream = client
                                .subscribe_group_updates(SubscribeGroupUpdatesRequest {
                                    side: None,
                                })
                                .await
                                .expect("subscribe")
                                .into_inner();
                            let stop = stop.clone();
                            drainers.push(tokio::spawn(async move {
                                // A short poll interval bounds how long a
                                // drainer can take to notice `stop` after
                                // its current wait, so that doesn't leak
                                // into the measured time below.
                                while !stop.load(Ordering::Relaxed) {
                                    let _ = tokio::time::timeout(
                                        Duration::from_millis(1),
                                        stream.next(),
                                    )
                                    .await;
                                }
                            }));
                        }
                        (handle, stop, drainers)
                    })
                },
                |(handle, stop, drainers)| {
                    push_one_tick(black_box(handle.state()), true);
                    rt.block_on(async {
                        stop.store(true, Ordering::Relaxed);
                        for drainer in drainers {
                            let _ = drainer.await;
                        }
                    });
                    // Returned rather than dropped here: `ServerHandle`'s
                    // graceful shutdown (up to its multi-second grace
                    // period) would otherwise run inside the timed section,
                    // measuring teardown instead of the fan-out itself.
                    handle
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_upsert_unit,
    bench_upsert_group_with_units,
    bench_fanout
);
criterion_main!(benches);
