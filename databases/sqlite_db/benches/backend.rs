//! Raw backend throughput: isolates the storage engine from the
//! store actor layer (serialization, ask round-trip, fencing).

use ave_actors_sqlite::SqliteManager;
use ave_actors_store::database::{Collection, DbManager, Durability};
use criterion::{Criterion, criterion_group, criterion_main};

const PAYLOAD: &[u8] = &[7u8; 32];

fn key(i: u64) -> String {
    format!("{i:020}")
}

fn manager_with(durability: Durability) -> (tempfile::TempDir, SqliteManager) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manager =
        SqliteManager::new(tmp.path(), durability, None).expect("manager");
    (tmp, manager)
}

fn manager() -> (tempfile::TempDir, SqliteManager) {
    manager_with(Durability::Relaxed)
}

fn bench_put(c: &mut Criterion) {
    let (_tmp, manager) = manager();
    let mut col = manager
        .create_collection("bench", "put")
        .expect("collection");
    c.bench_function("sqlite/put_x100", |b| {
        b.iter(|| {
            for i in 0..100 {
                col.put(&key(i), PAYLOAD).expect("put");
            }
        });
    });
}

/// Same as `bench_put` with full fsync per write: the cost of
/// `Durability::Sync` in production.
fn bench_put_sync(c: &mut Criterion) {
    let (_tmp, manager) = manager_with(Durability::Sync);
    let mut col = manager
        .create_collection("bench", "put_sync")
        .expect("collection");
    let mut group = c.benchmark_group("sqlite_put_sync");
    group.sample_size(20);
    group.bench_function("x20", |b| {
        b.iter(|| {
            for i in 0..20 {
                col.put(&key(i), PAYLOAD).expect("put");
            }
        });
    });
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let (_tmp, manager) = manager();
    let mut col = manager
        .create_collection("bench", "get")
        .expect("collection");
    for i in 0..100 {
        col.put(&key(i), PAYLOAD).expect("fill");
    }
    c.bench_function("sqlite/get_hit_x100", |b| {
        b.iter(|| {
            for i in 0..100 {
                let _ = col.get(&key(i)).expect("get");
            }
        });
    });
}

fn bench_scan_5k(c: &mut Criterion) {
    let (_tmp, manager) = manager();
    let mut col = manager
        .create_collection("bench", "scan")
        .expect("collection");
    for i in 0..5_000 {
        col.put(&key(i), PAYLOAD).expect("fill");
    }
    let mut group = c.benchmark_group("sqlite_scan_5k");
    group.sample_size(20);
    group.bench_function("iter_full", |b| {
        b.iter(|| {
            let n = col.iter(false).expect("iter").count();
            assert_eq!(n, 5_000);
        });
    });
    group.bench_function("get_by_range_1k", |b| {
        b.iter(|| {
            let rows = col.get_by_range(None, 1_000).expect("range");
            assert_eq!(rows.len(), 1_000);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_put, bench_put_sync, bench_get, bench_scan_5k,);
criterion_main!(benches);
