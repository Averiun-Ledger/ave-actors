//! Persistence benchmarks: Full vs Light throughput on the memory
//! backend, recovery time over a 5k-event log, and Full throughput on
//! SQLite (relaxed durability) for a production-adjacent number.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, Error as ActorError,
    Event, Handler, Message, NotPersistentActor, Response, SystemRef,
};
use ave_actors_store::database::Durability;
use ave_actors_store::database::{Collection, DbManager};
use ave_actors_store::memory::MemoryManager;
use ave_actors_store::store::{
    FullPersistence, LightPersistence, PersistentActor, Store, StoreCommand,
    StoreResponse,
};
use criterion::{Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// ---------------------------------------------------------------------------
// Shared state / event
// ---------------------------------------------------------------------------

#[derive(
    Debug, Clone, Default, borsh::BorshSerialize, borsh::BorshDeserialize,
)]
struct BenchState {
    value: i64,
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
struct BenchPersistEvent {
    delta: i64,
}

impl Event for BenchPersistEvent {}

macro_rules! bench_actor {
    ($name:ident, $persistence:ty) => {
        #[derive(Debug)]
        struct $name {
            state_ptr: Arc<BenchState>,
        }

        #[async_trait]
        impl Actor for $name {
            type Message = StoreCommand<Self>;
            type Response = StoreResponse<Self>;
            type Event = BenchPersistEvent;
            type SinkEvent = Self::Event;
            type ChildError = ActorError;
            type ChildFault = ActorError;

            fn get_span(
                id: &str,
                _parent_span: Option<tracing::Span>,
            ) -> tracing::Span {
                info_span!("BenchStore", id = %id)
            }
        }

        #[async_trait]
        impl PersistentActor for $name {
            type Persistence = $persistence;
            type InitParams = ();
            type State = BenchState;

            fn create_initial(_: ()) -> Self {
                Self {
                    state_ptr: Arc::new(BenchState::default()),
                }
            }

            fn apply(
                state: Arc<Self::State>,
                event: &Self::Event,
            ) -> Result<Arc<Self::State>, ActorError> {
                let mut next = state;
                Arc::make_mut(&mut next).value += event.delta;
                Ok(next)
            }

            fn state(&self) -> Arc<Self::State> {
                Arc::clone(&self.state_ptr)
            }

            fn set_state(&mut self, state: Arc<Self::State>) {
                self.state_ptr = state;
            }
        }

        #[async_trait]
        impl Handler<Self> for $name {
            async fn handle_message(
                &mut self,
                _sender: ActorPath,
                _msg: StoreCommand<Self>,
                _ctx: &mut ActorContext<Self>,
            ) -> Result<StoreResponse<Self>, ActorError> {
                unreachable!(
                    "Store intercepts StoreCommand before user handling"
                );
            }
        }
    };
}

bench_actor!(FullActor, FullPersistence);
bench_actor!(LightActor, LightPersistence);

/// Local copy of the `store_new!` test helper (that macro lives in the
/// integration-test targets, so benches need their own).
macro_rules! store_new {
    ($type:ty, $($arg:expr),* $(,)?) => {
        {
            #[cfg(feature = "prometheus")]
            {
                ::ave_actors_store::store::Store::<$type>::new(
                    $($arg),*,
                    ::std::option::Option::None,
                    ::std::sync::Arc::from("/bench"),
                )
            }
            #[cfg(not(feature = "prometheus"))]
            {
                ::ave_actors_store::store::Store::<$type>::new(
                    $($arg),*
                )
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

fn system() -> (
    SystemRef,
    tokio::task::JoinHandle<ave_actors_actor::ShutdownReason>,
    tokio::runtime::Runtime,
) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("bench runtime");
    let (system, mut runner) = rt.block_on(async {
        ActorSystem::create(CancellationToken::new(), CancellationToken::new())
    });
    let handle = rt.spawn(async move { runner.run().await });
    (system, handle, rt)
}

async fn fill_full(store: &ActorRef<Store<FullActor>>, n: u64) {
    for _ in 0..n {
        store
            .ask(StoreCommand::PersistFull {
                event: Arc::new(BenchPersistEvent { delta: 1 }),
                state: Arc::new(BenchState::default()),
                snapshot_every: None,
            })
            .await
            .expect("persist");
    }
}

// ---------------------------------------------------------------------------
// Benches
// ---------------------------------------------------------------------------

fn bench_persist_full_memory(c: &mut Criterion) {
    let (system, _runner, rt) = system();
    let store = store_new!(
        FullActor,
        "bench",
        "full",
        MemoryManager::default(),
        None,
        Arc::new(BenchState::default()),
    )
    .expect("store");
    let store_ref = rt
        .block_on(system.create_root_actor("store", store))
        .expect("root");
    c.bench_function("persist_full_memory/20_per_iter", |b| {
        b.to_async(&rt).iter(|| async {
            for _ in 0..20 {
                store_ref
                    .ask(StoreCommand::PersistFull {
                        event: Arc::new(BenchPersistEvent { delta: 1 }),
                        state: Arc::new(BenchState::default()),
                        snapshot_every: None,
                    })
                    .await
                    .expect("persist");
            }
        });
    });
    system.stop_system();
}

fn bench_persist_light_memory(c: &mut Criterion) {
    let (system, _runner, rt) = system();
    let store = store_new!(
        LightActor,
        "bench",
        "light",
        MemoryManager::default(),
        None,
        Arc::new(BenchState::default()),
    )
    .expect("store");
    let store_ref = rt
        .block_on(system.create_root_actor("store", store))
        .expect("root");
    c.bench_function("persist_light_memory/20_per_iter", |b| {
        b.to_async(&rt).iter(|| async {
            for _ in 0..20 {
                store_ref
                    .ask(StoreCommand::PersistLight(Arc::new(BenchState {
                        value: 1,
                    })))
                    .await
                    .expect("persist");
            }
        });
    });
    system.stop_system();
}

fn bench_recover_5k(c: &mut Criterion) {
    let (system, _runner, rt) = system();
    let store = store_new!(
        FullActor,
        "bench",
        "recover",
        MemoryManager::default(),
        None,
        Arc::new(BenchState::default()),
    )
    .expect("store");
    let store_ref = rt
        .block_on(system.create_root_actor("store", store))
        .expect("root");
    rt.block_on(fill_full(&store_ref, 5_000));
    let mut group = c.benchmark_group("recover_5k_events");
    group.sample_size(20);
    group.bench_function("full_memory", |b| {
        b.to_async(&rt).iter(|| async {
            store_ref.ask(StoreCommand::Recover).await.expect("recover");
        });
    });
    group.finish();
    system.stop_system();
}

/// Recovery cost when there is NO usable snapshot: every event is
/// replayed through `apply`. Contrasts with `bench_recover_5k`, which
/// only loads the latest snapshot.
fn bench_recover_replay_5k(c: &mut Criterion) {
    let (system, _runner, rt) = system();
    let store = store_new!(
        FullActor,
        "bench",
        "replay",
        MemoryManager::default(),
        None,
        Arc::new(BenchState::default()),
    )
    .expect("store");
    let store_ref = rt
        .block_on(system.create_root_actor("store", store))
        .expect("root");
    rt.block_on(async {
        for _ in 0..5_000 {
            store_ref
                .ask(StoreCommand::PersistFull {
                    event: Arc::new(BenchPersistEvent { delta: 1 }),
                    state: Arc::new(BenchState::default()),
                    snapshot_every: Some(u64::MAX),
                })
                .await
                .expect("persist");
        }
    });
    let mut group = c.benchmark_group("recover_replay_5k_events");
    group.sample_size(20);
    // Correctness gate: replay must actually apply all 5k events.
    let check = rt
        .block_on(store_ref.ask(StoreCommand::Recover))
        .expect("recover");
    match check {
        StoreResponse::State(Some(state)) => {
            assert_eq!(state.value, 5_000, "replay must apply every event");
        }
        other => panic!("expected full replay, got {other:?}"),
    }
    group.bench_function("full_memory_no_snapshot", |b| {
        b.to_async(&rt).iter(|| async {
            store_ref.ask(StoreCommand::Recover).await.expect("recover");
        });
    });
    group.finish();
    system.stop_system();
}

fn bench_get_events_range_5k(c: &mut Criterion) {
    let (system, _runner, rt) = system();
    let store = store_new!(
        FullActor,
        "bench",
        "range",
        MemoryManager::default(),
        None,
        Arc::new(BenchState::default()),
    )
    .expect("store");
    let store_ref = rt
        .block_on(system.create_root_actor("store", store))
        .expect("root");
    rt.block_on(fill_full(&store_ref, 5_000));
    let mut group = c.benchmark_group("get_events_range_5k");
    group.sample_size(20);
    group.bench_function("full_memory", |b| {
        b.to_async(&rt).iter(|| async {
            store_ref
                .ask(StoreCommand::GetEvents { from: 0, to: 4_999 })
                .await
                .expect("range");
        });
    });
    group.finish();
    system.stop_system();
}

/// Pure ask + dispatch cost on a live store actor: no backend read,
/// no encoding. Subtracting this from `persist_full_memory` isolates
/// the persistence work itself.
fn bench_ask_overhead(c: &mut Criterion) {
    let (system, _runner, rt) = system();
    let store = store_new!(
        FullActor,
        "bench",
        "askover",
        MemoryManager::default(),
        None,
        Arc::new(BenchState::default()),
    )
    .expect("store");
    let store_ref = rt
        .block_on(system.create_root_actor("store", store))
        .expect("root");
    c.bench_function("ask_overhead/next_number_x50", |b| {
        b.to_async(&rt).iter(|| async {
            for _ in 0..50 {
                store_ref
                    .ask(StoreCommand::NextEventNumber)
                    .await
                    .expect("ask");
            }
        });
    });
    system.stop_system();
}

// ---------------------------------------------------------------------------
// get_child probe: how much does the per-persist child lookup cost?
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ProbeMsg {
    Lookup,
}

impl Message for ProbeMsg {}

#[derive(Debug, Clone, PartialEq)]
struct ProbeResp;

impl Response for ProbeResp {}

#[derive(Debug, Clone)]
struct ProbeChild;

impl NotPersistentActor for ProbeChild {}

#[async_trait]
impl Actor for ProbeChild {
    type Message = ProbeMsg;
    type Response = ProbeResp;
    type Event = BenchPersistEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("BenchProbeChild", id = %id)
    }
}

#[async_trait]
impl Handler<ProbeChild> for ProbeChild {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: ProbeMsg,
        _ctx: &mut ActorContext<ProbeChild>,
    ) -> Result<ProbeResp, ActorError> {
        Ok(ProbeResp)
    }
}

#[derive(Debug, Clone)]
struct ProbeParent;

impl NotPersistentActor for ProbeParent {}

#[async_trait]
impl Actor for ProbeParent {
    type Message = ProbeMsg;
    type Response = ProbeResp;
    type Event = BenchPersistEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("BenchProbe", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        ctx.create_child("probe-child", ProbeChild).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<ProbeParent> for ProbeParent {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: ProbeMsg,
        ctx: &mut ActorContext<ProbeParent>,
    ) -> Result<ProbeResp, ActorError> {
        let _ = ctx.get_child::<ProbeChild>("probe-child").await?;
        Ok(ProbeResp)
    }
}

fn bench_get_child(c: &mut Criterion) {
    let (system, _runner, rt) = system();
    let parent = rt
        .block_on(system.create_root_actor("probe", ProbeParent))
        .expect("root");
    // Wait until the child exists (pre_start ran).
    rt.block_on(async {
        for _ in 0..100 {
            if parent.ask(ProbeMsg::Lookup).await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    });
    c.bench_function("get_child/lookup_x50", |b| {
        b.to_async(&rt).iter(|| async {
            for _ in 0..50 {
                parent.ask(ProbeMsg::Lookup).await.expect("lookup");
            }
        });
    });
    system.stop_system();
}

/// Floor of the memory backend: raw put/get without actor, ask, fence
/// or encoding overhead.
fn bench_memory_backend(c: &mut Criterion) {
    let manager = MemoryManager::default();
    let mut col = manager
        .create_collection("bench", "raw")
        .expect("collection");
    c.bench_function("memory_backend/put_x100", |b| {
        b.iter(|| {
            for i in 0..100 {
                col.put(&format!("{i:020}"), &[7u8; 32]).expect("put");
            }
        });
    });
    let mut col2 = manager
        .create_collection("bench", "raw2")
        .expect("collection");
    for i in 0..100 {
        col2.put(&format!("{i:020}"), &[7u8; 32]).expect("fill");
    }
    c.bench_function("memory_backend/get_hit_x100", |b| {
        b.iter(|| {
            for i in 0..100 {
                let _ = col2.get(&format!("{i:020}")).expect("get");
            }
        });
    });
}

fn bench_persist_full_sqlite(c: &mut Criterion) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let manager = ave_actors_sqlite::SqliteManager::new(
        tmp.path(),
        Durability::Relaxed,
        None,
    )
    .expect("sqlite manager");
    let (system, _runner, rt) = system();
    let store = store_new!(
        FullActor,
        "bench",
        "sqlite",
        manager,
        None,
        Arc::new(BenchState::default()),
    )
    .expect("store");
    let store_ref = rt
        .block_on(system.create_root_actor("store", store))
        .expect("root");
    let mut group = c.benchmark_group("persist_full_sqlite");
    group.sample_size(20);
    group.bench_function("relaxed_10_per_iter", |b| {
        b.to_async(&rt).iter(|| async {
            for _ in 0..10 {
                store_ref
                    .ask(StoreCommand::PersistFull {
                        event: Arc::new(BenchPersistEvent { delta: 1 }),
                        state: Arc::new(BenchState::default()),
                        snapshot_every: None,
                    })
                    .await
                    .expect("persist");
            }
        });
    });
    group.finish();
    system.stop_system();
}

criterion_group!(
    benches,
    bench_persist_full_memory,
    bench_persist_light_memory,
    bench_recover_5k,
    bench_recover_replay_5k,
    bench_get_events_range_5k,
    bench_ask_overhead,
    bench_get_child,
    bench_memory_backend,
    bench_persist_full_sqlite,
);
criterion_main!(benches);
