//! Messaging benchmarks modelling Ave's real traffic patterns:
//! tell-heavy fan-out (request manager), ask round-trips (sync-peer
//! queries), chained forwarding (compilation pipeline stages), and sink
//! broadcast fan-out (sink manager).

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, Error, Event,
    Handler, Message, NotPersistentActor, Response, Subscriber, SystemRef,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// ---------------------------------------------------------------------------
// Counter actor (tell throughput + ask latency)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CounterMsg {
    Add(u64),
    Get,
}

impl Message for CounterMsg {}

#[derive(Debug, Clone, PartialEq)]
struct Count(u64);

impl Response for Count {}

#[derive(Debug, Clone)]
struct CounterActor {
    hits: Arc<AtomicU64>,
}

impl NotPersistentActor for CounterActor {}

#[async_trait]
impl Actor for CounterActor {
    type Message = CounterMsg;
    type Response = Count;
    type Event = BenchEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("BenchCounter", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for CounterActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: CounterMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<Count, Error> {
        match msg {
            CounterMsg::Add(n) => {
                self.hits.fetch_add(n, Ordering::Relaxed);
                Ok(Count(0))
            }
            CounterMsg::Get => Ok(Count(self.hits.load(Ordering::Relaxed))),
        }
    }
}

// ---------------------------------------------------------------------------
// Chain actor (pipeline forwarding, like compilation stages)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ChainMsg {
    Forward,
    Get,
}

impl Message for ChainMsg {}

#[derive(Debug, Clone)]
struct ChainActor {
    next: Option<ActorRef<Self>>,
    terminal: Arc<AtomicU64>,
}

impl NotPersistentActor for ChainActor {}

#[async_trait]
impl Actor for ChainActor {
    type Message = ChainMsg;
    type Response = Count;
    type Event = BenchEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("BenchChain", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for ChainActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: ChainMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<Count, Error> {
        match msg {
            ChainMsg::Forward => {
                if let Some(next) = &self.next {
                    next.tell(ChainMsg::Forward).await?;
                } else {
                    self.terminal.fetch_add(1, Ordering::Relaxed);
                }
                Ok(Count(0))
            }
            ChainMsg::Get => Ok(Count(self.terminal.load(Ordering::Relaxed))),
        }
    }
}

// ---------------------------------------------------------------------------
// Emitter actor + counting subscribers (sink broadcast fan-out)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BenchEvent {
    id: u32,
}

impl Event for BenchEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum EmitMsg {
    Emit(u32),
    Flush,
}

impl Message for EmitMsg {}

#[derive(Debug, Clone)]
struct EmitterActor;

impl NotPersistentActor for EmitterActor {}

#[async_trait]
impl Actor for EmitterActor {
    type Message = EmitMsg;
    type Response = Count;
    type Event = BenchEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("BenchEmitter", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for EmitterActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: EmitMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<Count, Error> {
        match msg {
            EmitMsg::Emit(id) => {
                ctx.publish_all(BenchEvent { id });
                Ok(Count(0))
            }
            EmitMsg::Flush => Ok(Count(0)),
        }
    }
}

#[derive(Clone)]
struct CountingSub {
    count: Arc<AtomicU64>,
}

#[async_trait]
impl Subscriber<BenchEvent> for CountingSub {
    async fn notify(&self, _event: Arc<BenchEvent>) -> Result<(), Error> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

type Sys = (
    SystemRef,
    tokio::task::JoinHandle<ave_actors_actor::ShutdownReason>,
    tokio::runtime::Runtime,
);

fn system() -> Sys {
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

fn counter(
    rt: &tokio::runtime::Runtime,
    system: &SystemRef,
) -> ActorRef<CounterActor> {
    rt.block_on(system.create_root_actor(
        "counter",
        CounterActor {
            hits: Arc::new(AtomicU64::new(0)),
        },
    ))
    .expect("create counter")
}

fn chain(
    rt: &tokio::runtime::Runtime,
    system: &SystemRef,
    depth: usize,
    terminal: &Arc<AtomicU64>,
) -> ActorRef<ChainActor> {
    const NAMES: [&str; 8] = ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"];
    let mut next = None;
    for i in (0..depth).rev() {
        let actor = rt
            .block_on(system.create_root_actor(
                NAMES[i],
                ChainActor {
                    next,
                    terminal: Arc::clone(terminal),
                },
            ))
            .expect("create chain link");
        next = Some(actor);
    }
    next.expect("non-empty chain")
}

fn emitter(
    rt: &tokio::runtime::Runtime,
    system: &SystemRef,
    subscribers: usize,
) -> (ActorRef<EmitterActor>, Vec<Arc<AtomicU64>>) {
    // Sink setup needs a runtime context (spawns pump tasks).
    let (actor, counters) = rt.block_on(async {
        let actor = system
            .create_root_actor("emitter", EmitterActor)
            .await
            .expect("create emitter");
        let sink = actor.register_sink("bench", None).expect("register sink");
        let mut counters = Vec::with_capacity(subscribers);
        for i in 0..subscribers {
            let count = Arc::new(AtomicU64::new(0));
            sink.add(
                format!("sub-{i}"),
                CountingSub {
                    count: Arc::clone(&count),
                },
            );
            counters.push(count);
        }
        (actor, counters)
    });
    (actor, counters)
}

// ---------------------------------------------------------------------------
// Benches
// ---------------------------------------------------------------------------

fn bench_tell_throughput(c: &mut Criterion) {
    let (system, _runner, rt) = system();
    let actor = counter(&rt, &system);
    c.bench_function("tell_throughput/200_per_iter", |b| {
        b.to_async(&rt).iter(|| async {
            for _ in 0..200 {
                actor.tell(CounterMsg::Add(1)).await.expect("tell");
            }
            // Barrier: the ask is queued behind the tells.
            actor.ask(CounterMsg::Get).await.expect("flush");
        });
    });
    system.stop_system();
}

fn bench_ask_latency(c: &mut Criterion) {
    let (system, _runner, rt) = system();
    let actor = counter(&rt, &system);
    c.bench_function("ask_latency/single", |b| {
        b.to_async(&rt).iter(|| async {
            actor.ask(CounterMsg::Get).await.expect("ask");
        });
    });
    system.stop_system();
}

fn bench_pipeline(c: &mut Criterion) {
    let (system, _runner, rt) = system();
    let terminal = Arc::new(AtomicU64::new(0));
    let head = chain(&rt, &system, 5, &terminal);
    c.bench_function("pipeline/5_links_50_msgs", |b| {
        b.to_async(&rt).iter(|| async {
            let before = terminal.load(Ordering::Relaxed);
            for _ in 0..50 {
                head.tell(ChainMsg::Forward).await.expect("tell");
            }
            // End-to-end flush: spin until all 50 crossed 5 links.
            while terminal.load(Ordering::Relaxed) < before + 50 {
                tokio::task::yield_now().await;
            }
        });
    });
    system.stop_system();
}

fn bench_sink_fanout(c: &mut Criterion) {
    for n in [1_u64, 8, 64] {
        let (system, _runner, rt) = system();
        let (actor, counters) = emitter(&rt, &system, n as usize);
        c.bench_with_input(
            BenchmarkId::new("sink_fanout", format!("{n}_subs_50_events")),
            &n,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    let before: u64 = counters
                        .iter()
                        .map(|c| c.load(Ordering::Relaxed))
                        .sum();
                    for id in 0..50 {
                        actor.tell(EmitMsg::Emit(id)).await.expect("tell");
                    }
                    // End-to-end: every subscriber got every event.
                    let target = before + 50 * n;
                    loop {
                        let got: u64 = counters
                            .iter()
                            .map(|c| c.load(Ordering::Relaxed))
                            .sum();
                        if got >= target {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                });
            },
        );
        system.stop_system();
    }
}

criterion_group!(
    benches,
    bench_tell_throughput,
    bench_ask_latency,
    bench_pipeline,
    bench_sink_fanout,
);
criterion_main!(benches);
