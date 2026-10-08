//! Router tests: distribution strategies, membership, pool mode,
//! and fault recovery. Workers are [`TestProbe`]s wherever the test
//! only observes delivery.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, Error, Event, Handler, Message,
    NotPersistentActor, ProbeActor, Response, Router, RouterMsg,
    RouterResponse, RoutingStrategy, TestProbe, TestSystem,
};
use serde::{Deserialize, Serialize};
use test_log::test;
use tracing::info_span;

mod helpers;

// ---------------------------------------------------------------------------
// Messages and workers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Ping(u64);

impl Message for Ping {}

impl Event for Ping {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct EchoReq(u64);

impl Message for EchoReq {}

#[derive(Debug, Clone, PartialEq)]
struct EchoResp(u64);

impl Response for EchoResp {}

#[derive(Debug, Clone)]
struct EchoWorker;

impl NotPersistentActor for EchoWorker {}

#[async_trait]
impl Actor for EchoWorker {
    type Message = EchoReq;
    type Response = EchoResp;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("EchoWorker", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for EchoWorker {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: EchoReq,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<EchoResp, Error> {
        Ok(EchoResp(msg.0))
    }
}

#[derive(Debug, Clone)]
struct CountingWorker {
    hits: Arc<AtomicU64>,
}

impl NotPersistentActor for CountingWorker {}

#[async_trait]
impl Actor for CountingWorker {
    type Message = Ping;
    type Response = ();
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("CountingWorker", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for CountingWorker {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: Ping,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        self.hits.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum FailMsg {
    Work,
    Fail,
}

impl Message for FailMsg {}

#[derive(Debug, Clone)]
struct FailWorker;

impl NotPersistentActor for FailWorker {}

#[async_trait]
impl Actor for FailWorker {
    type Message = FailMsg;
    type Response = ();
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("FailWorker", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for FailWorker {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: FailMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        match msg {
            FailMsg::Work => Ok(()),
            FailMsg::Fail => Err(Error::Functional {
                description: "intentional failure".to_owned(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type ProbeRouter = Router<ProbeActor<Ping>>;
type ProbeRouterRef = ActorRef<ProbeRouter>;

/// Spawns one probe actor per name and a router over them.
async fn probe_router(
    harness: &TestSystem,
    router_name: &str,
    worker_names: &[&str],
    strategy: RoutingStrategy,
) -> (Vec<TestProbe<Ping>>, ProbeRouterRef) {
    let system = harness.system();
    let mut probes = Vec::new();
    let mut refs = Vec::new();
    for name in worker_names {
        let probe = TestProbe::new();
        let probe_ref = probe.spawn(system, name).await.unwrap();
        probes.push(probe);
        refs.push(probe_ref);
    }
    let router_ref = system
        .create_root_actor(router_name, Router::new(refs, strategy))
        .await
        .unwrap();
    (probes, router_ref)
}

fn count(response: RouterResponse<ProbeActor<Ping>>) -> usize {
    match response {
        RouterResponse::Count(n) => n,
        other => panic!("expected Count, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test(tokio::test)]
async fn test_round_robin_splits_evenly() {
    let harness = TestSystem::start();
    let (probes, router) = probe_router(
        &harness,
        "router",
        &["w0", "w1", "w2"],
        RoutingStrategy::RoundRobin,
    )
    .await;

    for i in 0..9 {
        router.tell(RouterMsg::Route(Ping(i))).await.unwrap();
    }
    for (worker, probe) in probes.iter().enumerate() {
        let got = probe.expect_count(3, Duration::from_secs(2)).await.unwrap();
        assert_eq!(got.len(), 3, "worker {worker} must get 3 messages");
    }

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_random_spreads_load() {
    let harness = TestSystem::start();
    let (probes, router) = probe_router(
        &harness,
        "router",
        &["w0", "w1", "w2"],
        RoutingStrategy::Random,
    )
    .await;

    for i in 0..300 {
        router.tell(RouterMsg::Route(Ping(i))).await.unwrap();
    }
    // 300 uniform draws over 3 workers: each gets ~100; anything below
    // 50 would be a six-sigma event, so this is not flaky.
    for (worker, probe) in probes.iter().enumerate() {
        let got = probe
            .expect_count(50, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            got.len() >= 50,
            "worker {worker} starved under random routing"
        );
    }

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_broadcast_reaches_all_workers() {
    let harness = TestSystem::start();
    let (probes, router) = probe_router(
        &harness,
        "router",
        &["w0", "w1", "w2"],
        RoutingStrategy::Broadcast,
    )
    .await;

    for i in 0..2 {
        router.tell(RouterMsg::Route(Ping(i))).await.unwrap();
    }
    for probe in &probes {
        let got = probe.expect_count(2, Duration::from_secs(2)).await.unwrap();
        assert_eq!(got, vec![Ping(0), Ping(1)]);
    }

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_ask_returns_worker_response() {
    let harness = TestSystem::start();
    let system = harness.system();
    let echo = system.create_root_actor("echo", EchoWorker).await.unwrap();
    let router = system
        .create_root_actor(
            "router",
            Router::new(vec![echo], RoutingStrategy::RoundRobin),
        )
        .await
        .unwrap();

    let response = router.ask(RouterMsg::Route(EchoReq(41))).await.unwrap();
    match response {
        RouterResponse::Routed(EchoResp(value)) => assert_eq!(value, 41),
        other => panic!("expected Routed response, got {other:?}"),
    }

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_empty_router_errors() {
    let harness = TestSystem::start();
    let router: ProbeRouterRef = harness
        .system()
        .create_root_actor(
            "router",
            Router::new(vec![], RoutingStrategy::RoundRobin),
        )
        .await
        .unwrap();

    let err = router.ask(RouterMsg::Route(Ping(1))).await.unwrap_err();
    assert!(
        matches!(err, Error::Functional { .. }),
        "empty router must fail loudly, got {err:?}"
    );

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_membership_add_remove_count() {
    let harness = TestSystem::start();
    let (probes, router) = probe_router(
        &harness,
        "router",
        &["w0", "w1"],
        RoutingStrategy::RoundRobin,
    )
    .await;

    let response = router.ask(RouterMsg::WorkerCount).await.unwrap();
    assert_eq!(count(response), 2);

    // Re-adding a live worker is a no-op (no double delivery).
    let extra = TestProbe::new();
    let extra_ref = extra.spawn(harness.system(), "w2").await.unwrap();
    let response = router
        .ask(RouterMsg::AddWorker(extra_ref.clone()))
        .await
        .unwrap();
    assert!(matches!(response, RouterResponse::Updated));
    let response = router.ask(RouterMsg::AddWorker(extra_ref)).await.unwrap();
    assert!(matches!(response, RouterResponse::Updated));
    let response = router.ask(RouterMsg::WorkerCount).await.unwrap();
    assert_eq!(count(response), 3);

    router
        .tell(RouterMsg::RemoveWorker(ActorPath::from("/user/w0")))
        .await
        .unwrap();
    let response = router.ask(RouterMsg::WorkerCount).await.unwrap();
    assert_eq!(count(response), 2);

    // Removing an absent path is a no-op.
    router
        .tell(RouterMsg::RemoveWorker(ActorPath::from("/user/ghost")))
        .await
        .unwrap();
    let response = router.ask(RouterMsg::WorkerCount).await.unwrap();
    assert_eq!(count(response), 2);

    // The removed worker gets nothing more.
    for i in 0..4 {
        router.tell(RouterMsg::Route(Ping(i))).await.unwrap();
    }
    probes[0]
        .expect_no_msg(Duration::from_millis(200))
        .await
        .unwrap();
    let _ = &probes;

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_set_strategy_switches_routing() {
    let harness = TestSystem::start();
    let (probes, router) = probe_router(
        &harness,
        "router",
        &["w0", "w1", "w2"],
        RoutingStrategy::RoundRobin,
    )
    .await;

    let response = router
        .ask(RouterMsg::SetStrategy(RoutingStrategy::Broadcast))
        .await
        .unwrap();
    assert!(matches!(response, RouterResponse::Updated));

    router.tell(RouterMsg::Route(Ping(7))).await.unwrap();
    for probe in &probes {
        let got = probe.expect_msg(Duration::from_secs(2)).await.unwrap();
        assert_eq!(got, Ping(7));
    }

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_pooled_spawns_children() {
    let harness = TestSystem::start();
    let system = harness.system();
    let hits = Arc::new(AtomicU64::new(0));
    let router = system
        .create_root_actor(
            "pool",
            Router::pooled(
                vec![
                    CountingWorker {
                        hits: Arc::clone(&hits),
                    },
                    CountingWorker {
                        hits: Arc::clone(&hits),
                    },
                ],
                RoutingStrategy::RoundRobin,
            ),
        )
        .await
        .unwrap();

    let response = router.ask(RouterMsg::WorkerCount).await.unwrap();
    match response {
        RouterResponse::Count(2) => {}
        other => panic!("pool must spawn 2 children, got {other:?}"),
    }

    for i in 0..10 {
        router.tell(RouterMsg::Route(Ping(i))).await.unwrap();
    }
    helpers::assert_eventually(
        "pooled workers process every message",
        Duration::from_secs(2),
        || async { (hits.load(Ordering::Relaxed) == 10).then_some(()) },
    )
    .await;

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_faulted_pool_child_restarts() {
    let harness = TestSystem::start();
    let system = harness.system();
    let router = system
        .create_root_actor(
            "pool",
            Router::pooled(vec![FailWorker], RoutingStrategy::RoundRobin),
        )
        .await
        .unwrap();

    // Fail the only worker: the ask reports the worker error...
    let err = router
        .ask(RouterMsg::Route(FailMsg::Fail))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Functional { .. }),
        "worker failure must surface, got {err:?}"
    );

    // ...but the router restarts it, so the pool keeps serving.
    helpers::assert_eventually(
        "faulted pool child restarts",
        Duration::from_secs(5),
        || async {
            let ask = router.ask(RouterMsg::Route(FailMsg::Work)).await;
            ask.ok().and_then(|response| match response {
                RouterResponse::Routed(()) => Some(()),
                _ => None,
            })
        },
    )
    .await;

    harness.shutdown().await;
}
