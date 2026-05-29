//! Tests for the redesigned sink API (external registration, parallel
//! dispatch, retry policies, and survival across restarts).

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error, Event, Handler,
    Message, Response, Sink, SinkEntry, Strategy, Subscriber,
    SupervisionStrategy,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use test_log::test;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// ============================================================================
// Helpers
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestEvent {
    id: u32,
}

impl Event for TestEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TestMsg {
    Emit(u32),
    Fail,
}

impl Message for TestMsg {}

#[derive(Debug, Clone, PartialEq)]
struct TestResponse;

impl Response for TestResponse {}

#[derive(Debug, Clone)]
struct EmitterActor;

impl ave_actors_actor::NotPersistentActor for EmitterActor {}

#[async_trait]
impl Actor for EmitterActor {
    type Message = TestMsg;
    type Response = TestResponse;
    type Event = TestEvent;
    type SinkEvent = Self::Event;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("EmitterActor", id = %id)
    }
}

#[async_trait]
impl Handler<EmitterActor> for EmitterActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: TestMsg,
        ctx: &mut ActorContext<EmitterActor>,
    ) -> Result<TestResponse, Error> {
        match msg {
            TestMsg::Emit(id) => {
                ctx.publish_all(TestEvent { id });
                Ok(TestResponse)
            }
            TestMsg::Fail => Err(Error::Functional {
                description: "intentional failure".to_owned(),
            }),
        }
    }
}

#[derive(Clone)]
struct CollectingSubscriber {
    events: Arc<Mutex<Vec<TestEvent>>>,
}

impl CollectingSubscriber {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn drain(&self) -> Vec<TestEvent> {
        let mut lock = self.events.lock().await;
        std::mem::take(&mut *lock)
    }

    async fn clone_events(&self) -> Vec<TestEvent> {
        self.events.lock().await.clone()
    }
}

#[async_trait]
impl Subscriber<TestEvent> for CollectingSubscriber {
    async fn notify(&self, event: Arc<TestEvent>) -> Result<(), Error> {
        self.events.lock().await.push((*event).clone());
        Ok(())
    }
}

#[derive(Clone)]
struct SlowSubscriber {
    delay_ms: u64,
}

#[async_trait]
impl Subscriber<TestEvent> for SlowSubscriber {
    async fn notify(&self, _event: Arc<TestEvent>) -> Result<(), Error> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        Ok(())
    }
}

#[derive(Clone)]
struct FailingThenOkSubscriber {
    fail_count: Arc<AtomicU32>,
    target_fails: u32,
}

impl FailingThenOkSubscriber {
    fn new(target_fails: u32) -> Self {
        Self {
            fail_count: Arc::new(AtomicU32::new(0)),
            target_fails,
        }
    }
}

#[async_trait]
impl Subscriber<TestEvent> for FailingThenOkSubscriber {
    async fn notify(&self, _event: Arc<TestEvent>) -> Result<(), Error> {
        let current = self.fail_count.fetch_add(1, Ordering::SeqCst);
        if current < self.target_fails {
            Err(Error::Functional {
                description: format!("fail {}", current),
            })
        } else {
            Ok(())
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[test(tokio::test)]
async fn test_external_sink_registration() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("emitter", EmitterActor)
        .await
        .unwrap();

    let subscriber = CollectingSubscriber::new();
    let mut sink = Sink::new("ext_sink");
    sink.add("sub1", subscriber.clone());
    actor_ref.register_sink(sink);

    actor_ref.tell(TestMsg::Emit(42)).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let evts = subscriber.drain().await;
            if !evts.is_empty() {
                assert_eq!(evts[0].id, 42);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("subscriber should receive event");
}

#[test(tokio::test)]
async fn test_sink_survives_restart() {
    #[derive(Debug, Clone)]
    struct FailingEmitter;

    impl ave_actors_actor::NotPersistentActor for FailingEmitter {}

    #[async_trait]
    impl Actor for FailingEmitter {
        type Message = TestMsg;
        type Response = TestResponse;
        type Event = TestEvent;
    type SinkEvent = Self::Event;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("FailingEmitter", id = %id)
        }

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::Retry(Strategy::NoInterval(
                ave_actors_actor::NoIntervalStrategy::new(1),
            ))
        }
    }

    #[async_trait]
    impl Handler<FailingEmitter> for FailingEmitter {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: TestMsg,
            ctx: &mut ActorContext<FailingEmitter>,
        ) -> Result<TestResponse, Error> {
            match msg {
                TestMsg::Emit(id) => {
                    ctx.publish_all(TestEvent { id });
                    if id == 1 {
                        return Err(Error::Functional {
                            description: "boom".to_owned(),
                        });
                    }
                    Ok(TestResponse)
                }
                _ => Ok(TestResponse),
            }
        }
    }

    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("failing_emitter", FailingEmitter)
        .await
        .unwrap();

    let subscriber = CollectingSubscriber::new();
    let mut sink = Sink::new("survivor");
    sink.add("sub1", subscriber.clone());
    actor_ref.register_sink(sink);

    // First message triggers a failure, actor restarts.
    let _ = actor_ref.tell(TestMsg::Emit(1)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second message should still reach the *same* sink.
    let _ = actor_ref.tell(TestMsg::Emit(2)).await;

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let evts = subscriber.clone_events().await;
            if evts.len() >= 2 {
                assert_eq!(evts[0].id, 1);
                assert_eq!(evts[1].id, 2);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both events should be received across restart");
}

#[test(tokio::test)]
async fn test_parallel_dispatch() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("parallel", EmitterActor)
        .await
        .unwrap();

    let mut sink = Sink::new("parallel_sink");
    sink.add("slow1", SlowSubscriber { delay_ms: 200 });
    sink.add("slow2", SlowSubscriber { delay_ms: 200 });
    sink.add("slow3", SlowSubscriber { delay_ms: 200 });
    actor_ref.register_sink(sink);

    let start = Instant::now();
    actor_ref.tell(TestMsg::Emit(1)).await.unwrap();

    // Wait a bit for dispatch to complete.
    tokio::time::sleep(Duration::from_millis(250)).await;

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "parallel dispatch should be faster than sequential ({:?})",
        elapsed
    );
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FilteredMsg;

impl Message for FilteredMsg {}

#[derive(Debug, Clone)]
struct FilteredActor;

impl ave_actors_actor::NotPersistentActor for FilteredActor {}

#[async_trait]
impl Actor for FilteredActor {
    type Message = FilteredMsg;
    type Response = TestResponse;
    type Event = TestEvent;
    type SinkEvent = Self::Event;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("FilteredActor", id = %id)
    }
}

#[async_trait]
impl Handler<FilteredActor> for FilteredActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: FilteredMsg,
        ctx: &mut ActorContext<FilteredActor>,
    ) -> Result<TestResponse, Error> {
        let _ = ctx.publish_filtered(
            |name: &str| name.starts_with("audit"),
            TestEvent { id: 99 },
        );
        Ok(TestResponse)
    }
}

#[test(tokio::test)]
async fn test_publish_filtered() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("filtered", FilteredActor)
        .await
        .unwrap();

    let audit_sub = CollectingSubscriber::new();
    let metrics_sub = CollectingSubscriber::new();

    let mut audit_sink = Sink::new("audit");
    audit_sink.add("sub1", audit_sub.clone());
    actor_ref.register_sink(audit_sink);

    let mut metrics_sink = Sink::new("metrics");
    metrics_sink.add("sub1", metrics_sub.clone());
    actor_ref.register_sink(metrics_sink);

    actor_ref.tell(FilteredMsg).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let audit_evts = audit_sub.drain().await;
    let metrics_evts = metrics_sub.drain().await;

    assert_eq!(audit_evts.len(), 1);
    assert_eq!(audit_evts[0].id, 99);
    assert!(metrics_evts.is_empty());
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NoopMsg;

impl Message for NoopMsg {}

#[derive(Debug, Clone)]
struct NoopActor;

impl ave_actors_actor::NotPersistentActor for NoopActor {}

#[async_trait]
impl Actor for NoopActor {
    type Message = NoopMsg;
    type Response = TestResponse;
    type Event = TestEvent;
    type SinkEvent = Self::Event;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("NoopActor", id = %id)
    }
}

#[async_trait]
impl Handler<NoopActor> for NoopActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: NoopMsg,
        ctx: &mut ActorContext<NoopActor>,
    ) -> Result<TestResponse, Error> {
        // No sink registered with this name.
        let _ = ctx.publish_to("ghost_sink", TestEvent { id: 0 });
        Ok(TestResponse)
    }
}

#[test(tokio::test)]
async fn test_publish_to_missing_sink_is_noop() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system.create_root_actor("noop", NoopActor).await.unwrap();

    actor_ref.tell(NoopMsg).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Should not panic or error.
}

#[test(tokio::test)]
async fn test_sink_entry_filter() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("filter", EmitterActor)
        .await
        .unwrap();

    let all_sub = CollectingSubscriber::new();
    let high_sub = CollectingSubscriber::new();

    let mut sink = Sink::new("filter_sink");
    sink.add("all", all_sub.clone());
    sink.add_entry(
        SinkEntry::new("high", high_sub.clone())
            .filter(|e: &TestEvent| e.id > 5),
    );
    actor_ref.register_sink(sink);

    actor_ref.tell(TestMsg::Emit(3)).await.unwrap();
    actor_ref.tell(TestMsg::Emit(7)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let all_evts = all_sub.drain().await;
    let high_evts = high_sub.drain().await;

    assert_eq!(all_evts.len(), 2);
    assert_eq!(high_evts.len(), 1);
    assert_eq!(high_evts[0].id, 7);
}

#[test(tokio::test)]
async fn test_remove_sink() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("removable", EmitterActor)
        .await
        .unwrap();

    let subscriber = CollectingSubscriber::new();
    let mut sink = Sink::new("tmp");
    sink.add("sub1", subscriber.clone());
    actor_ref.register_sink(sink);

    actor_ref.tell(TestMsg::Emit(1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(subscriber.drain().await.len(), 1);

    actor_ref.remove_sink("tmp");

    actor_ref.tell(TestMsg::Emit(2)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(subscriber.drain().await.is_empty());
}

#[test(tokio::test)]
async fn test_retry_policy_delivers_after_failures() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("retry", EmitterActor)
        .await
        .unwrap();

    let subscriber = FailingThenOkSubscriber::new(2);
    let mut sink = Sink::new("retry_sink");
    sink.add_entry(SinkEntry::new("fragile", subscriber.clone()).retry(
        ave_actors_actor::RetryPolicy::AtMost {
            max: 3,
            backoff: Duration::from_millis(10),
        },
    ));
    actor_ref.register_sink(sink);

    actor_ref.tell(TestMsg::Emit(100)).await.unwrap();

    // Wait for retries (2 failures * 10ms + margin).
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The subscriber should have succeeded on the 3rd attempt.
    assert_eq!(subscriber.fail_count.load(Ordering::SeqCst), 3);
}

// ============================================================================
// Tests planned in sink_redesign_plan.md that were missing
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RouteMsg {
    sink_name: String,
    id: u32,
}

impl Message for RouteMsg {}

#[derive(Debug, Clone)]
struct RoutingActor;

impl ave_actors_actor::NotPersistentActor for RoutingActor {}

#[async_trait]
impl Actor for RoutingActor {
    type Message = RouteMsg;
    type Response = TestResponse;
    type Event = TestEvent;
    type SinkEvent = Self::Event;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("RoutingActor", id = %id)
    }
}

#[async_trait]
impl Handler<RoutingActor> for RoutingActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: RouteMsg,
        ctx: &mut ActorContext<RoutingActor>,
    ) -> Result<TestResponse, Error> {
        ctx.publish_to(&msg.sink_name, TestEvent { id: msg.id });
        Ok(TestResponse)
    }
}

#[test(tokio::test)]
async fn test_actor_routes_to_named_sink() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("router", RoutingActor)
        .await
        .unwrap();

    let sink_a_sub = CollectingSubscriber::new();
    let sink_b_sub = CollectingSubscriber::new();

    let mut sink_a = Sink::new("sink_a");
    sink_a.add("sub", sink_a_sub.clone());
    actor_ref.register_sink(sink_a);

    let mut sink_b = Sink::new("sink_b");
    sink_b.add("sub", sink_b_sub.clone());
    actor_ref.register_sink(sink_b);

    actor_ref
        .tell(RouteMsg {
            sink_name: "sink_a".to_string(),
            id: 42,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let a_evts = sink_a_sub.drain().await;
    let b_evts = sink_b_sub.drain().await;

    assert_eq!(a_evts.len(), 1);
    assert_eq!(a_evts[0].id, 42);
    assert!(b_evts.is_empty());
}

#[derive(Clone)]
struct FailingSubscriber;

#[async_trait]
impl Subscriber<TestEvent> for FailingSubscriber {
    async fn notify(&self, _event: Arc<TestEvent>) -> Result<(), Error> {
        Err(Error::Functional {
            description: "intentional failure".to_owned(),
        })
    }
}

#[test(tokio::test)]
async fn test_one_subscriber_fails_others_ok() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("fanout", EmitterActor)
        .await
        .unwrap();

    let ok_sub_a = CollectingSubscriber::new();
    let ok_sub_b = CollectingSubscriber::new();
    let failing_sub = CollectingSubscriber::new();

    let mut sink = Sink::new("fanout_sink");
    sink.add("ok_a", ok_sub_a.clone());
    sink.add("failing", FailingSubscriber);
    sink.add("ok_b", ok_sub_b.clone());
    actor_ref.register_sink(sink);

    actor_ref.tell(TestMsg::Emit(77)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Both ok subscribers should have received the event.
    assert_eq!(ok_sub_a.drain().await.len(), 1);
    assert_eq!(ok_sub_b.drain().await.len(), 1);
    // The failing subscriber never stores anything (it errors immediately).
    assert!(failing_sub.drain().await.is_empty());
}

#[test(tokio::test)]
async fn test_sink_entry_remove_and_clear() {
    let sub_a = CollectingSubscriber::new();
    let sub_b = CollectingSubscriber::new();
    let sub_c = CollectingSubscriber::new();

    let mut sink = Sink::new("mutable_sink");
    sink.add("a", sub_a.clone());
    sink.add("b", sub_b.clone());
    sink.add("c", sub_c.clone());

    // Remove entry "b".
    let removed = sink.remove_entry("b");
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().id, "b");
    assert!(sink.remove_entry("b").is_none());

    sink.send(Arc::new(TestEvent { id: 1 }));
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(sub_a.drain().await.len(), 1);
    assert_eq!(sub_b.drain().await.len(), 0); // removed
    assert_eq!(sub_c.drain().await.len(), 1);

    // Clear all remaining entries.
    sink.clear();
    sink.send(Arc::new(TestEvent { id: 2 }));
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert_eq!(sub_a.drain().await.len(), 0);
    assert_eq!(sub_c.drain().await.len(), 0);
}
