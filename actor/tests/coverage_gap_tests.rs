//! Integration tests targeting specific coverage gaps in ave-actors-actor.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, ChildAction, Error,
    Event, Handler, IntervalStrategy, Message, NoIntervalStrategy, Response,
    RetryActor, RetryMessage, ShutdownReason, Strategy, SupervisionStrategy,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use test_log::test;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// ============================================================================
// Helpers
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SimpleEvent(pub u32);

impl Event for SimpleEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SimpleMsg;

impl Message for SimpleMsg {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SimpleResponse(pub u32);

impl Response for SimpleResponse {}

// ============================================================================
// ShutdownReason, crash_system, subscribe_system_events
// ============================================================================

#[test(tokio::test)]
async fn test_crash_system_returns_crash_reason() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    system.crash_system();

    let reason = tokio::time::timeout(Duration::from_secs(2), runner_handle)
        .await
        .expect("runner should finish")
        .expect("runner should not panic");

    assert_eq!(reason, ave_actors_actor::ShutdownReason::Crash);
}

#[derive(Debug, Clone)]
struct EventEmitterActor;

impl ave_actors_actor::NotPersistentActor for EventEmitterActor {}

/// Parent that observes errors escalated by its `EventEmitterActor` child.
#[derive(Clone)]
struct ErrorObservingParent {
    errors_seen: Arc<AtomicUsize>,
}

impl ave_actors_actor::NotPersistentActor for ErrorObservingParent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ErrorObservingMsg {
    Trigger,
}

impl Message for ErrorObservingMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorObservingResponse(pub usize);

impl Response for ErrorObservingResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorObservingEvent;

impl Event for ErrorObservingEvent {}

#[async_trait]
impl Actor for ErrorObservingParent {
    type Message = ErrorObservingMsg;
    type Response = ErrorObservingResponse;
    type Event = ErrorObservingEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ErrorObservingParent", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        ctx.create_child("emitter", EventEmitterActor).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Self> for ErrorObservingParent {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: ErrorObservingMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<ErrorObservingResponse, Error> {
        match msg {
            ErrorObservingMsg::Trigger => Ok(ErrorObservingResponse(
                self.errors_seen.load(Ordering::SeqCst),
            )),
        }
    }

    async fn on_child_error(
        &mut self,
        _error: Error,
        _ctx: &mut ActorContext<Self>,
    ) {
        self.errors_seen.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl Actor for EventEmitterActor {
    type Message = SimpleMsg;
    type Response = ();
    type Event = SimpleEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("EventEmitterActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for EventEmitterActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: SimpleMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        ctx.get_parent::<ErrorObservingParent>()
            .await?
            .emit_error(Error::Functional {
                description: "root error".to_owned(),
            })
            .await?;
        Ok(())
    }
}

#[test(tokio::test)]
async fn test_child_error_observed_by_parent() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let parent = ErrorObservingParent {
        errors_seen: Arc::new(AtomicUsize::new(0)),
    };
    let parent_ref = system
        .create_root_actor("emitter_parent", parent)
        .await
        .unwrap();

    let actor_ref: ActorRef<EventEmitterActor> = system
        .get_actor(&ActorPath::from("/user/emitter_parent/emitter"))
        .await
        .unwrap();

    actor_ref.tell(SimpleMsg).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let count = parent_ref.ask(ErrorObservingMsg::Trigger).await.unwrap().0;
        if count >= 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "parent should observe the child error"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    let _ = runner_handle.await;
}

// ============================================================================
// get_actor error, children, get_child, reference, ask_timeout
// ============================================================================

#[derive(Debug, Clone)]
struct MinimalActor;

impl ave_actors_actor::NotPersistentActor for MinimalActor {}

#[async_trait]
impl Actor for MinimalActor {
    type Message = SimpleMsg;
    type Response = SimpleResponse;
    type Event = SimpleEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("MinimalActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for MinimalActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: SimpleMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<SimpleResponse, Error> {
        // Use reference() to cover that path
        let _me = ctx.reference().await?;
        // Use get_child on non-existent child
        let _ = ctx.get_child::<Self>("nope").await;
        Ok(SimpleResponse(1))
    }
}

#[test(tokio::test)]
async fn test_get_actor_not_found() {
    let (system, _runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());

    let result = system
        .get_actor::<MinimalActor>(&ActorPath::from("/user/nonexistent"))
        .await;
    assert!(matches!(result, Err(Error::NotFound { .. })));
}

#[test(tokio::test)]
async fn test_ask_timeout_hits_deadline() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    #[derive(Debug, Clone)]
    struct SlowActor;
    impl ave_actors_actor::NotPersistentActor for SlowActor {}

    #[async_trait]
    impl Actor for SlowActor {
        type Message = SimpleMsg;
        type Response = ();
        type Event = SimpleEvent;
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;
        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("SlowActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for SlowActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: SimpleMsg,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        }
    }

    let actor_ref = system.create_root_actor("slow", SlowActor).await.unwrap();
    let result = actor_ref
        .ask_timeout(SimpleMsg, Duration::from_millis(50))
        .await;
    assert!(matches!(result, Err(Error::Timeout { .. })));
}

#[test(tokio::test)]
async fn test_ask_timeout_success() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("fast", MinimalActor)
        .await
        .unwrap();
    let result = actor_ref
        .ask_timeout(SimpleMsg, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(result.0, 1);
}

// ============================================================================
// Child error / fault propagation to parent
// ============================================================================

#[derive(Debug, Clone)]
struct ParentOfFaultyChild {
    child_fault_count: Arc<AtomicUsize>,
    child_error_count: Arc<AtomicUsize>,
    action_to_return: ChildAction,
}

impl ave_actors_actor::NotPersistentActor for ParentOfFaultyChild {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ParentMsg {
    CreateFaultyChild,
    CreateErrorChild,
    GetCounts,
}

impl Message for ParentMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ParentResponse {
    Counts(usize, usize),
}

impl Response for ParentResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParentEvent;

impl Event for ParentEvent {}

#[async_trait]
impl Actor for ParentOfFaultyChild {
    type Message = ParentMsg;
    type Response = ParentResponse;
    type Event = ParentEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ParentOfFaultyChild", id = %id)
    }

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Retry(Strategy::Interval(IntervalStrategy::new(
            3,
            Duration::from_millis(10),
        )))
    }
}

#[async_trait]
impl Handler<Self> for ParentOfFaultyChild {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: ParentMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<ParentResponse, Error> {
        match msg {
            ParentMsg::CreateFaultyChild => {
                let child = FaultyChildActor;
                let _ = ctx.create_child("faulty", child).await?;
                Ok(ParentResponse::Counts(0, 0))
            }
            ParentMsg::CreateErrorChild => {
                let child = ErrorChildActor;
                let _ = ctx.create_child("error_child", child).await?;
                Ok(ParentResponse::Counts(0, 0))
            }
            ParentMsg::GetCounts => Ok(ParentResponse::Counts(
                self.child_fault_count.load(Ordering::SeqCst),
                self.child_error_count.load(Ordering::SeqCst),
            )),
        }
    }

    async fn on_child_error(
        &mut self,
        _error: Error,
        _ctx: &mut ActorContext<Self>,
    ) {
        self.child_error_count.fetch_add(1, Ordering::SeqCst);
    }

    async fn on_child_fault(
        &mut self,
        _error: Error,
        _ctx: &mut ActorContext<Self>,
    ) -> ChildAction {
        self.child_fault_count.fetch_add(1, Ordering::SeqCst);
        self.action_to_return.clone()
    }
}

// Child that emits a fault via emit_fail
#[derive(Debug, Clone)]
struct FaultyChildActor;

impl ave_actors_actor::NotPersistentActor for FaultyChildActor {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultyChildMsg;

impl Message for FaultyChildMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultyChildResponse;

impl Response for FaultyChildResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FaultyChildEvent;

impl Event for FaultyChildEvent {}

#[async_trait]
impl Actor for FaultyChildActor {
    type Message = FaultyChildMsg;
    type Response = FaultyChildResponse;
    type Event = FaultyChildEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("FaultyChildActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for FaultyChildActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: FaultyChildMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<FaultyChildResponse, Error> {
        ctx.get_parent::<ParentOfFaultyChild>()
            .await?
            .emit_fail(Error::Functional {
                description: "child fault".to_owned(),
            })
            .await?;
        Ok(FaultyChildResponse)
    }
}

// Child that emits an error via emit_error
#[derive(Debug, Clone)]
struct ErrorChildActor;

impl ave_actors_actor::NotPersistentActor for ErrorChildActor {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorChildMsg;

impl Message for ErrorChildMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorChildResponse;

impl Response for ErrorChildResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorChildEvent;

impl Event for ErrorChildEvent {}

#[async_trait]
impl Actor for ErrorChildActor {
    type Message = ErrorChildMsg;
    type Response = ErrorChildResponse;
    type Event = ErrorChildEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ErrorChildActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for ErrorChildActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: ErrorChildMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<ErrorChildResponse, Error> {
        ctx.get_parent::<ParentOfFaultyChild>()
            .await?
            .emit_error(Error::Functional {
                description: "child error".to_owned(),
            })
            .await?;
        Ok(ErrorChildResponse)
    }
}

#[test(tokio::test)]
async fn test_child_fault_propagates_to_parent() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let parent = ParentOfFaultyChild {
        child_fault_count: Arc::new(AtomicUsize::new(0)),
        child_error_count: Arc::new(AtomicUsize::new(0)),
        action_to_return: ChildAction::Stop,
    };

    let parent_ref = system
        .create_root_actor("faulty_parent", parent)
        .await
        .unwrap();

    parent_ref.tell(ParentMsg::CreateFaultyChild).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a message to the child to trigger emit_fail
    let child = system
        .get_actor::<FaultyChildActor>(&ActorPath::from(
            "/user/faulty_parent/faulty",
        ))
        .await
        .unwrap();
    child.tell(FaultyChildMsg).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let counts = parent_ref.ask(ParentMsg::GetCounts).await.unwrap();
    match counts {
        ParentResponse::Counts(faults, _errors) => {
            assert!(
                faults >= 1,
                "parent should have received at least one child fault"
            );
        }
    }
}

#[test(tokio::test)]
async fn test_child_error_propagates_to_parent() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let parent = ParentOfFaultyChild {
        child_fault_count: Arc::new(AtomicUsize::new(0)),
        child_error_count: Arc::new(AtomicUsize::new(0)),
        action_to_return: ChildAction::Stop,
    };

    let parent_ref = system
        .create_root_actor("error_parent", parent)
        .await
        .unwrap();

    parent_ref.tell(ParentMsg::CreateErrorChild).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send a message to the child to trigger emit_error
    let child = system
        .get_actor::<ErrorChildActor>(&ActorPath::from(
            "/user/error_parent/error_child",
        ))
        .await
        .unwrap();
    child.tell(ErrorChildMsg).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let counts = parent_ref.ask(ParentMsg::GetCounts).await.unwrap();
    match counts {
        ParentResponse::Counts(_faults, errors) => {
            assert!(
                errors >= 1,
                "parent should have received at least one child error"
            );
        }
    }
}

// ============================================================================
// create_child duplicate name error
// ============================================================================

#[derive(Debug, Clone)]
struct ChildCreatorActor;

impl ave_actors_actor::NotPersistentActor for ChildCreatorActor {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CreatorMsg {
    CreateChild,
    CreateDuplicate,
}

impl Message for CreatorMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CreatorResponse {
    Ok,
    Err(String),
}

impl Response for CreatorResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreatorEvent;

impl Event for CreatorEvent {}

#[async_trait]
impl Actor for ChildCreatorActor {
    type Message = CreatorMsg;
    type Response = CreatorResponse;
    type Event = CreatorEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ChildCreatorActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for ChildCreatorActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: CreatorMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<CreatorResponse, Error> {
        match msg {
            CreatorMsg::CreateChild => {
                ctx.create_child("dup", MinimalActor).await?;
                Ok(CreatorResponse::Ok)
            }
            CreatorMsg::CreateDuplicate => {
                match ctx.create_child("dup", MinimalActor).await {
                    Ok(_) => Ok(CreatorResponse::Ok),
                    Err(e) => Ok(CreatorResponse::Err(e.to_string())),
                }
            }
        }
    }
}

#[test(tokio::test)]
async fn test_create_child_duplicate_returns_error() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let parent_ref = system
        .create_root_actor("creator", ChildCreatorActor)
        .await
        .unwrap();

    parent_ref.tell(CreatorMsg::CreateChild).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let result = parent_ref.ask(CreatorMsg::CreateDuplicate).await.unwrap();
    match result {
        CreatorResponse::Err(msg) => {
            assert!(msg.contains("Exists") || msg.contains("already"));
        }
        other => panic!("Expected error response, got {:?}", other),
    }
}

// ============================================================================
// Root actor emit_fail stops the actor (no parent to escalate)
// ============================================================================

/// Parent that stops its child when it reports a fault.
#[derive(Clone)]
struct StopOnFaultParent;

impl ave_actors_actor::NotPersistentActor for StopOnFaultParent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RootFailMsg;

impl Message for RootFailMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RootFailResponse;

impl Response for RootFailResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RootFailEvent;

impl Event for RootFailEvent {}

#[async_trait]
impl Actor for StopOnFaultParent {
    type Message = RootFailMsg;
    type Response = RootFailResponse;
    type Event = RootFailEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("StopOnFaultParent", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        ctx.create_child("fail_child", RootFailActor).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Self> for StopOnFaultParent {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: RootFailMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<RootFailResponse, Error> {
        Ok(RootFailResponse)
    }

    async fn on_child_fault(
        &mut self,
        _error: Error,
        _ctx: &mut ActorContext<Self>,
    ) -> ChildAction {
        ChildAction::Stop
    }
}

#[derive(Debug, Clone)]
struct RootFailActor;

impl ave_actors_actor::NotPersistentActor for RootFailActor {}

#[async_trait]
impl Actor for RootFailActor {
    type Message = RootFailMsg;
    type Response = RootFailResponse;
    type Event = RootFailEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("RootFailActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for RootFailActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: RootFailMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<RootFailResponse, Error> {
        ctx.get_parent::<StopOnFaultParent>()
            .await?
            .emit_fail(Error::Functional {
                description: "root fail".to_owned(),
            })
            .await?;
        Ok(RootFailResponse)
    }
}

#[test(tokio::test)]
async fn test_child_emit_fail_stops_actor() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let _parent_ref = system
        .create_root_actor("root_fail", StopOnFaultParent)
        .await
        .unwrap();

    let actor_ref: ActorRef<RootFailActor> = system
        .get_actor(&ActorPath::from("/user/root_fail/fail_child"))
        .await
        .unwrap();

    actor_ref.tell(RootFailMsg).await.unwrap();

    // Actor should stop after emit_fail
    tokio::time::timeout(Duration::from_secs(2), actor_ref.closed())
        .await
        .expect("actor should stop");
}

// ============================================================================
// ChildStopped notification via inner_handle
// ============================================================================

#[derive(Debug, Clone)]
struct WatchingParent {
    child_stopped: Arc<Mutex<bool>>,
}

impl ave_actors_actor::NotPersistentActor for WatchingParent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum WatchMsg {
    SpawnChild,
    StopChild,
    IsChildStopped,
}

impl Message for WatchMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WatchResponse(pub bool);

impl Response for WatchResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WatchEvent;

impl Event for WatchEvent {}

#[async_trait]
impl Actor for WatchingParent {
    type Message = WatchMsg;
    type Response = WatchResponse;
    type Event = WatchEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("WatchingParent", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for WatchingParent {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: WatchMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<WatchResponse, Error> {
        match msg {
            WatchMsg::SpawnChild => {
                let child = MinimalActor;
                ctx.create_child("watched", child).await?;
                Ok(WatchResponse(false))
            }
            WatchMsg::StopChild => {
                if let Ok(child) =
                    ctx.get_child::<MinimalActor>("watched").await
                {
                    child.ask_stop().await?;
                }
                Ok(WatchResponse(false))
            }
            WatchMsg::IsChildStopped => {
                let stopped = *self.child_stopped.lock().await;
                Ok(WatchResponse(stopped))
            }
        }
    }

    async fn on_child_error(
        &mut self,
        _error: Error,
        _ctx: &mut ActorContext<Self>,
    ) {
        // Default logs error; we override to do nothing extra.
    }
}

#[test(tokio::test)]
async fn test_child_stopped_removes_from_parent() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let parent = WatchingParent {
        child_stopped: Arc::new(Mutex::new(false)),
    };

    let parent_ref =
        system.create_root_actor("watching", parent).await.unwrap();

    parent_ref.tell(WatchMsg::SpawnChild).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    parent_ref.tell(WatchMsg::StopChild).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // After child stops, get_child should fail
    let result = system
        .get_actor::<MinimalActor>(&ActorPath::from("/user/watching/watched"))
        .await;
    assert!(result.is_err());
}

// ============================================================================
// Supervision strategy Stop (no retries)
// ============================================================================

#[derive(Debug, Clone)]
struct AlwaysFailActor;

impl ave_actors_actor::NotPersistentActor for AlwaysFailActor {}

#[async_trait]
impl Actor for AlwaysFailActor {
    type Message = SimpleMsg;
    type Response = ();
    type Event = SimpleEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("AlwaysFailActor", id = %id)
    }

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Stop
    }

    async fn pre_start(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Err(Error::FunctionalCritical {
            description: "always fail".to_owned(),
        })
    }
}

#[async_trait]
impl Handler<Self> for AlwaysFailActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: SimpleMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

#[test(tokio::test)]
async fn test_stop_supervision_no_retries() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let result = system
        .create_root_actor("always_fail", AlwaysFailActor)
        .await;
    assert!(result.is_err());
}

// ============================================================================
// RetryActor with parent message (covers new_with_parent_message)
// ============================================================================

#[derive(Debug, Clone)]
struct NotifyParentTarget;

impl ave_actors_actor::NotPersistentActor for NotifyParentTarget {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NotifyTargetMsg;

impl Message for NotifyTargetMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NotifyTargetResponse;

impl Response for NotifyTargetResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NotifyTargetEvent;

impl Event for NotifyTargetEvent {}

#[async_trait]
impl Actor for NotifyParentTarget {
    type Message = NotifyTargetMsg;
    type Response = NotifyTargetResponse;
    type Event = NotifyTargetEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("NotifyParentTarget", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for NotifyParentTarget {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: NotifyTargetMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<NotifyTargetResponse, Error> {
        Ok(NotifyTargetResponse)
    }
}

#[derive(Debug, Clone)]
struct RetryNotifyParent {
    completions: Arc<AtomicUsize>,
}

impl ave_actors_actor::NotPersistentActor for RetryNotifyParent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RetryNotifyMsg {
    Start,
    Done,
}

impl Message for RetryNotifyMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryNotifyResponse;

impl Response for RetryNotifyResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RetryNotifyEvent;

impl Event for RetryNotifyEvent {}

#[async_trait]
impl Actor for RetryNotifyParent {
    type Message = RetryNotifyMsg;
    type Response = RetryNotifyResponse;
    type Event = RetryNotifyEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("RetryNotifyParent", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        let retry = RetryActor::new_with_parent_message::<Self>(
            NotifyParentTarget,
            NotifyTargetMsg,
            Strategy::NoInterval(NoIntervalStrategy::new(2)),
            RetryNotifyMsg::Done,
        );
        let retry_ref: ActorRef<RetryActor<NotifyParentTarget>> =
            ctx.create_child("retry", retry).await?;
        retry_ref.tell(RetryMessage::Retry).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Self> for RetryNotifyParent {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: RetryNotifyMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<RetryNotifyResponse, Error> {
        if matches!(msg, RetryNotifyMsg::Done) {
            self.completions.fetch_add(1, Ordering::SeqCst);
        }
        Ok(RetryNotifyResponse)
    }
}

#[test(tokio::test)]
async fn test_retry_actor_with_parent_notification() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let completions = Arc::new(AtomicUsize::new(0));
    let parent = RetryNotifyParent {
        completions: completions.clone(),
    };

    let _parent_ref: ActorRef<RetryNotifyParent> = system
        .create_root_actor("retry_notify", parent)
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if completions.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("parent should receive Done notification");
}

// ============================================================================
// Non-critical messages discarded during shutdown drain
// ============================================================================

#[derive(Debug, Clone)]
struct DrainTestActor {
    processed: Arc<Mutex<Vec<&'static str>>>,
}

impl ave_actors_actor::NotPersistentActor for DrainTestActor {}

#[derive(Debug, Clone)]
enum DrainTestMsg {
    Block,
    NonCritical,
}

impl Message for DrainTestMsg {
    fn is_critical(&self) -> bool {
        matches!(self, Self::Block)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DrainTestResponse;

impl Response for DrainTestResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DrainTestEvent;

impl Event for DrainTestEvent {}

#[async_trait]
impl Actor for DrainTestActor {
    type Message = DrainTestMsg;
    type Response = DrainTestResponse;
    type Event = DrainTestEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("DrainTestActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for DrainTestActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: DrainTestMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<DrainTestResponse, Error> {
        match msg {
            DrainTestMsg::Block => {
                tokio::time::sleep(Duration::from_millis(200)).await;
                self.processed.lock().await.push("block");
            }
            DrainTestMsg::NonCritical => {
                self.processed.lock().await.push("non_critical");
            }
        }
        Ok(DrainTestResponse)
    }
}

#[test(tokio::test)]
async fn test_non_critical_discarded_on_stop() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let processed = Arc::new(Mutex::new(Vec::new()));
    let actor = DrainTestActor {
        processed: processed.clone(),
    };

    let actor_ref = system.create_root_actor("drain", actor).await.unwrap();

    // Block the actor first
    actor_ref.tell(DrainTestMsg::Block).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Queue non-critical message while blocked
    let ask_join = tokio::spawn({
        let r = actor_ref.clone();
        async move { r.ask(DrainTestMsg::NonCritical).await }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Stop the actor
    actor_ref.tell_stop().await;

    let result = tokio::time::timeout(Duration::from_secs(2), ask_join)
        .await
        .expect("should finish")
        .expect("join ok");

    // Non-critical ask should receive ActorStopped
    assert!(matches!(result, Err(Error::ActorStopped)));
}

// ============================================================================
// SystemRunner::run handles child error propagation and closed channel
// ============================================================================

#[derive(Clone)]
struct RunnerErrorParent {
    errors_seen: Arc<AtomicUsize>,
}

impl ave_actors_actor::NotPersistentActor for RunnerErrorParent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RunnerErrorMsg {
    GetCount,
}

impl Message for RunnerErrorMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunnerErrorResponse(pub usize);

impl Response for RunnerErrorResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RunnerErrorEvent;

impl Event for RunnerErrorEvent {}

#[async_trait]
impl Actor for RunnerErrorParent {
    type Message = RunnerErrorMsg;
    type Response = RunnerErrorResponse;
    type Event = RunnerErrorEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("RunnerErrorParent", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        ctx.create_child("publisher", ErrorPublisherActor).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Self> for RunnerErrorParent {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: RunnerErrorMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<RunnerErrorResponse, Error> {
        match msg {
            RunnerErrorMsg::GetCount => {
                Ok(RunnerErrorResponse(self.errors_seen.load(Ordering::SeqCst)))
            }
        }
    }

    async fn on_child_error(
        &mut self,
        _error: Error,
        _ctx: &mut ActorContext<Self>,
    ) {
        self.errors_seen.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone)]
struct ErrorPublisherActor;

impl ave_actors_actor::NotPersistentActor for ErrorPublisherActor {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorPublisherMsg;

impl Message for ErrorPublisherMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorPublisherResponse;

impl Response for ErrorPublisherResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorPublisherEvent;

impl Event for ErrorPublisherEvent {}

#[async_trait]
impl Actor for ErrorPublisherActor {
    type Message = ErrorPublisherMsg;
    type Response = ErrorPublisherResponse;
    type Event = ErrorPublisherEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ErrorPublisherActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for ErrorPublisherActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: ErrorPublisherMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<ErrorPublisherResponse, Error> {
        ctx.get_parent::<RunnerErrorParent>()
            .await?
            .emit_error(Error::Functional {
                description: "published".to_owned(),
            })
            .await?;
        Ok(ErrorPublisherResponse)
    }
}

#[test(tokio::test)]
async fn test_system_runner_handles_child_error_events() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());

    // Drive the runner in a task but also intercept its result.
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let parent = RunnerErrorParent {
        errors_seen: Arc::new(AtomicUsize::new(0)),
    };
    let parent_ref = system
        .create_root_actor("error_publisher", parent)
        .await
        .unwrap();

    let actor_ref: ActorRef<ErrorPublisherActor> = system
        .get_actor(&ActorPath::from("/user/error_publisher/publisher"))
        .await
        .unwrap();

    actor_ref.tell(ErrorPublisherMsg).await.unwrap();

    // Give the runner time to process the child error internally
    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = parent_ref.ask(RunnerErrorMsg::GetCount).await.unwrap().0;
    assert!(count >= 1, "parent should have observed the child error");

    system.stop_system();

    let reason = tokio::time::timeout(Duration::from_secs(2), runner_handle)
        .await
        .expect("runner should finish")
        .expect("runner should not panic");

    assert_eq!(reason, ave_actors_actor::ShutdownReason::Graceful);
}

// ============================================================================
// Default trait methods coverage
// ============================================================================

#[derive(Debug, Clone)]
struct DefaultBehaviorActor {
    child_error_count: Arc<AtomicUsize>,
    child_fault_count: Arc<AtomicUsize>,
}

impl ave_actors_actor::NotPersistentActor for DefaultBehaviorActor {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum DefaultBehaviorMsg {
    SpawnErrorChild,
    SpawnFaultChild,
    GetCounts,
}

impl Message for DefaultBehaviorMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DefaultBehaviorResponse {
    errors: usize,
    faults: usize,
}

impl Response for DefaultBehaviorResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DefaultBehaviorEvent;

impl Event for DefaultBehaviorEvent {}

#[async_trait]
impl Actor for DefaultBehaviorActor {
    type Message = DefaultBehaviorMsg;
    type Response = DefaultBehaviorResponse;
    type Event = DefaultBehaviorEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("DefaultBehaviorActor", id = %id)
    }

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Retry(Strategy::Interval(IntervalStrategy::new(
            1,
            Duration::from_millis(10),
        )))
    }

    async fn pre_start(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        // Fail once to trigger pre_restart default
        static FAIL_ONCE: AtomicUsize = AtomicUsize::new(0);
        if FAIL_ONCE.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(Error::FunctionalCritical {
                description: "fail once".to_owned(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<Self> for DefaultBehaviorActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: DefaultBehaviorMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<DefaultBehaviorResponse, Error> {
        match msg {
            DefaultBehaviorMsg::SpawnErrorChild => {
                let child = ErrorChildActor;
                ctx.create_child("err_child", child).await?;
                Ok(DefaultBehaviorResponse {
                    errors: 0,
                    faults: 0,
                })
            }
            DefaultBehaviorMsg::SpawnFaultChild => {
                let child = FaultyChildActor;
                ctx.create_child("fault_child", child).await?;
                Ok(DefaultBehaviorResponse {
                    errors: 0,
                    faults: 0,
                })
            }
            DefaultBehaviorMsg::GetCounts => Ok(DefaultBehaviorResponse {
                errors: self.child_error_count.load(Ordering::SeqCst),
                faults: self.child_fault_count.load(Ordering::SeqCst),
            }),
        }
    }

    // Override on_child_error and on_child_fault just to count them,
    // but we also test that the defaults exist by not overriding in other actors.
    async fn on_child_error(
        &mut self,
        _error: Error,
        _ctx: &mut ActorContext<Self>,
    ) {
        self.child_error_count.fetch_add(1, Ordering::SeqCst);
    }

    async fn on_child_fault(
        &mut self,
        _error: Error,
        _ctx: &mut ActorContext<Self>,
    ) -> ChildAction {
        self.child_fault_count.fetch_add(1, Ordering::SeqCst);
        ChildAction::Stop
    }
}

#[test(tokio::test)]
async fn test_default_pre_restart_and_supervision() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor = DefaultBehaviorActor {
        child_error_count: Arc::new(AtomicUsize::new(0)),
        child_fault_count: Arc::new(AtomicUsize::new(0)),
    };

    let actor_ref = system
        .create_root_actor("defaults_behavior", actor)
        .await
        .unwrap();

    // Wait for retry to succeed
    tokio::time::sleep(Duration::from_millis(200)).await;

    let resp = actor_ref.ask(DefaultBehaviorMsg::GetCounts).await.unwrap();
    assert_eq!(resp.errors, 0);
    assert_eq!(resp.faults, 0);
}

// ============================================================================
// RetryActor End before Retry (covers is_end early return)
// ============================================================================

#[test(tokio::test)]
async fn test_retry_end_before_retry_returns_early() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let retry_actor = RetryActor::new(
        MinimalActor,
        SimpleMsg,
        Strategy::NoInterval(NoIntervalStrategy::new(3)),
    );

    let retry_ref: ActorRef<RetryActor<MinimalActor>> = system
        .create_root_actor("retry_end_first", retry_actor)
        .await
        .unwrap();

    // Send End first, then Retry immediately without sleeping so that
    // End is processed before Retry while the actor is still running.
    retry_ref.tell(RetryMessage::End).await.unwrap();
    retry_ref.tell(RetryMessage::Retry).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), retry_ref.closed())
        .await
        .expect("retry actor should stop");
}

// ============================================================================
// RetryActor double End (retries.rs 144-145)
// ============================================================================

#[test(tokio::test)]
async fn test_retry_double_end_finishes_once() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let retry_actor = RetryActor::new(
        MinimalActor,
        SimpleMsg,
        Strategy::NoInterval(NoIntervalStrategy::new(3)),
    );

    let retry_ref: ActorRef<RetryActor<MinimalActor>> = system
        .create_root_actor("retry_double_end", retry_actor)
        .await
        .unwrap();

    // Send End twice. The second should hit the already-notified path.
    retry_ref.tell(RetryMessage::End).await.unwrap();
    retry_ref.tell(RetryMessage::End).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), retry_ref.closed())
        .await
        .expect("retry actor should stop");
}

// ============================================================================
// respond_stopped when the oneshot receiver has been dropped (handler.rs 66)
// ============================================================================

// ============================================================================
// Root actor already stopped when system sends stop signal (system.rs 147)
// ============================================================================

#[derive(Debug, Clone)]
struct SelfStopActor;

impl ave_actors_actor::NotPersistentActor for SelfStopActor {}

#[derive(Debug, Clone)]
struct SelfStopMsg;

impl Message for SelfStopMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SelfStopResponse;

impl Response for SelfStopResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SelfStopEvent;

impl Event for SelfStopEvent {}

#[async_trait]
impl Actor for SelfStopActor {
    type Message = SelfStopMsg;
    type Response = SelfStopResponse;
    type Event = SelfStopEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("SelfStopActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for SelfStopActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: SelfStopMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<SelfStopResponse, Error> {
        ctx.stop(None).await;
        Ok(SelfStopResponse)
    }
}

#[test(tokio::test)]
async fn test_root_already_stopped_on_system_shutdown() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("self_stop", SelfStopActor)
        .await
        .unwrap();

    // Tell actor to stop itself
    actor_ref.tell(SelfStopMsg).await.unwrap();

    // Wait for the actor to fully terminate
    tokio::time::timeout(Duration::from_secs(2), actor_ref.closed())
        .await
        .expect("actor should terminate");

    // Now stop the system; the root sender is closed, so the system
    // should hit the warn! branch at system.rs:147.
    system.stop_system();

    let shutdown = tokio::time::timeout(Duration::from_secs(2), runner_handle)
        .await
        .expect("runner should finish")
        .expect("runner task should not panic");
    assert_eq!(shutdown, ShutdownReason::Graceful);
}

// ============================================================================
// Typed child error/fault propagation
//
// Demonstrates that a child and its parent do not need to share the same
// ChildError/ChildFault associated types. The child declares its own types
// (which would be used by its own children), but when escalating to the parent
// it uses the types declared by the parent.
// ============================================================================

#[derive(Debug, Clone)]
struct ParentErrorType(pub String);

#[derive(Debug, Clone)]
struct ParentFaultType(pub String);

impl From<Error> for ParentFaultType {
    fn from(err: Error) -> Self {
        Self(err.to_string())
    }
}

#[derive(Debug, Clone)]
struct ChildErrorType;

#[derive(Debug, Clone)]
struct ChildFaultType;

impl From<Error> for ChildFaultType {
    fn from(_err: Error) -> Self {
        Self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TypedChildMsg {
    EmitError,
    EmitFault,
}

impl Message for TypedChildMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypedChildResponse;

impl Response for TypedChildResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypedChildEvent;

impl Event for TypedChildEvent {}

#[derive(Clone)]
struct TypedChild;

impl ave_actors_actor::NotPersistentActor for TypedChild {}

#[async_trait]
impl Actor for TypedChild {
    type Message = TypedChildMsg;
    type Response = TypedChildResponse;
    type Event = TypedChildEvent;
    type SinkEvent = Self::Event;
    type ChildError = ChildErrorType;
    type ChildFault = ChildFaultType;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TypedChild", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for TypedChild {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: TypedChildMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<TypedChildResponse, Error> {
        match msg {
            // The child escalates to the parent using the parent's types, not
            // its own ChildError/ChildFault types.
            TypedChildMsg::EmitError => {
                ctx.get_parent::<TypedParent>()
                    .await?
                    .emit_error(ParentErrorType(
                        "parent typed error".to_owned(),
                    ))
                    .await?;
            }
            TypedChildMsg::EmitFault => {
                ctx.get_parent::<TypedParent>()
                    .await?
                    .emit_fail(ParentFaultType("parent typed fault".to_owned()))
                    .await?;
            }
        }
        Ok(TypedChildResponse)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TypedParentMsg {
    GetErrors,
    GetFaults,
}

impl Message for TypedParentMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypedParentResponse {
    errors: Vec<String>,
    faults: Vec<String>,
}

impl Response for TypedParentResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TypedParentEvent;

impl Event for TypedParentEvent {}

#[derive(Clone)]
struct TypedParent {
    errors: Arc<Mutex<Vec<ParentErrorType>>>,
    faults: Arc<Mutex<Vec<ParentFaultType>>>,
}

impl ave_actors_actor::NotPersistentActor for TypedParent {}

#[async_trait]
impl Actor for TypedParent {
    type Message = TypedParentMsg;
    type Response = TypedParentResponse;
    type Event = TypedParentEvent;
    type SinkEvent = Self::Event;
    type ChildError = ParentErrorType;
    type ChildFault = ParentFaultType;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TypedParent", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        ctx.create_child("typed_child", TypedChild).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Self> for TypedParent {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: TypedParentMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<TypedParentResponse, Error> {
        match msg {
            TypedParentMsg::GetErrors => {
                let errors = self
                    .errors
                    .lock()
                    .await
                    .iter()
                    .map(|e| e.0.clone())
                    .collect();
                Ok(TypedParentResponse {
                    errors,
                    faults: vec![],
                })
            }
            TypedParentMsg::GetFaults => {
                let faults = self
                    .faults
                    .lock()
                    .await
                    .iter()
                    .map(|f| f.0.clone())
                    .collect();
                Ok(TypedParentResponse {
                    errors: vec![],
                    faults,
                })
            }
        }
    }

    async fn on_child_error(
        &mut self,
        error: ParentErrorType,
        _ctx: &mut ActorContext<Self>,
    ) {
        self.errors.lock().await.push(error);
    }

    async fn on_child_fault(
        &mut self,
        fault: ParentFaultType,
        _ctx: &mut ActorContext<Self>,
    ) -> ChildAction {
        self.faults.lock().await.push(fault);
        ChildAction::Stop
    }
}

#[test(tokio::test)]
async fn test_child_and_parent_use_different_error_types() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let parent = TypedParent {
        errors: Arc::new(Mutex::new(vec![])),
        faults: Arc::new(Mutex::new(vec![])),
    };
    let parent_ref = system
        .create_root_actor("typed_parent", parent)
        .await
        .unwrap();

    let child_ref: ActorRef<TypedChild> = system
        .get_actor(&ActorPath::from("/user/typed_parent/typed_child"))
        .await
        .unwrap();

    child_ref.tell(TypedChildMsg::EmitError).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = parent_ref.ask(TypedParentMsg::GetErrors).await.unwrap();
        if !resp.errors.is_empty() {
            assert_eq!(resp.errors, vec!["parent typed error"]);
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("parent did not receive typed error");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    child_ref.tell(TypedChildMsg::EmitFault).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = parent_ref.ask(TypedParentMsg::GetFaults).await.unwrap();
        if !resp.faults.is_empty() {
            assert_eq!(resp.faults, vec!["parent typed fault"]);
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("parent did not receive typed fault");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    let _ = tokio::time::timeout(Duration::from_secs(2), runner_handle)
        .await
        .expect("runner should finish")
        .expect("runner task should not panic");
}
