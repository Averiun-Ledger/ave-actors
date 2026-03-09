use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, Error, Event,
    FixedIntervalStrategy, Handler, Message, NoIntervalStrategy,
    NotPersistentActor, Response, RetryActor, RetryMessage, ShutdownReason,
    Strategy, SupervisionStrategy, SystemEvent, build_tracing_subscriber,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StartMessage {
    Ping,
}

impl Message for StartMessage {}

#[derive(Debug, Clone, PartialEq)]
enum StartResponse {
    Pong,
}

impl Response for StartResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StartEvent;

impl Event for StartEvent {}

#[derive(Clone)]
struct AlwaysFailStartActor;

impl NotPersistentActor for AlwaysFailStartActor {}

#[async_trait]
impl Actor for AlwaysFailStartActor {
    type Message = StartMessage;
    type Response = StartResponse;
    type Event = StartEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("AlwaysFailStartActor", id = %id)
    }

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Stop
    }

    async fn pre_start(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Err(Error::FunctionalCritical {
            description: "forced pre_start failure".to_owned(),
        })
    }
}

#[async_trait]
impl Handler<AlwaysFailStartActor> for AlwaysFailStartActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: StartMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<StartResponse, Error> {
        Ok(StartResponse::Pong)
    }
}

#[derive(Clone)]
struct HealthyActor;

impl NotPersistentActor for HealthyActor {}

#[async_trait]
impl Actor for HealthyActor {
    type Message = StartMessage;
    type Response = StartResponse;
    type Event = StartEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("HealthyActor", id = %id)
    }
}

#[async_trait]
impl Handler<HealthyActor> for HealthyActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: StartMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<StartResponse, Error> {
        Ok(StartResponse::Pong)
    }
}

#[derive(Default)]
struct RestartHooks {
    pre_restart_calls: AtomicUsize,
    ping_calls: AtomicUsize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RestartMessage {
    Ping,
    Fail,
}

impl Message for RestartMessage {}

#[derive(Debug, Clone, PartialEq)]
enum RestartResponse {
    Pong(usize),
    RestartScheduled,
}

impl Response for RestartResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestartEvent;

impl Event for RestartEvent {}

#[derive(Clone)]
struct RuntimeRestartActor {
    hooks: Arc<RestartHooks>,
}

impl RuntimeRestartActor {
    fn new(hooks: Arc<RestartHooks>) -> Self {
        Self { hooks }
    }
}

impl NotPersistentActor for RuntimeRestartActor {}

#[async_trait]
impl Actor for RuntimeRestartActor {
    type Message = RestartMessage;
    type Response = RestartResponse;
    type Event = RestartEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("RuntimeRestartActor", id = %id)
    }

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Retry(Strategy::FixedInterval(
            FixedIntervalStrategy::new(4, Duration::from_millis(10)),
        ))
    }

    async fn pre_restart(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _error: Option<&Error>,
    ) -> Result<(), Error> {
        self.hooks.pre_restart_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl Handler<RuntimeRestartActor> for RuntimeRestartActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: RestartMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<RestartResponse, Error> {
        match msg {
            RestartMessage::Ping => {
                let ping =
                    self.hooks.ping_calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(RestartResponse::Pong(ping))
            }
            RestartMessage::Fail => {
                ctx.emit_fail(Error::FunctionalCritical {
                    description: "forced runtime failure".to_owned(),
                })
                .await?;
                Ok(RestartResponse::RestartScheduled)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CountMessage;

impl Message for CountMessage {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CountEvent;

impl Event for CountEvent {}

#[derive(Clone)]
struct CountingTarget {
    deliveries: Arc<AtomicUsize>,
}

impl NotPersistentActor for CountingTarget {}

#[async_trait]
impl Actor for CountingTarget {
    type Message = CountMessage;
    type Response = ();
    type Event = CountEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("CountingTarget", id = %id)
    }
}

#[async_trait]
impl Handler<CountingTarget> for CountingTarget {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: CountMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RootErrorMessage {
    Emit,
    Ping,
}

impl Message for RootErrorMessage {}

#[derive(Debug, Clone, PartialEq)]
enum RootErrorResponse {
    Ok,
}

impl Response for RootErrorResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RootErrorEvent;

impl Event for RootErrorEvent {}

#[derive(Clone)]
struct RootErrorActor;

impl NotPersistentActor for RootErrorActor {}

#[async_trait]
impl Actor for RootErrorActor {
    type Message = RootErrorMessage;
    type Response = RootErrorResponse;
    type Event = RootErrorEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("RootErrorActor", id = %id)
    }
}

#[async_trait]
impl Handler<RootErrorActor> for RootErrorActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: RootErrorMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<RootErrorResponse, Error> {
        match msg {
            RootErrorMessage::Emit => {
                ctx.emit_error(Error::Functional {
                    description: "root emitted error".to_owned(),
                })
                .await?;
            }
            RootErrorMessage::Ping => {}
        }
        Ok(RootErrorResponse::Ok)
    }
}

#[derive(Clone)]
struct HangingStartActor {
    started: Arc<Notify>,
}

impl NotPersistentActor for HangingStartActor {}

#[async_trait]
impl Actor for HangingStartActor {
    type Message = StartMessage;
    type Response = StartResponse;
    type Event = StartEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("HangingStartActor", id = %id)
    }

    fn startup_timeout() -> Option<Duration> {
        Some(Duration::from_millis(20))
    }

    async fn pre_start(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        self.started.notify_one();
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(())
    }
}

#[async_trait]
impl Handler<HangingStartActor> for HangingStartActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: StartMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<StartResponse, Error> {
        Ok(StartResponse::Pong)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StopControl {
    BlockChild,
}

impl Message for StopControl {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StopEvent;

impl Event for StopEvent {}

#[derive(Clone)]
struct BlockingChild {
    entered: Arc<Notify>,
}

impl NotPersistentActor for BlockingChild {}

#[async_trait]
impl Actor for BlockingChild {
    type Message = StopControl;
    type Response = ();
    type Event = StopEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("BlockingChild", id = %id)
    }

    fn stop_timeout() -> Option<Duration> {
        Some(Duration::from_millis(20))
    }
}

#[async_trait]
impl Handler<BlockingChild> for BlockingChild {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: StopControl,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        self.entered.notify_one();
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(())
    }
}

#[derive(Clone)]
struct ParentWithBlockingChild {
    child_entered: Arc<Notify>,
}

impl NotPersistentActor for ParentWithBlockingChild {}

#[async_trait]
impl Actor for ParentWithBlockingChild {
    type Message = StopControl;
    type Response = ();
    type Event = StopEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ParentWithBlockingChild", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        let child = BlockingChild {
            entered: self.child_entered.clone(),
        };
        let _: ActorRef<BlockingChild> =
            ctx.create_child("child", child).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<ParentWithBlockingChild> for ParentWithBlockingChild {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: StopControl,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        match msg {
            StopControl::BlockChild => {
                let child = ctx.get_child::<BlockingChild>("child").await?;
                child.tell(StopControl::BlockChild).await?;
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct BlockingRootActor {
    entered: Arc<Notify>,
}

impl NotPersistentActor for BlockingRootActor {}

#[async_trait]
impl Actor for BlockingRootActor {
    type Message = StopControl;
    type Response = ();
    type Event = StopEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("BlockingRootActor", id = %id)
    }

    fn stop_timeout() -> Option<Duration> {
        Some(Duration::from_millis(20))
    }
}

#[async_trait]
impl Handler<BlockingRootActor> for BlockingRootActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: StopControl,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        self.entered.notify_one();
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(())
    }
}

async fn shutdown_system(mut handle: ActorSystemRefHandle) {
    handle.system.stop_system();
    let shutdown =
        tokio::time::timeout(Duration::from_secs(5), &mut handle.runner)
            .await
            .expect("runner should finish within timeout")
            .expect("runner task should not panic");
    assert_eq!(shutdown, ShutdownReason::Graceful);
}

struct ActorSystemRefHandle {
    system: ave_actors_actor::SystemRef,
    runner: tokio::task::JoinHandle<ShutdownReason>,
}

fn spawn_system() -> ActorSystemRefHandle {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner = tokio::spawn(async move { runner.run().await });
    ActorSystemRefHandle { system, runner }
}

#[tokio::test]
async fn test_cleanup_after_pre_start_failure_allows_recreate() {
    build_tracing_subscriber();
    let handle = spawn_system();

    let result = handle
        .system
        .create_root_actor("reusable-name", AlwaysFailStartActor)
        .await;
    assert!(result.is_err());

    let actor_ref = handle
        .system
        .create_root_actor("reusable-name", HealthyActor)
        .await
        .expect("failed actor should have been removed from registry");

    let response = actor_ref.ask(StartMessage::Ping).await.unwrap();
    assert_eq!(response, StartResponse::Pong);

    shutdown_system(handle).await;
}

#[tokio::test]
async fn test_startup_timeout_aborts_stuck_actor_and_releases_path() {
    build_tracing_subscriber();
    let handle = spawn_system();
    let started = Arc::new(Notify::new());

    let result = handle
        .system
        .create_root_actor(
            "slow-start",
            HangingStartActor {
                started: started.clone(),
            },
        )
        .await;

    assert!(matches!(result, Err(Error::Timeout { ms }) if ms == 20));
    assert!(
        handle
            .system
            .get_actor::<HangingStartActor>(&ActorPath::from(
                "/user/slow-start"
            ))
            .await
            .is_err()
    );

    let actor_ref = handle
        .system
        .create_root_actor("slow-start", HealthyActor)
        .await
        .expect("startup timeout should release actor path");
    let response = actor_ref.ask(StartMessage::Ping).await.unwrap();
    assert_eq!(response, StartResponse::Pong);

    shutdown_system(handle).await;
}

#[tokio::test]
async fn test_runtime_restart_preserves_registry_lookup() {
    build_tracing_subscriber();
    let handle = spawn_system();
    let hooks = Arc::new(RestartHooks::default());

    let actor_ref = handle
        .system
        .create_root_actor(
            "restart-registry",
            RuntimeRestartActor::new(hooks.clone()),
        )
        .await
        .unwrap();

    actor_ref.tell(RestartMessage::Fail).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let path = ActorPath::from("/user/restart-registry");
    loop {
        if hooks.pre_restart_calls.load(Ordering::SeqCst) > 0 {
            let fetched = handle
                .system
                .get_actor::<RuntimeRestartActor>(&path)
                .await
                .expect("restarted actor should remain registered");
            let response = fetched.ask(RestartMessage::Ping).await.unwrap();
            assert_eq!(response, RestartResponse::Pong(1));
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "actor never restarted"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown_system(handle).await;
}

#[tokio::test]
async fn test_runtime_restart_resumes_mailbox_processing() {
    build_tracing_subscriber();
    let handle = spawn_system();
    let hooks = Arc::new(RestartHooks::default());

    let actor_ref: ActorRef<RuntimeRestartActor> = handle
        .system
        .create_root_actor(
            "restart-mailbox",
            RuntimeRestartActor::new(hooks.clone()),
        )
        .await
        .unwrap();

    actor_ref.tell(RestartMessage::Fail).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if hooks.pre_restart_calls.load(Ordering::SeqCst) > 0 {
            match actor_ref.ask(RestartMessage::Ping).await {
                Ok(RestartResponse::Pong(count)) => {
                    assert!(count >= 1);
                    break;
                }
                Ok(other) => {
                    panic!("unexpected response after restart: {other:?}")
                }
                Err(Error::ActorStopped) => {}
                Err(error) => {
                    panic!("unexpected ask error after restart: {error:?}")
                }
            }
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "actor did not resume mailbox processing"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    shutdown_system(handle).await;
}

#[tokio::test]
async fn test_retry_actor_with_no_interval_strategy_retries_immediately() {
    build_tracing_subscriber();
    let handle = spawn_system();
    let deliveries = Arc::new(AtomicUsize::new(0));

    let retry_actor = RetryActor::new(
        CountingTarget {
            deliveries: deliveries.clone(),
        },
        CountMessage,
        Strategy::NoInterval(NoIntervalStrategy::new(3)),
    );

    let retry_ref: ActorRef<RetryActor<CountingTarget>> = handle
        .system
        .create_root_actor("no-interval-retry", retry_actor)
        .await
        .unwrap();

    retry_ref.tell(RetryMessage::Retry).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if deliveries.load(Ordering::SeqCst) == 3 {
            break;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "RetryActor with NoIntervalStrategy did not deliver all retries"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    retry_ref.ask_stop().await.unwrap();
    shutdown_system(handle).await;
}

#[tokio::test]
async fn test_create_root_actor_rejected_once_shutdown_starts() {
    build_tracing_subscriber();
    let handle = spawn_system();
    let system = handle.system.clone();

    system.stop_system();

    let result = system.create_root_actor("late-root", HealthyActor).await;
    assert!(matches!(result, Err(Error::SystemStopped)));
    assert!(
        system
            .get_actor::<HealthyActor>(&ActorPath::from("/user/late-root"))
            .await
            .is_err()
    );

    let shutdown = tokio::time::timeout(Duration::from_secs(5), handle.runner)
        .await
        .expect("runner should finish within timeout")
        .expect("runner task should not panic");
    assert_eq!(shutdown, ShutdownReason::Graceful);

    let result = system.create_root_actor("late-root-2", HealthyActor).await;
    assert!(matches!(result, Err(Error::SystemStopped)));
}

#[tokio::test]
async fn test_root_emit_error_publishes_system_event_without_stopping_actor() {
    build_tracing_subscriber();
    let handle = spawn_system();
    let mut system_events = handle.system.subscribe_system_events();

    let actor_ref: ActorRef<RootErrorActor> = handle
        .system
        .create_root_actor("root-error", RootErrorActor)
        .await
        .unwrap();

    actor_ref.tell(RootErrorMessage::Emit).await.unwrap();

    let event =
        tokio::time::timeout(Duration::from_secs(1), system_events.recv())
            .await
            .expect("system event should arrive within timeout")
            .expect("system event channel should remain open");

    assert!(matches!(
        event,
        SystemEvent::ActorError {
            path,
            error: Error::Functional { description }
        } if path == ActorPath::from("/user/root-error")
            && description == "root emitted error"
    ));

    let response = actor_ref.ask(RootErrorMessage::Ping).await.unwrap();
    assert_eq!(response, RootErrorResponse::Ok);

    shutdown_system(handle).await;
}

#[tokio::test]
async fn test_parent_stop_uses_configured_shutdown_timeout_for_blocked_child() {
    build_tracing_subscriber();
    let handle = spawn_system();
    let child_entered = Arc::new(Notify::new());

    let parent_ref: ActorRef<ParentWithBlockingChild> = handle
        .system
        .create_root_actor(
            "blocked-child-parent",
            ParentWithBlockingChild {
                child_entered: child_entered.clone(),
            },
        )
        .await
        .unwrap();

    parent_ref.tell(StopControl::BlockChild).await.unwrap();
    child_entered.notified().await;

    tokio::time::timeout(Duration::from_secs(1), parent_ref.ask_stop())
        .await
        .expect("parent stop should not hang forever")
        .expect("parent stop should complete even if child is blocked");

    shutdown_system(handle).await;
}

#[tokio::test]
async fn test_system_shutdown_uses_root_stop_timeout_for_blocked_root() {
    build_tracing_subscriber();
    let mut handle = spawn_system();
    let root_entered = Arc::new(Notify::new());

    let root_ref: ActorRef<BlockingRootActor> = handle
        .system
        .create_root_actor(
            "blocked-root",
            BlockingRootActor {
                entered: root_entered.clone(),
            },
        )
        .await
        .unwrap();

    root_ref.tell(StopControl::BlockChild).await.unwrap();
    root_entered.notified().await;

    let shutdown =
        tokio::time::timeout(Duration::from_millis(50), &mut handle.runner)
            .await;
    assert!(
        shutdown.is_err(),
        "runner should still be alive before stop_system"
    );

    handle.system.stop_system();

    let shutdown =
        tokio::time::timeout(Duration::from_secs(1), &mut handle.runner)
            .await
            .expect("system shutdown should respect configured timeout")
            .expect("runner task should not panic");
    assert_eq!(shutdown, ShutdownReason::Graceful);
}
