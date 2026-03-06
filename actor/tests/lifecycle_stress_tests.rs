use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, Error, Event,
    FixedIntervalStrategy, Handler, Message, NotPersistentActor, Response,
    ShutdownReason, Strategy, SupervisionStrategy, build_tracing_subscriber,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Barrier;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Default)]
struct LifecycleHooks {
    pre_start_calls: AtomicUsize,
    pre_restart_calls: AtomicUsize,
    post_stop_calls: AtomicUsize,
    fail_messages: AtomicUsize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StressMessage {
    Ping,
    Fail,
}

impl Message for StressMessage {}

#[derive(Debug, Clone, PartialEq)]
enum StressResponse {
    Ack,
}

impl Response for StressResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StressEvent;

impl Event for StressEvent {}

#[derive(Clone)]
struct StressActor {
    hooks: Arc<LifecycleHooks>,
    startup_failures_left: Arc<AtomicUsize>,
}

impl StressActor {
    fn new(hooks: Arc<LifecycleHooks>, startup_failures: usize) -> Self {
        Self {
            hooks,
            startup_failures_left: Arc::new(AtomicUsize::new(startup_failures)),
        }
    }

    fn consume_startup_failure(&self) -> bool {
        let remaining = self.startup_failures_left.load(Ordering::SeqCst);
        if remaining > 0 {
            self.startup_failures_left.fetch_sub(1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }
}

impl NotPersistentActor for StressActor {}

#[async_trait]
impl Actor for StressActor {
    type Message = StressMessage;
    type Response = StressResponse;
    type Event = StressEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("StressActor", id = %id)
    }

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Retry(Strategy::FixedInterval(
            FixedIntervalStrategy::new(128, Duration::from_millis(1)),
        ))
    }

    async fn pre_start(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        self.hooks.pre_start_calls.fetch_add(1, Ordering::SeqCst);
        if self.consume_startup_failure() {
            return Err(Error::FunctionalCritical {
                description: "forced pre_start failure".to_owned(),
            });
        }
        Ok(())
    }

    async fn pre_restart(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _error: Option<&Error>,
    ) -> Result<(), Error> {
        self.hooks.pre_restart_calls.fetch_add(1, Ordering::SeqCst);
        if self.consume_startup_failure() {
            return Err(Error::FunctionalCritical {
                description: "forced pre_restart failure".to_owned(),
            });
        }
        Ok(())
    }

    async fn post_stop(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        self.hooks.post_stop_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl Handler<StressActor> for StressActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: StressMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<StressResponse, Error> {
        match msg {
            StressMessage::Ping => Ok(StressResponse::Ack),
            StressMessage::Fail => {
                self.hooks.fail_messages.fetch_add(1, Ordering::SeqCst);
                ctx.emit_fail(Error::FunctionalCritical {
                    description: "forced runtime failure".to_owned(),
                })
                .await?;
                Ok(StressResponse::Ack)
            }
        }
    }
}

#[tokio::test]
async fn test_stress_concurrent_create_same_root_path() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    const ATTEMPTS: usize = 64;
    let barrier = Arc::new(Barrier::new(ATTEMPTS));
    let mut joins = Vec::with_capacity(ATTEMPTS);

    for _ in 0..ATTEMPTS {
        let system = system.clone();
        let barrier = barrier.clone();
        joins.push(tokio::spawn(async move {
            barrier.wait().await;
            let hooks = Arc::new(LifecycleHooks::default());
            system
                .create_root_actor("same-path", StressActor::new(hooks, 0))
                .await
        }));
    }

    let mut success_count = 0usize;
    let mut exists_count = 0usize;
    let mut created_ref: Option<ActorRef<StressActor>> = None;

    for join in joins {
        match join.await.unwrap() {
            Ok(actor_ref) => {
                success_count += 1;
                created_ref = Some(actor_ref);
            }
            Err(Error::Exists { .. }) => {
                exists_count += 1;
            }
            Err(error) => panic!("unexpected creation error: {error:?}"),
        }
    }

    assert_eq!(success_count, 1);
    assert_eq!(exists_count, ATTEMPTS - 1);

    if let Some(actor_ref) = created_ref {
        actor_ref.ask_stop().await.unwrap();
    }

    system.stop_system();
    let shutdown = tokio::time::timeout(Duration::from_secs(5), runner_handle)
        .await
        .expect("runner should finish within timeout")
        .expect("runner task should not panic");
    assert_eq!(shutdown, ShutdownReason::Graceful);
}

#[tokio::test]
async fn test_stress_concurrent_stop_requests() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let hooks = Arc::new(LifecycleHooks::default());
    let actor_ref = system
        .create_root_actor("stop-stress", StressActor::new(hooks.clone(), 0))
        .await
        .unwrap();

    const STOP_CALLS: usize = 48;
    let barrier = Arc::new(Barrier::new(STOP_CALLS));
    let mut joins = Vec::with_capacity(STOP_CALLS);

    for _ in 0..STOP_CALLS {
        let actor_ref = actor_ref.clone();
        let barrier = barrier.clone();
        joins.push(tokio::spawn(async move {
            barrier.wait().await;
            actor_ref.ask_stop().await
        }));
    }

    for join in joins {
        let result = join.await.unwrap();
        assert!(
            matches!(result, Ok(()) | Err(Error::Send { .. }) | Err(Error::ActorStopped)),
            "unexpected concurrent ask_stop result: {result:?}"
        );
    }

    assert_eq!(hooks.post_stop_calls.load(Ordering::SeqCst), 1);

    let path = ActorPath::from("/user/stop-stress");
    assert!(system.get_actor::<StressActor>(&path).await.is_err());
    assert_eq!(actor_ref.ask(StressMessage::Ping).await, Err(Error::ActorStopped));

    system.stop_system();
    let shutdown = tokio::time::timeout(Duration::from_secs(5), runner_handle)
        .await
        .expect("runner should finish within timeout")
        .expect("runner task should not panic");
    assert_eq!(shutdown, ShutdownReason::Graceful);
}

#[tokio::test]
async fn test_stress_concurrent_fail_terminates_after_startup_retries() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let hooks = Arc::new(LifecycleHooks::default());
    let actor_ref = system
        .create_root_actor("fail-stress", StressActor::new(hooks.clone(), 2))
        .await
        .unwrap();

    assert_eq!(hooks.pre_start_calls.load(Ordering::SeqCst), 1);
    assert_eq!(hooks.pre_restart_calls.load(Ordering::SeqCst), 2);

    const FAIL_BURST: usize = 64;
    let barrier = Arc::new(Barrier::new(FAIL_BURST));
    let mut joins = Vec::with_capacity(FAIL_BURST);

    for _ in 0..FAIL_BURST {
        let actor_ref = actor_ref.clone();
        let barrier = barrier.clone();
        joins.push(tokio::spawn(async move {
            barrier.wait().await;
            actor_ref.tell(StressMessage::Fail).await
        }));
    }

    for join in joins {
        let _ = join.await.unwrap();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if system
            .get_actor::<StressActor>(&ActorPath::from("/user/fail-stress"))
            .await
            .is_err()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "actor did not terminate after fail burst"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert!(hooks.pre_restart_calls.load(Ordering::SeqCst) >= 2);
    assert!(hooks.fail_messages.load(Ordering::SeqCst) > 0);
    assert_eq!(actor_ref.ask(StressMessage::Ping).await, Err(Error::ActorStopped));

    runner_handle.abort();
    let _ = runner_handle.await;
}
