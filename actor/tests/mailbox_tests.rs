//! Tests for bounded mailboxes and overflow strategies.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error, Handler, Message,
    NotPersistentActor, OverflowStrategy, Response,
};
use test_log::test;
use tokio::sync::{Mutex, Notify};
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone)]
enum BoundMsg {
    /// Notifies `started`, then blocks until `release` is notified.
    Block,
    /// Increments the processed counter.
    Process,
    /// Returns the current processed counter.
    GetCount,
}

impl Message for BoundMsg {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CountResponse(usize);

impl Response for CountResponse {}

#[derive(Clone)]
struct BackpressureActor {
    count: Arc<Mutex<usize>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl NotPersistentActor for BackpressureActor {}

#[async_trait]
impl Actor for BackpressureActor {
    type Message = BoundMsg;
    type Response = CountResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn mailbox_capacity() -> usize {
        1
    }

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("BackpressureActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for BackpressureActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: BoundMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<CountResponse, Error> {
        match msg {
            BoundMsg::Block => {
                self.started.notify_one();
                self.release.notified().await;
            }
            BoundMsg::Process => {
                *self.count.lock().await += 1;
            }
            BoundMsg::GetCount => {
                return Ok(CountResponse(*self.count.lock().await));
            }
        }
        Ok(CountResponse(0))
    }
}

#[derive(Clone)]
struct DropNewestActor {
    count: Arc<Mutex<usize>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl NotPersistentActor for DropNewestActor {}

#[async_trait]
impl Actor for DropNewestActor {
    type Message = BoundMsg;
    type Response = CountResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn mailbox_capacity() -> usize {
        1
    }

    fn mailbox_overflow_strategy() -> OverflowStrategy {
        OverflowStrategy::DropNewest
    }

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("DropNewestActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for DropNewestActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: BoundMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<CountResponse, Error> {
        match msg {
            BoundMsg::Block => {
                self.started.notify_one();
                self.release.notified().await;
            }
            BoundMsg::Process => {
                *self.count.lock().await += 1;
            }
            BoundMsg::GetCount => {
                return Ok(CountResponse(*self.count.lock().await));
            }
        }
        Ok(CountResponse(0))
    }
}

#[derive(Clone)]
struct FailActor {
    count: Arc<Mutex<usize>>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

impl NotPersistentActor for FailActor {}

#[async_trait]
impl Actor for FailActor {
    type Message = BoundMsg;
    type Response = CountResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn mailbox_capacity() -> usize {
        1
    }

    fn mailbox_overflow_strategy() -> OverflowStrategy {
        OverflowStrategy::Fail
    }

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("FailActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for FailActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: BoundMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<CountResponse, Error> {
        match msg {
            BoundMsg::Block => {
                self.started.notify_one();
                self.release.notified().await;
            }
            BoundMsg::Process => {
                *self.count.lock().await += 1;
            }
            BoundMsg::GetCount => {
                return Ok(CountResponse(*self.count.lock().await));
            }
        }
        Ok(CountResponse(0))
    }
}

#[test(tokio::test)]
async fn test_backpressure_blocks_when_full() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let actor = BackpressureActor {
        count: Arc::new(Mutex::new(0)),
        started: started.clone(),
        release: release.clone(),
    };
    let actor_ref = system.create_root_actor("backpressure", actor).await?;

    // Block the actor and wait until it has entered the handler.
    actor_ref.tell(BoundMsg::Block).await?;
    started.notified().await;

    // Fill the single mailbox slot.
    actor_ref.tell(BoundMsg::Process).await?;

    // The next tell should block because the mailbox is full.
    let blocked = tokio::time::timeout(
        Duration::from_millis(50),
        actor_ref.tell(BoundMsg::Process),
    )
    .await;
    assert!(
        blocked.is_err(),
        "tell should have blocked on a full mailbox"
    );

    // Release the actor; it will process Block and the queued Process.
    release.notify_one();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let count = actor_ref.ask(BoundMsg::GetCount).await?;
        if count.0 == 1 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "backpressure did not preserve messages"
                    .to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    join_runner(runner_handle).await
}

async fn join_runner(
    handle: tokio::task::JoinHandle<ave_actors_actor::ShutdownReason>,
) -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .map_err(|_| Error::Functional {
            description: "runner timed out".to_owned(),
        })?
        .map_err(|_| Error::Functional {
            description: "runner panicked".to_owned(),
        })?;
    Ok(())
}

#[test(tokio::test)]
async fn test_drop_newest_discards_when_full() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let actor = DropNewestActor {
        count: Arc::new(Mutex::new(0)),
        started: started.clone(),
        release: release.clone(),
    };
    let actor_ref = system.create_root_actor("drop_newest", actor).await?;

    // Block the actor and wait until it has entered the handler.
    actor_ref.tell(BoundMsg::Block).await?;
    started.notified().await;

    // This Process fills the mailbox.
    actor_ref.tell(BoundMsg::Process).await?;
    // These two should be dropped because the mailbox is full.
    actor_ref.tell(BoundMsg::Process).await?;
    actor_ref.tell(BoundMsg::Process).await?;

    release.notify_one();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let count = actor_ref.ask(BoundMsg::GetCount).await?;
        if count.0 == 1 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "DropNewest did not discard surplus messages"
                    .to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_fail_returns_mailbox_full() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let actor = FailActor {
        count: Arc::new(Mutex::new(0)),
        started: started.clone(),
        release: release.clone(),
    };
    let actor_ref = system.create_root_actor("fail", actor).await?;

    // Block the actor and wait until it has entered the handler.
    actor_ref.tell(BoundMsg::Block).await?;
    started.notified().await;

    // This Process fills the mailbox.
    actor_ref.tell(BoundMsg::Process).await?;

    // The next tell must fail immediately with MailboxFull.
    let result = actor_ref.tell(BoundMsg::Process).await;
    assert_eq!(result, Err(Error::MailboxFull));

    // ask should also fail with MailboxFull instead of blocking.
    let ask_result = actor_ref.ask(BoundMsg::GetCount).await;
    assert_eq!(ask_result, Err(Error::MailboxFull));

    release.notify_one();

    system.stop_system();
    join_runner(runner_handle).await
}

// ============================================================================
// Mailbox capacity validation tests
// ============================================================================

#[derive(Clone)]
struct CapacityActor;

impl NotPersistentActor for CapacityActor {}

#[async_trait]
impl Actor for CapacityActor {
    type Message = BoundMsg;
    type Response = CountResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("CapacityActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for CapacityActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: BoundMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<CountResponse, Error> {
        Ok(CountResponse(0))
    }
}

struct ZeroCapacityActor;

impl NotPersistentActor for ZeroCapacityActor {}

#[async_trait]
impl Actor for ZeroCapacityActor {
    type Message = BoundMsg;
    type Response = CountResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn mailbox_capacity() -> usize {
        0
    }

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ZeroCapacityActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for ZeroCapacityActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: BoundMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<CountResponse, Error> {
        Ok(CountResponse(0))
    }
}

struct TooLargeCapacityActor;

impl NotPersistentActor for TooLargeCapacityActor {}

#[async_trait]
impl Actor for TooLargeCapacityActor {
    type Message = BoundMsg;
    type Response = CountResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn mailbox_capacity() -> usize {
        1_000_001
    }

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TooLargeCapacityActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for TooLargeCapacityActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: BoundMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<CountResponse, Error> {
        Ok(CountResponse(0))
    }
}

#[test(tokio::test)]
async fn test_mailbox_capacity_zero_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let result = system
        .create_root_actor::<ZeroCapacityActor, _>(
            "zero_capacity",
            ZeroCapacityActor,
        )
        .await;

    assert!(
        matches!(result, Err(Error::InvalidConfiguration { .. })),
        "expected InvalidConfiguration for mailbox capacity 0, got {:?}",
        result
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_mailbox_capacity_above_max_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let result = system
        .create_root_actor::<TooLargeCapacityActor, _>(
            "too_large_capacity",
            TooLargeCapacityActor,
        )
        .await;

    assert!(
        matches!(result, Err(Error::InvalidConfiguration { .. })),
        "expected InvalidConfiguration for mailbox capacity above max, got {:?}",
        result
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_mailbox_capacity_one_is_accepted() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor::<CapacityActor, _>("valid_capacity", CapacityActor)
        .await?;

    let response = actor.ask(BoundMsg::GetCount).await?;
    assert_eq!(response.0, 0);

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_mailbox_capacity_at_max_is_accepted() -> Result<(), Error> {
    struct MaxCapacityActor;

    impl NotPersistentActor for MaxCapacityActor {}

    #[async_trait]
    impl Actor for MaxCapacityActor {
        type Message = BoundMsg;
        type Response = CountResponse;
        type Event = ();
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn mailbox_capacity() -> usize {
            1_000_000
        }

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("MaxCapacityActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for MaxCapacityActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: BoundMsg,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<CountResponse, Error> {
            Ok(CountResponse(0))
        }
    }

    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor::<MaxCapacityActor, _>(
            "max_capacity",
            MaxCapacityActor,
        )
        .await?;

    let response = actor.ask(BoundMsg::GetCount).await?;
    assert_eq!(response.0, 0);

    system.stop_system();
    join_runner(runner_handle).await
}
