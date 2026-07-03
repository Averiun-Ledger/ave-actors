#![cfg(feature = "prometheus")]

//! Integration tests for actor Prometheus metrics.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error, Handler, Message,
    NotPersistentActor, OverflowStrategy,
};
use prometheus_client::registry::Registry;
use serde::{Deserialize, Serialize};
use test_log::test;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Ping;

impl Message for Ping {}

#[derive(Clone)]
struct PingActor;

impl NotPersistentActor for PingActor {}

#[async_trait]
impl Actor for PingActor {
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
        info_span!("PingActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for PingActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: Ping,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

async fn join_runner(
    handle: tokio::task::JoinHandle<ave_actors_actor::ShutdownReason>,
) -> Result<(), Error> {
    tokio::time::timeout(std::time::Duration::from_secs(5), handle)
        .await
        .map_err(|_| Error::Functional {
            description: "runner timed out".to_owned(),
        })?
        .map_err(|_| Error::Functional {
            description: "runner panicked".to_owned(),
        })?;
    Ok(())
}

fn encode_registry(registry: &Registry) -> String {
    let mut body = String::new();
    prometheus_client::encoding::text::encode(&mut body, registry)
        .expect("prometheus registry should encode to text");
    body
}

#[test(tokio::test)]
async fn actor_metrics_are_registered_and_emitted() -> Result<(), Error> {
    let mut registry = Registry::default();
    let (system, mut runner) = ActorSystem::create_with_registry(
        CancellationToken::new(),
        CancellationToken::new(),
        &mut registry,
    );
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor::<PingActor, _>("pinger", PingActor)
        .await?;
    actor.tell(Ping).await?;

    system.stop_system();
    join_runner(runner_handle).await?;

    let body = encode_registry(&registry);
    assert!(body.contains("ave_actors_actor_messages_processed_total"));
    assert!(body.contains("PingActor"));
    Ok(())
}

#[test(tokio::test)]
async fn actor_message_wait_seconds_metric_is_emitted() -> Result<(), Error> {
    let mut registry = Registry::default();
    let (system, mut runner) = ActorSystem::create_with_registry(
        CancellationToken::new(),
        CancellationToken::new(),
        &mut registry,
    );
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor::<PingActor, _>("waiter", PingActor)
        .await?;
    actor.tell(Ping).await?;

    system.stop_system();
    join_runner(runner_handle).await?;

    let body = encode_registry(&registry);
    assert!(
        body.contains("ave_actors_actor_message_wait_seconds"),
        "expected message wait metric in output: {body}"
    );
    Ok(())
}

#[derive(Debug, Clone)]
enum DropMsg {
    Block,
    Process,
}

impl Message for DropMsg {}

#[derive(Clone)]
struct DropActor {
    started: std::sync::Arc<Notify>,
    release: std::sync::Arc<Notify>,
}

impl NotPersistentActor for DropActor {}

#[async_trait]
impl Actor for DropActor {
    type Message = DropMsg;
    type Response = ();
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
        info_span!("DropActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for DropActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: DropMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        if matches!(msg, DropMsg::Block) {
            self.started.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
}

#[test(tokio::test)]
async fn mailbox_drop_metric_is_emitted() -> Result<(), Error> {
    let mut registry = Registry::default();
    let (system, mut runner) = ActorSystem::create_with_registry(
        CancellationToken::new(),
        CancellationToken::new(),
        &mut registry,
    );
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let started = std::sync::Arc::new(Notify::new());
    let release = std::sync::Arc::new(Notify::new());
    let actor = system
        .create_root_actor::<DropActor, _>(
            "dropper",
            DropActor {
                started: started.clone(),
                release: release.clone(),
            },
        )
        .await?;

    // Block the actor and wait until it has entered the handler.
    actor.tell(DropMsg::Block).await?;
    started.notified().await;

    // Flood the actor while it is blocked. With a capacity of one, only one
    // Process message can queue and the rest are dropped.
    for _ in 0..50 {
        let _ = actor.tell(DropMsg::Process).await;
    }

    // Release the actor so shutdown can complete.
    release.notify_one();

    system.stop_system();
    join_runner(runner_handle).await?;

    let body = encode_registry(&registry);
    assert!(
        body.contains("ave_actors_actor_mailbox_dropped_total"),
        "expected mailbox drop metric in output: {body}"
    );
    Ok(())
}

#[derive(Debug, Clone)]
enum FullMsg {
    Block,
    Process,
}

impl Message for FullMsg {}

#[derive(Clone)]
struct FullActor {
    started: std::sync::Arc<Notify>,
    release: std::sync::Arc<Notify>,
}

impl NotPersistentActor for FullActor {}

#[async_trait]
impl Actor for FullActor {
    type Message = FullMsg;
    type Response = ();
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
        info_span!("FullActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for FullActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: FullMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        if matches!(msg, FullMsg::Block) {
            self.started.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
}

#[test(tokio::test)]
async fn mailbox_full_metric_is_emitted() -> Result<(), Error> {
    let mut registry = Registry::default();
    let (system, mut runner) = ActorSystem::create_with_registry(
        CancellationToken::new(),
        CancellationToken::new(),
        &mut registry,
    );
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let started = std::sync::Arc::new(Notify::new());
    let release = std::sync::Arc::new(Notify::new());
    let actor = system
        .create_root_actor::<FullActor, _>(
            "full",
            FullActor {
                started: started.clone(),
                release: release.clone(),
            },
        )
        .await?;

    // Block the actor and wait until it has entered the handler.
    actor.tell(FullMsg::Block).await?;
    started.notified().await;

    // Flood the actor while it is blocked. With a capacity of one and the
    // Fail strategy, the first message queues and the rest return
    // MailboxFull, incrementing the mailbox-full counter.
    for _ in 0..50 {
        let _ = actor.tell(FullMsg::Process).await;
    }

    // Release the actor so shutdown can complete.
    release.notify_one();

    system.stop_system();
    join_runner(runner_handle).await?;

    let body = encode_registry(&registry);
    assert!(
        body.contains("ave_actors_actor_mailbox_full_total"),
        "expected mailbox full metric in output: {body}"
    );
    Ok(())
}
