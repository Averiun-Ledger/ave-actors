#![cfg(feature = "prometheus")]

//! Integration tests for store Prometheus metrics.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error as ActorError, Event,
    Handler, Message, Response,
};
use ave_actors_store::{
    memory::MemoryManager,
    metrics::{STORE_METRICS_HELPER, StoreMetrics},
    store::{LightPersistence, PersistentActor},
};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct CounterState {
    value: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CounterMessage {
    Add(i32),
    Snapshot,
}

impl Message for CounterMessage {}

#[derive(Debug, Clone, PartialEq)]
enum CounterResponse {
    Success,
}

impl Response for CounterResponse {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct CounterEvent {
    delta: i32,
}

impl Event for CounterEvent {}

#[derive(Debug)]
struct CounterActor {
    state: Arc<CounterState>,
}

#[async_trait]
impl Actor for CounterActor {
    type Message = CounterMessage;
    type Response = CounterResponse;
    type Event = CounterEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("CounterActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let db: MemoryManager = ctx
            .system()
            .get_helper("db")
            .expect("db helper should be installed");
        self.start_store("store", None, ctx, db, None).await
    }
}

#[async_trait]
impl PersistentActor for CounterActor {
    type Persistence = LightPersistence;
    type InitParams = ();
    type State = CounterState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(CounterState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut state = state;
        Arc::make_mut(&mut state).value += event.delta;
        Ok(state)
    }

    fn state(&self) -> Arc<Self::State> {
        Arc::clone(&self.state)
    }

    fn set_state(&mut self, state: Arc<Self::State>) {
        self.state = state;
    }
}

#[async_trait]
impl Handler<Self> for CounterActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: CounterMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<CounterResponse, ActorError> {
        match msg {
            CounterMessage::Add(v) => {
                self.persist(CounterEvent { delta: v }, ctx).await?;
                Ok(CounterResponse::Success)
            }
            CounterMessage::Snapshot => {
                self.snapshot(ctx).await?;
                Ok(CounterResponse::Success)
            }
        }
    }
}

async fn join_runner(
    handle: tokio::task::JoinHandle<ave_actors_actor::ShutdownReason>,
) -> Result<(), ActorError> {
    tokio::time::timeout(std::time::Duration::from_secs(5), handle)
        .await
        .map_err(|_| ActorError::Functional {
            description: "runner timed out".to_owned(),
        })?
        .map_err(|_| ActorError::Functional {
            description: "runner panicked".to_owned(),
        })?;
    Ok(())
}

#[test(tokio::test)]
async fn store_metrics_are_registered_and_emitted() -> Result<(), ActorError> {
    let mut registry = prometheus_client::registry::Registry::default();
    let (system, mut runner) = ActorSystem::create_with_registry(
        CancellationToken::new(),
        CancellationToken::new(),
        &mut registry,
    );
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let store_metrics = StoreMetrics::new();
    store_metrics.register_into(&mut registry);
    system.add_helper(STORE_METRICS_HELPER, Arc::new(store_metrics));

    system.add_helper("db", MemoryManager::default());

    let actor = system
        .create_root_actor("counter", CounterActor::initial(()))
        .await?;

    // Persist a few events; LightPersistence snapshots on every persist.
    for i in 1..=3 {
        actor.ask(CounterMessage::Add(i * 10)).await?;
    }

    // Request an explicit snapshot to exercise the snapshot metric.
    actor.ask(CounterMessage::Snapshot).await?;

    system.stop_system();
    join_runner(runner_handle).await?;

    let mut body = String::new();
    prometheus_client::encoding::text::encode(&mut body, &registry)
        .expect("prometheus registry should encode to text");
    assert!(
        body.contains("ave_actors_store_operation_duration_seconds"),
        "expected operation_duration metric in output: {body}"
    );

    Ok(())
}
