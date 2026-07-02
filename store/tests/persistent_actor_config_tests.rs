//! Tests for PersistentActor configuration validation.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorSystem, Error as ActorError, Event, Handler,
    Message, Response,
};
use ave_actors_store::{
    memory::MemoryManager,
    store::{FullPersistence, PersistentActor},
};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct DummyState;

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct DummyEvent;
impl Event for DummyEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DummyMessage;
impl Message for DummyMessage {}

#[derive(Debug, Clone, PartialEq)]
struct DummyResponse;
impl Response for DummyResponse {}

#[derive(Debug)]
struct ZeroSnapshotActor;

#[async_trait]
impl Actor for ZeroSnapshotActor {
    type Message = DummyMessage;
    type Response = DummyResponse;
    type Event = DummyEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ZeroSnapshotActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        self.start_store(
            "zero_snapshot",
            None,
            ctx,
            MemoryManager::default(),
            None,
        )
        .await
    }
}

#[async_trait]
impl PersistentActor for ZeroSnapshotActor {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = DummyState;

    fn create_initial(_: ()) -> Self {
        Self
    }

    fn apply(
        _state: Arc<Self::State>,
        _event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        Ok(Arc::new(DummyState))
    }

    fn state(&self) -> Arc<Self::State> {
        Arc::new(DummyState)
    }

    fn set_state(&mut self, _state: Arc<Self::State>) {}

    fn snapshot_every() -> Option<u64> {
        Some(0)
    }
}

#[async_trait]
impl Handler<Self> for ZeroSnapshotActor {
    async fn handle_message(
        &mut self,
        _sender: ave_actors_actor::ActorPath,
        _msg: DummyMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<DummyResponse, ActorError> {
        Ok(DummyResponse)
    }
}

#[test(tokio::test)]
async fn test_snapshot_every_zero_is_rejected() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor = ZeroSnapshotActor::initial(());
    let result = system.create_root_actor("zero_snapshot", actor).await;

    assert!(
        matches!(result, Err(ActorError::InvalidConfiguration { .. })),
        "expected InvalidConfiguration for snapshot_every Some(0), got {:?}",
        result
    );
}
