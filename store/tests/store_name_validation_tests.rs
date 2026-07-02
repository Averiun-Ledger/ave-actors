//! Tests for store/collection name validation in `Store::new`.

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
struct NamedStoreActor {
    name: &'static str,
    prefix: Option<&'static str>,
}

#[async_trait]
impl Actor for NamedStoreActor {
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
        info_span!("NamedStoreActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        self.start_store(
            self.name,
            self.prefix,
            ctx,
            MemoryManager::default(),
            None,
        )
        .await
    }
}

#[async_trait]
impl PersistentActor for NamedStoreActor {
    type Persistence = FullPersistence;
    type InitParams = (&'static str, Option<&'static str>);
    type State = DummyState;

    fn create_initial(params: Self::InitParams) -> Self {
        Self {
            name: params.0,
            prefix: params.1,
        }
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
}

#[async_trait]
impl Handler<Self> for NamedStoreActor {
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
async fn test_store_name_empty_is_rejected() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor = NamedStoreActor::initial(("", None));
    let result = system.create_root_actor("empty_name", actor).await;

    assert!(
        matches!(result, Err(ActorError::InvalidConfiguration { .. })),
        "expected InvalidConfiguration for empty store name, got {:?}",
        result
    );
}

#[test(tokio::test)]
async fn test_store_name_with_invalid_chars_is_rejected() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor = NamedStoreActor::initial(("my store!", None));
    let result = system.create_root_actor("invalid_name", actor).await;

    assert!(
        matches!(result, Err(ActorError::InvalidConfiguration { .. })),
        "expected InvalidConfiguration for invalid store name, got {:?}",
        result
    );
}

#[test(tokio::test)]
async fn test_store_prefix_empty_is_rejected() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor = NamedStoreActor::initial(("valid_store", Some("")));
    let result = system.create_root_actor("empty_prefix", actor).await;

    assert!(
        matches!(result, Err(ActorError::InvalidConfiguration { .. })),
        "expected InvalidConfiguration for empty prefix, got {:?}",
        result
    );
}

#[test(tokio::test)]
async fn test_store_name_and_prefix_valid_succeeds() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor = NamedStoreActor::initial(("valid_store", Some("valid_prefix")));
    let result = system.create_root_actor("valid_name", actor).await;

    assert!(
        result.is_ok(),
        "expected actor creation to succeed for valid name/prefix, got {:?}",
        result
    );
}
