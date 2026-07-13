//! Regression test for `LightPersistence` counters.
//!
//! `LightPersistence` writes only a snapshot (no event log). This test pins
//! the invariant that `event_counter == state_counter` after a light persist
//! and that recovery restores the snapshot exactly once (no double apply).

#[macro_use]
mod helpers;
use ave_actors_store::{
    memory::MemoryManager,
    store::{LightPersistence, PersistentActor, StoreCommand, StoreResponse},
};
use test_log::test;

use ave_actors_actor::{
    Actor, ActorContext, ActorSystem, Error as ActorError, Event, Handler,
    Message, Response,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(
    Debug, Clone, Default, borsh::BorshSerialize, borsh::BorshDeserialize,
)]
struct DebugActorState {
    value: i32,
}

#[derive(Debug)]
struct DebugActor {
    state_ptr: Arc<DebugActorState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum DebugMessage {
    Add(i32),
    GetValue,
}

impl Message for DebugMessage {}

#[derive(Debug, Clone, PartialEq)]
enum DebugResponse {
    Success,
    Value(i32),
}

impl Response for DebugResponse {}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
struct DebugEvent {
    delta: i32,
}

impl Event for DebugEvent {}

#[async_trait]
impl Actor for DebugActor {
    type Message = DebugMessage;
    type Response = DebugResponse;
    type Event = DebugEvent;
    type SinkEvent = Self::Event;

    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("DebugActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for DebugActor {
    async fn handle_message(
        &mut self,
        _sender: ave_actors_actor::ActorPath,
        msg: DebugMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<DebugResponse, ActorError> {
        match msg {
            DebugMessage::Add(_) => Ok(DebugResponse::Success),
            DebugMessage::GetValue => {
                Ok(DebugResponse::Value(self.state_ptr.value))
            }
        }
    }
}

#[async_trait]
impl PersistentActor for DebugActor {
    type Persistence = LightPersistence;
    type InitParams = ();
    type State = DebugActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state_ptr: Arc::new(DebugActorState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut new_state = state;
        Arc::make_mut(&mut new_state).value += event.delta;
        Ok(new_state)
    }

    fn state(&self) -> Arc<Self::State> {
        Arc::clone(&self.state_ptr)
    }

    fn set_state(&mut self, state: Arc<Self::State>) {
        self.state_ptr = state;
    }
}

#[test(tokio::test)]
async fn test_light_persistence_counters() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    let store = store_new!(
        DebugActor,
        "debug_test",
        "test_light",
        memory_manager.clone(),
        None,
        Arc::new(DebugActorState::default()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // A fresh store starts with no events.
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => assert_eq!(count, 0),
        _ => panic!("Expected LastEventNumber response"),
    }

    // One light persist advances both counters together (snapshot only).
    let state = Arc::new(DebugActorState { value: 10 });
    let result = store_ref
        .ask(StoreCommand::PersistLight(state))
        .await
        .unwrap();
    match result {
        StoreResponse::Persisted => {}
        _ => panic!("Expected Persisted response"),
    }

    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => assert_eq!(count, 1),
        _ => panic!("Expected LastEventNumber response"),
    }

    // Recovery must restore the snapshot exactly once (no double apply).
    drop(store_ref);

    let store2 = store_new!(
        DebugActor,
        "debug_test",
        "test_light",
        memory_manager.clone(),
        None,
        Arc::new(DebugActorState::default()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();

    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(state)) => assert_eq!(state.value, 10),
        _ => panic!("Expected State response with Some"),
    }

    // After recovery the counter reflects the persisted snapshot, not zero.
    let result = store_ref2.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => assert_eq!(count, 1),
        _ => panic!("Expected LastEventNumber response"),
    }
}
