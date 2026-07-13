//! Regression tests for FullPersistence recovery when events exist but no
//! snapshot was ever created.
//!
//! Guarantees that recover() falls back to replaying every event from index 0
//! (`recover_from_initial_events`), rebuilding the full state and restoring
//! the event counter, instead of starting from the initial state.

#[macro_use]
mod helpers;
use ave_actors_store::{
    memory::MemoryManager,
    store::{FullPersistence, PersistentActor, StoreCommand, StoreResponse},
};

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorSystem, Error as ActorError, Event, Handler,
    Message, Response,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(
    Debug, Clone, Default, borsh::BorshSerialize, borsh::BorshDeserialize,
)]
struct TestActorState {
    value: i32,
}

#[derive(Debug)]
struct TestActor {
    state_ptr: Arc<TestActorState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestMessage;
impl Message for TestMessage {}

#[derive(Debug, Clone)]
struct TestResponse;
impl Response for TestResponse {}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
struct TestEvent {
    delta: i32,
}
impl Event for TestEvent {}

#[async_trait]
impl Actor for TestActor {
    type Message = TestMessage;
    type Response = TestResponse;
    type Event = TestEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TestActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for TestActor {
    async fn handle_message(
        &mut self,
        _sender: ave_actors_actor::ActorPath,
        _msg: TestMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<TestResponse, ActorError> {
        Ok(TestResponse)
    }
}

#[async_trait]
impl PersistentActor for TestActor {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = TestActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state_ptr: Arc::new(TestActorState::default()),
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
async fn test_full_persistence_recovery_without_snapshot() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    let store = store_new!(
        TestActor,
        "test",
        "no_snapshot",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    for i in 1..=3 {
        let event = TestEvent { delta: i };
        store_ref
            .ask(StoreCommand::Persist(Arc::new(event)))
            .await
            .unwrap();
    }

    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        assert_eq!(count, 3);
    }

    drop(store_ref);

    let store2 = store_new!(
        TestActor,
        "test",
        "no_snapshot",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();

    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();

    match result {
        StoreResponse::State(Some(state)) => {
            // All three events are replayed: 1 + 2 + 3 = 6.
            assert_eq!(
                state.value, 6,
                "Should have replayed all 3 events (1+2+3=6)"
            );
        }
        StoreResponse::State(None) => {
            panic!(
                "BUG: recover() returned None when there are events in the DB"
            );
        }
        _ => panic!("Unexpected response type"),
    }
}

#[test(tokio::test)]
async fn test_recover_without_snapshot_replays_all_events() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    let store = store_new!(
        TestActor,
        "test",
        "logic_test",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Persist two events (5 and 7) without ever creating a snapshot.
    store_ref
        .ask(StoreCommand::Persist(Arc::new(TestEvent { delta: 5 })))
        .await
        .unwrap();
    store_ref
        .ask(StoreCommand::Persist(Arc::new(TestEvent { delta: 7 })))
        .await
        .unwrap();

    drop(store_ref);

    let store2 = store_new!(
        TestActor,
        "test",
        "logic_test",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();

    // With no snapshot, recover() replays both events from index 0 onto the
    // initial state, yielding 5 + 7 = 12.
    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(state)) => {
            assert_eq!(
                state.value, 12,
                "Should have replayed both events (5+7=12)"
            );
        }
        StoreResponse::State(None) => {
            panic!("recover() must replay events when no snapshot exists");
        }
        _ => panic!("Unexpected response type"),
    }

    // Two events were persisted, so the counter must be restored to 2.
    let result = store_ref2.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        assert_eq!(count, 2);
    }
}
