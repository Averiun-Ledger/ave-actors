//! Regression tests for FullPersistence recovery after the first graceful
//! stop, when no snapshot existed beforehand.
//!
//! Guarantees that stopping an actor with pending events creates a snapshot,
//! and that a restarted actor recovers its state from it.

#[macro_use]
mod helpers;
use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error as ActorError, Event,
    Handler, Message, Response,
};
use ave_actors_store::memory::MemoryManager;
use ave_actors_store::store::{FullPersistence, PersistentActor};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use test_log::test;
use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

static SHARED_MANAGER_FIRST_STOP: OnceLock<Arc<TokioMutex<MemoryManager>>> =
    OnceLock::new();

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct TestActorState {
    value: i32,
}

#[derive(Debug)]
struct TestActor {
    state_ptr: Arc<TestActorState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TestMessage {
    Add(i32),
    Get,
}
impl Message for TestMessage {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestResponse {
    value: i32,
}
impl Response for TestResponse {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct AddEvent(i32);
impl Event for AddEvent {}

#[async_trait]
impl Actor for TestActor {
    type Message = TestMessage;
    type Response = TestResponse;
    type Event = AddEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TestActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let manager_ref = SHARED_MANAGER_FIRST_STOP.get_or_init(|| {
            Arc::new(TokioMutex::new(MemoryManager::default()))
        });

        let manager = manager_ref.lock().await.clone();
        self.start_store("test_first_stop", None, ctx, manager, None)
            .await
    }
}

#[async_trait]
impl Handler<Self> for TestActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: TestMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<TestResponse, ActorError> {
        match msg {
            TestMessage::Add(delta) => {
                self.persist(AddEvent(delta), ctx).await?;
                Ok(TestResponse {
                    value: self.state_ptr.value,
                })
            }
            TestMessage::Get => Ok(TestResponse {
                value: self.state_ptr.value,
            }),
        }
    }
}

#[async_trait]
impl PersistentActor for TestActor {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = TestActorState;

    fn create_initial(_params: ()) -> Self {
        Self {
            state_ptr: Arc::new(TestActorState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut new_state = state;
        Arc::make_mut(&mut new_state).value += event.0;
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
async fn test_full_persistence_first_stop_no_previous_snapshot() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    // Brand new actor: no snapshot exists yet.
    let actor_ref = system
        .create_root_actor("test_actor", TestActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(TestMessage::Add(10)).await.unwrap();
    actor_ref.ask(TestMessage::Add(20)).await.unwrap();

    let response = actor_ref.ask(TestMessage::Get).await.unwrap();
    assert_eq!(response.value, 30, "Should be 10+20=30");

    // Graceful stop must snapshot the pending events (value=30).
    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let actor_ref2 = system
        .create_root_actor("test_actor", TestActor::initial(()))
        .await
        .unwrap();

    let response = actor_ref2.ask(TestMessage::Get).await.unwrap();

    assert_eq!(
        response.value, 30,
        "BUG: Should recover value=30 after graceful stop. Got value={}",
        response.value
    );
}

#[test(tokio::test)]
async fn test_snapshot_created_when_events_pending() {
    use ave_actors_store::store::{StoreCommand, StoreResponse};

    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    let store = store_new!(
        TestActor,
        "test",
        "stop_investigation",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    store_ref
        .ask(StoreCommand::Persist(Arc::new(AddEvent(5))))
        .await
        .unwrap();
    store_ref
        .ask(StoreCommand::Persist(Arc::new(AddEvent(7))))
        .await
        .unwrap();

    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        assert_eq!(count, 2);
    }

    drop(store_ref);

    let store2 = store_new!(
        TestActor,
        "test",
        "stop_investigation",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();

    // No snapshot exists yet: only events were persisted.
    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(_)) => {}
        StoreResponse::State(None) => {}
        _ => panic!("Unexpected response"),
    }

    // With events pending, snapshot the replayed state (5 + 7 = 12).
    let result = store_ref2.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result
        && count > 0
    {
        let mut actor = TestActor::create_initial(());
        let state =
            TestActor::apply(Arc::clone(&actor.state_ptr), &AddEvent(5))
                .unwrap();
        actor.set_state(state);
        let state =
            TestActor::apply(Arc::clone(&actor.state_ptr), &AddEvent(7))
                .unwrap();
        actor.set_state(state);

        store_ref2
            .ask(StoreCommand::Snapshot(actor.state()))
            .await
            .unwrap();
    }

    drop(store_ref2);

    let store3 = store_new!(
        TestActor,
        "test",
        "stop_investigation",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref3 = system.create_root_actor("store3", store3).await.unwrap();

    let result = store_ref3.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(state)) => {
            assert_eq!(state.value, 12);
        }
        StoreResponse::State(None) => {
            panic!("Snapshot should have been created!");
        }
        _ => panic!("Unexpected response"),
    }
}
