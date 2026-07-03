//! Investigate if LastEventNumber might be returning 0 incorrectly

#[macro_use]
mod helpers;
use ave_actors_store::{
    memory::MemoryManager,
    store::{FullPersistence, PersistentActor, StoreCommand, StoreResponse},
};

use ave_actors_actor::{
    Actor, ActorContext, ActorSystem, Error as ActorError, Event, Handler,
    Message, Response,
};
use test_log::test;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// State struct
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
struct TestEvent(i32);
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
async fn test_last_event_number_after_persist() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    println!(
        "\n╔════════════════════════════════════════════════════════════╗"
    );
    println!("║  Investigation: LastEventNumber Behavior                  ║");
    println!(
        "╚════════════════════════════════════════════════════════════╝\n"
    );

    let store = store_new!(
        TestActor,
        "test",
        "last_event",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    println!("📊 STEP 1: Initial state");
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   LastEventNumber = {}", count);
        assert_eq!(count, 0);
    }

    println!("\n📊 STEP 2: After persisting 1 event");
    store_ref
        .ask(StoreCommand::Persist(Arc::new(TestEvent(10))))
        .await
        .unwrap();

    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   LastEventNumber = {}", count);
        println!("   Expected: 1");

        if count == 0 {
            println!("   ❌ PROBLEM: Still 0!");
            println!("   ❌ This would cause stop_store() to skip snapshot!");
        } else {
            println!("   ✅ Correct");
        }

        assert_eq!(count, 1);
    }

    println!("\n📊 STEP 3: After persisting 2nd event");
    store_ref
        .ask(StoreCommand::Persist(Arc::new(TestEvent(20))))
        .await
        .unwrap();

    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   LastEventNumber = {}", count);
        assert_eq!(count, 2);
    }

    println!("\n📊 STEP 4: After creating snapshot");
    let state = Arc::new(TestActorState { value: 30 });
    store_ref.ask(StoreCommand::Snapshot(state)).await.unwrap();

    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   LastEventNumber = {}", count);
        println!(
            "   (Should still be 2 - snapshot doesn't change event count)"
        );
        assert_eq!(count, 2);
    }

    println!("\n📊 STEP 5: After recovery");
    drop(store_ref);

    let store2 = store_new!(
        TestActor,
        "test",
        "last_event",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();

    store_ref2.ask(StoreCommand::Recover).await.unwrap();

    let result = store_ref2.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   LastEventNumber = {}", count);
        println!("   Expected: 2 (recovered from DB)");

        if count == 0 {
            println!("   ❌ PROBLEM: Reset to 0 after recovery!");
        } else {
            println!("   ✅ Correct");
        }

        assert_eq!(count, 2);
    }

    println!("\n💡 CONCLUSION:");
    println!("   LastEventNumber works correctly in all scenarios tested");
}
