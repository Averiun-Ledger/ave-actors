//! Investigate if LastEventNumber might be returning 0 incorrectly

use ave_actors_store::{
    memory::MemoryManager,
    store::{
        FullPersistence, PersistentActor, Store, StoreCommand, StoreResponse,
    },
};

use ave_actors_actor::{
    Actor, ActorContext, ActorSystem, Error as ActorError, Event, Handler,
    Message, Response, 
};
use test_log::test;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    Default,
)]
struct TestActor {
    value: i32,
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

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TestActor", id = %id)
    }
}

#[async_trait]
impl Handler<TestActor> for TestActor {
    async fn handle_message(
        &mut self,
        _sender: ave_actors_actor::ActorPath,
        _msg: TestMessage,
        _ctx: &mut ActorContext<TestActor>,
    ) -> Result<TestResponse, ActorError> {
        Ok(TestResponse)
    }
}

#[async_trait]
impl PersistentActor for TestActor {
    type Persistence = FullPersistence;
    type InitParams = ();

    fn create_initial(_: ()) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.value += event.0;
        Ok(())
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

    let store = Store::<TestActor>::new(
        "test",
        "last_event",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
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
        .ask(StoreCommand::Persist(TestEvent(10)))
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
        .ask(StoreCommand::Persist(TestEvent(20)))
        .await
        .unwrap();

    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   LastEventNumber = {}", count);
        assert_eq!(count, 2);
    }

    println!("\n📊 STEP 4: After creating snapshot");
    let actor = TestActor { value: 30 };
    store_ref.ask(StoreCommand::Snapshot(actor)).await.unwrap();

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

    let store2 = Store::<TestActor>::new(
        "test",
        "last_event",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
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
