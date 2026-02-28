//! Test to investigate why FullPersistence doesn't recover state after FIRST stop
//! when there was no previous snapshot

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error as ActorError, Event,
    Handler, Message, Response, build_tracing_subscriber,
};
use ave_actors_store::memory::MemoryManager;
use ave_actors_store::store::{FullPersistence, PersistentActor};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

static SHARED_MANAGER_FIRST_STOP: OnceLock<Arc<TokioMutex<MemoryManager>>> =
    OnceLock::new();

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct TestActor {
    value: i32,
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

    async fn pre_stop(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        self.stop_store(ctx).await
    }
}

#[async_trait]
impl Handler<TestActor> for TestActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: TestMessage,
        ctx: &mut ActorContext<TestActor>,
    ) -> Result<TestResponse, ActorError> {
        match msg {
            TestMessage::Add(delta) => {
                self.persist(&AddEvent(delta), ctx).await?;
                Ok(TestResponse { value: self.value })
            }
            TestMessage::Get => Ok(TestResponse { value: self.value }),
        }
    }
}

#[async_trait]
impl PersistentActor for TestActor {
    type Persistence = FullPersistence;
    type InitParams = ();

    fn create_initial(_params: ()) -> Self {
        println!("  [CREATE_INITIAL] Creating new actor with value=0");
        Self { value: 0 }
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        println!(
            "  [APPLY] value: {} + {} = {}",
            self.value,
            event.0,
            self.value + event.0
        );
        self.value += event.0;
        Ok(())
    }
}

#[tokio::test]
async fn test_full_persistence_first_stop_no_previous_snapshot() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    println!(
        "\n╔════════════════════════════════════════════════════════════╗"
    );
    println!("║  FullPersistence: First Stop Without Previous Snapshot    ║");
    println!(
        "╚════════════════════════════════════════════════════════════╝\n"
    );

    println!("📝 SCENARIO:");
    println!("   - Brand new actor (no previous snapshot exists)");
    println!("   - Persist some events");
    println!("   - Stop GRACEFULLY with stop()");
    println!("   - Restart and try to recover");
    println!("");

    // FIRST LIFECYCLE: Brand new actor, no previous snapshot
    println!("🔷 LIFECYCLE 1: Brand new actor");
    let actor_ref = system
        .create_root_actor("test_actor", TestActor::initial(()))
        .await
        .unwrap();

    println!("\n📝 Persisting events:");
    actor_ref.ask(TestMessage::Add(10)).await.unwrap();
    actor_ref.ask(TestMessage::Add(20)).await.unwrap();

    let response = actor_ref.ask(TestMessage::Get).await.unwrap();
    println!("   Current value: {}", response.value);
    assert_eq!(response.value, 30, "Should be 10+20=30");

    println!("\n🛑 STOPPING ACTOR GRACEFULLY (calling stop)");
    println!("   This should trigger:");
    println!("   1. pre_stop()");
    println!("   2. stop_store()");
    println!("   3. Check: event_counter > 0? YES (we have 2 events)");
    println!("   4. Create snapshot with current state (value=30)");

    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // SECOND LIFECYCLE: Restart and recover
    println!("\n🔷 LIFECYCLE 2: Restart and recover");
    let actor_ref2 = system
        .create_root_actor("test_actor", TestActor::initial(()))
        .await
        .unwrap();

    let response = actor_ref2.ask(TestMessage::Get).await.unwrap();
    println!("\n📊 RECOVERED VALUE: {}", response.value);

    if response.value == 0 {
        println!("   ❌ BUG: Value is 0 (started fresh)");
        println!("   ❌ Even though we stopped gracefully!");
        println!("   ❌ Snapshot was NOT created or NOT loaded");
    } else if response.value == 30 {
        println!("   ✅ OK: Value is 30 (correctly recovered)");
    } else {
        println!("   ⚠️  UNEXPECTED: Value is {}", response.value);
    }

    assert_eq!(
        response.value, 30,
        "BUG: Should recover value=30 after graceful stop. Got value={}",
        response.value
    );
}

#[tokio::test]
async fn test_investigate_stop_store_snapshot_creation() {
    build_tracing_subscriber();
    use ave_actors_store::store::{Store, StoreCommand, StoreResponse};

    let (system, mut runner) = ActorSystem::create(CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    println!(
        "\n╔════════════════════════════════════════════════════════════╗"
    );
    println!("║     Investigation: Does stop_store() Create Snapshot?     ║");
    println!(
        "╚════════════════════════════════════════════════════════════╝\n"
    );

    // Create store and persist events
    let store = Store::<TestActor>::new(
        "test",
        "stop_investigation",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    println!("📝 STEP 1: Persist 2 events");
    store_ref
        .ask(StoreCommand::Persist(AddEvent(5)))
        .await
        .unwrap();
    store_ref
        .ask(StoreCommand::Persist(AddEvent(7)))
        .await
        .unwrap();

    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   Event count: {}", count);
        assert_eq!(count, 2);
    }

    println!("\n📝 STEP 2: Check if snapshot exists BEFORE stop");
    drop(store_ref);

    let store2 = Store::<TestActor>::new(
        "test",
        "stop_investigation",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();

    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(_)) => {
            println!("   ✅ Snapshot exists (unexpected at this point)");
        }
        StoreResponse::State(None) => {
            println!(
                "   ❌ No snapshot exists (expected - we haven't called Snapshot command yet)"
            );
        }
        _ => panic!("Unexpected response"),
    }

    println!("\n📝 STEP 3: Simulate what stop_store() does");
    println!("   Code from stop_store() (lines 406-411):");
    println!("   if event_counter > 0 {{");
    println!("       store.ask(StoreCommand::Snapshot(self.clone())).await");
    println!("   }}");

    // Get event count
    let result = store_ref2.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("\n   event_counter = {}", count);

        if count > 0 {
            println!("   ✅ Condition met: event_counter > 0");
            println!("   ➡️  Should create snapshot");

            // Create an actor state to snapshot
            let mut actor = TestActor::create_initial(());
            // Apply events manually
            actor.apply(&AddEvent(5)).unwrap();
            actor.apply(&AddEvent(7)).unwrap();

            println!("   Creating snapshot with value={}", actor.value);
            store_ref2.ask(StoreCommand::Snapshot(actor)).await.unwrap();
            println!("   ✅ Snapshot created");
        } else {
            println!("   ❌ Condition NOT met: event_counter = 0");
            println!("   ❌ Snapshot will NOT be created");
        }
    }

    println!("\n📝 STEP 4: Verify snapshot was created");
    drop(store_ref2);

    let store3 = Store::<TestActor>::new(
        "test",
        "stop_investigation",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
    )
    .unwrap();
    let store_ref3 = system.create_root_actor("store3", store3).await.unwrap();

    let result = store_ref3.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(state)) => {
            println!("   ✅ Snapshot recovered: value = {}", state.value);
            assert_eq!(state.value, 12);
        }
        StoreResponse::State(None) => {
            println!("   ❌ No snapshot recovered");
            panic!("Snapshot should have been created!");
        }
        _ => panic!("Unexpected response"),
    }
}
