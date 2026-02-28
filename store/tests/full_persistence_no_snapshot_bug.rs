//! Test to investigate recovery when there's no snapshot but there ARE events

use ave_actors_store::{
    memory::MemoryManager,
    store::{
        FullPersistence, PersistentActor, Store, StoreCommand, StoreResponse,
    },
};

use ave_actors_actor::{
    Actor, ActorContext, ActorSystem, Error as ActorError, Event, Handler,
    Message, Response, build_tracing_subscriber,
};

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
struct TestEvent {
    delta: i32,
}
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
        self.value += event.delta;
        Ok(())
    }
}

#[tokio::test]
async fn test_full_persistence_recovery_without_snapshot() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    println!(
        "\n╔════════════════════════════════════════════════════════════╗"
    );
    println!("║  FullPersistence Recovery WITHOUT Snapshot Investigation  ║");
    println!(
        "╚════════════════════════════════════════════════════════════╝\n"
    );

    // Create store and persist events WITHOUT creating a snapshot
    println!("🔷 STEP 1: Persist events WITHOUT snapshot");
    let store = Store::<TestActor>::new(
        "test",
        "no_snapshot",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Persist 3 events directly (bypassing the actor's persist which would create snapshots on stop)
    for i in 1..=3 {
        let event = TestEvent { delta: i };
        println!("   Persisting event {}", i);
        store_ref.ask(StoreCommand::Persist(event)).await.unwrap();
    }

    // Check event count
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   Total events persisted: {}", count);
        assert_eq!(count, 3);
    }

    // DON'T create snapshot - just drop the store
    println!("\n🛑 DROPPING STORE without snapshot");
    drop(store_ref);

    // Try to recover
    println!("\n🔷 STEP 2: Attempting recovery");
    let store2 = Store::<TestActor>::new(
        "test",
        "no_snapshot",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();

    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();

    println!("\n📊 RECOVERY RESULT:");
    match result {
        StoreResponse::State(Some(state)) => {
            println!("   ✅ State recovered: value = {}", state.value);
            println!("   Expected: value = 6 (1+2+3)");

            if state.value == 6 {
                println!("   ✅ OK: All events were replayed correctly");
            } else if state.value == 0 {
                println!("   ❌ BUG: No events were replayed (started fresh)");
            } else {
                println!("   ⚠️  UNEXPECTED: value = {}", state.value);
            }

            assert_eq!(
                state.value, 6,
                "Should have replayed all 3 events (1+2+3=6)"
            );
        }
        StoreResponse::State(None) => {
            println!("   ❌ BUG: No state recovered (returned None)");
            println!(
                "   ❌ This means the actor will start fresh with initial state"
            );
            println!("   ❌ Even though there are 3 events in the database!");
            panic!(
                "BUG: recover() returned None when there are events in the DB"
            );
        }
        _ => panic!("Unexpected response type"),
    }
}

#[tokio::test]
async fn test_full_persistence_recovery_logic_investigation() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    println!(
        "\n╔════════════════════════════════════════════════════════════╗"
    );
    println!("║     Detailed Investigation of Recovery Logic              ║");
    println!(
        "╚════════════════════════════════════════════════════════════╝\n"
    );

    // Create store and persist events
    let store = Store::<TestActor>::new(
        "test",
        "logic_test",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    println!("📝 Scenario: Events exist, but NO snapshot");
    println!("   Persisting 2 events...");
    store_ref
        .ask(StoreCommand::Persist(TestEvent { delta: 5 }))
        .await
        .unwrap();
    store_ref
        .ask(StoreCommand::Persist(TestEvent { delta: 7 }))
        .await
        .unwrap();

    drop(store_ref);

    println!("\n🔄 Recovery process analysis:");
    println!("   1. recover() is called");
    println!("   2. Checks: if let Some((state, counter)) = self.get_state()?");
    println!("      → get_state() returns None (no snapshot exists)");
    println!("   3. Goes to else branch (line 779-782)");
    println!("   4. Returns: Ok(None)");
    println!("   5. ❌ Result: Actor starts with initial state");
    println!("   6. ❌ Events in DB are IGNORED!");

    println!("\n🐛 THE PROBLEM:");
    println!("   recover() only replays events if there's a snapshot.");
    println!("   If there's NO snapshot but there ARE events:");
    println!("   → It returns None");
    println!("   → Actor starts fresh");
    println!("   → Data loss!");

    println!("\n💡 WHEN THIS HAPPENS:");
    println!("   - FullPersistence actor persists events");
    println!("   - Actor crashes BEFORE calling stop_store()");
    println!("   - No snapshot was created (only happens on stop)");
    println!("   - On restart: events exist but no snapshot");
    println!("   - Result: Actor loses all state");

    // Verify
    let store2 = Store::<TestActor>::new(
        "test",
        "logic_test",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();

    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();

    if let StoreResponse::State(None) = result {
        println!(
            "\n❌ CONFIRMED: recover() returned None even though events exist"
        );
    } else {
        println!(
            "\n⚠️  Unexpected: recover() returned Some (maybe the logic changed?)"
        );
    }
}
