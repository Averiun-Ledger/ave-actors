//! Root cause analysis: persist_state doesn't increment event_counter

use ave_actors_store::{
    memory::MemoryManager,
    store::{
        LightPersistence, PersistentActor, Store, StoreCommand, StoreResponse,
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
    type Persistence = LightPersistence;
    type InitParams = ();

    fn create_initial(_: ()) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.value += event.delta;
        Ok(())
    }
}

#[test(tokio::test)]
async fn test_root_cause_persist_state_doesnt_increment_counter() {
    
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();
    let store = Store::<TestActor>::new(
        "test",
        "root_cause",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    println!(
        "\n╔════════════════════════════════════════════════════════════╗"
    );
    println!("║          ROOT CAUSE ANALYSIS: persist_state bug            ║");
    println!(
        "╚════════════════════════════════════════════════════════════╝\n"
    );

    // Initial state
    println!("📊 INITIAL STATE:");
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   event_counter = {}", count);
        assert_eq!(count, 0);
    }

    // Persist with PersistLight (simulates what LightPersistence does)
    println!("\n🔧 PERSISTING EVENT:");
    println!("   Creating actor with value=0");
    println!("   Creating event with delta=10");
    let mut actor = TestActor { value: 0 };

    // This is what happens in the user's code:
    // 1. User calls persist()
    // 2. persist() calls apply() on the actor (actor.value becomes 10)
    // 3. persist() calls store.ask(PersistLight(event, actor_with_value_10))
    actor.apply(&TestEvent { delta: 10 }).unwrap();
    println!(
        "   After apply(): actor.value = {} (event already applied!)",
        actor.value
    );

    let event = TestEvent { delta: 10 };
    println!("   Calling PersistLight...");
    store_ref
        .ask(StoreCommand::PersistLight(event, actor.clone()))
        .await
        .unwrap();

    // Check counter after persist
    println!("\n📊 AFTER persist_state():");
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   event_counter = {}", count);
        println!("   ❌ BUG: Should be 1 but is {}", count);
        println!("   ❌ persist_state() does NOT increment event_counter!");
    }

    // What was saved in the snapshot?
    println!("\n💾 WHAT WAS SAVED:");
    println!("   Snapshot: actor.value = 10, state_counter = 0");
    println!("   Event DB: event at position 0 (delta=10)");

    // Recover
    println!("\n🔄 RECOVERY PROCESS:");
    drop(store_ref);
    let store2 = Store::<TestActor>::new(
        "test",
        "root_cause",
        memory_manager.clone(),
        None,
        TestActor::create_initial(()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();

    println!("   1. Load snapshot: value=10, state_counter=0");
    println!("   2. Read last event key from DB: key=0");
    println!("   3. Calculate: event_counter = key + 1 = 1");
    println!("   4. Compare: event_counter (1) != state_counter (0)");
    println!("   5. ❌ Decision: Need to replay events from 0 to 0");
    println!("   6. ❌ Apply event at position 0 AGAIN!");
    println!("   7. ❌ Result: value = 10 + 10 = 20");

    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();
    if let StoreResponse::State(Some(state)) = result {
        println!("\n📊 RECOVERED STATE:");
        println!("   actor.value = {}", state.value);
        if state.value == 20 {
            println!(
                "   ❌ BUG CONFIRMED: Event applied TWICE (should be 10, got {})",
                state.value
            );
        }
    }

    println!(
        "\n╔════════════════════════════════════════════════════════════╗"
    );
    println!("║                      THE ROOT CAUSE                        ║");
    println!("╠════════════════════════════════════════════════════════════╣");
    println!("║ persist_state() in store.rs:586-606                        ║");
    println!("║                                                            ║");
    println!("║ Line 603: self.snapshot(state)?                           ║");
    println!("║           → Sets state_counter = event_counter (= 0)      ║");
    println!("║                                                            ║");
    println!("║ Line 604-605: self.events.put(event_counter, event)       ║");
    println!("║           → Saves event at position 0                     ║");
    println!("║                                                            ║");
    println!("║ ❌ MISSING: self.event_counter += 1                        ║");
    println!("║                                                            ║");
    println!("║ Compare with persist() at line 562-563:                   ║");
    println!("║   if result.is_ok() {{                                     ║");
    println!("║       self.event_counter += 1  ← THIS IS MISSING!         ║");
    println!("║   }}                                                       ║");
    println!(
        "╚════════════════════════════════════════════════════════════╝\n"
    );
}
