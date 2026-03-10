//! Test to debug the state_counter and event_counter values during LightPersistence

use ave_actors_store::{
    memory::MemoryManager,
    store::{
        LightPersistence, PersistentActor, Store, StoreCommand, StoreResponse,
    },
};
use test_log::test;

use ave_actors_actor::{
    Actor, ActorContext, ActorSystem, Error as ActorError, Event, Handler,
    Message, Response, 
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
struct DebugActor {
    value: i32,
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
    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("DebugActor", id = %id)
    }
}

#[async_trait]
impl Handler<DebugActor> for DebugActor {
    async fn handle_message(
        &mut self,
        _sender: ave_actors_actor::ActorPath,
        msg: DebugMessage,
        _ctx: &mut ActorContext<DebugActor>,
    ) -> Result<DebugResponse, ActorError> {
        match msg {
            DebugMessage::Add(_) => Ok(DebugResponse::Success),
            DebugMessage::GetValue => Ok(DebugResponse::Value(self.value)),
        }
    }
}

#[async_trait]
impl PersistentActor for DebugActor {
    type Persistence = LightPersistence;
    type InitParams = ();

    fn create_initial(_: ()) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        println!("  [DEBUG] Applying event with delta: {}", event.delta);
        self.value += event.delta;
        println!("  [DEBUG] State after apply: value={}", self.value);
        Ok(())
    }
}

#[test(tokio::test)]

async fn test_debug_light_persistence_counters() {
    
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    // Create store
    let store = Store::<DebugActor>::new(
        "debug_test",
        "test_light",
        memory_manager.clone(),
        None,
        DebugActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    println!("\n=== STEP 1: Check initial counters ===");
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => {
            println!("Initial event_counter: {}", count);
            assert_eq!(count, 0, "Should start at 0");
        }
        _ => panic!("Expected LastEventNumber response"),
    }

    println!("\n=== STEP 2: Persist first event with PersistLight ===");
    let actor = DebugActor { value: 0 };
    let event = DebugEvent { delta: 10 };

    println!("Before persist: actor.value = {}", actor.value);
    let result = store_ref
        .ask(StoreCommand::PersistLight(event, actor.clone()))
        .await
        .unwrap();
    match result {
        StoreResponse::Persisted => println!("Event persisted successfully"),
        _ => panic!("Expected Persisted response"),
    }

    println!("\n=== STEP 3: Check counters after first persist ===");
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => {
            println!("event_counter after first persist: {}", count);
            println!("Expected: 1 (one event at position 0)");
            println!("Actual: {}", count);
        }
        _ => panic!("Expected LastEventNumber response"),
    }

    println!("\n=== STEP 4: Recover state ===");
    drop(store_ref);

    let store2 = Store::<DebugActor>::new(
        "debug_test",
        "test_light",
        memory_manager.clone(),
        None,
        DebugActor::create_initial(()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();

    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(state)) => {
            println!("Recovered state: value = {}", state.value);
            println!("Expected: 10 (event was applied once)");
            println!("Actual: {}", state.value);

            if state.value == 20 {
                println!(
                    "BUG CONFIRMED: Event was applied TWICE (10 + 10 = 20)"
                );
            } else if state.value == 10 {
                println!("OK: Event was applied only once");
            }
        }
        _ => panic!("Expected State response with Some"),
    }

    println!("\n=== STEP 5: Check final counters ===");
    let result = store_ref2.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => {
            println!("event_counter after recovery: {}", count);
        }
        _ => panic!("Expected LastEventNumber response"),
    }
}
