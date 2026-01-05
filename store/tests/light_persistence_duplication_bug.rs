//! Test that demonstrates the duplication bug in Light persistence mode.
//!
//! When an actor with Light persistence receives an event:
//! 1. The event is applied to the actor's state
//! 2. Both the event AND the modified state are persisted
//!
//! When the actor is stopped and restarted:
//! 1. The state snapshot is loaded (which already has the event applied)
//! 2. The event is replayed and applied AGAIN
//! 3. This causes duplication
//!
//! Expected: vector [3]
//! Actual: vector [3, 3] after restart

use ave_actors_actor::{Actor, ActorContext, ActorSystem, Handler, Message, Response, Event, Error as ActorError, ActorPath};
use ave_actors_store::store::{PersistentActor, LightPersistence};
use ave_actors_store::memory::MemoryManager;
use serde::{Serialize, Deserialize};
use borsh::{BorshSerialize, BorshDeserialize};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex as TokioMutex;

// Shared manager for testing
static SHARED_MANAGER: OnceLock<Arc<TokioMutex<MemoryManager>>> = OnceLock::new();

// Actor with a vector that accumulates numbers
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
struct VectorActor {
    numbers: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum VectorMessage {
    Add(i32),
    Get,
}
impl Message for VectorMessage {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VectorResponse {
    numbers: Vec<i32>,
}
impl Response for VectorResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
struct NumberAdded(i32);
impl Event for NumberAdded {}

#[async_trait]
impl Actor for VectorActor {
    type Message = VectorMessage;
    type Response = VectorResponse;
    type Event = NumberAdded;

    async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        // Get or initialize the shared manager
        let manager_ref = SHARED_MANAGER.get_or_init(|| {
            Arc::new(TokioMutex::new(MemoryManager::default()))
        });

        let manager = manager_ref.lock().await.clone();

        self.start_store("vector_test", None, ctx, manager, None).await
    }

    async fn pre_stop(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        self.stop_store(ctx).await
    }
}

#[async_trait]
impl Handler<VectorActor> for VectorActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: VectorMessage,
        ctx: &mut ActorContext<VectorActor>,
    ) -> Result<VectorResponse, ActorError> {
        match msg {
            VectorMessage::Add(number) => {
                // Persist the event (this will apply it AND save the state)
                self.persist(&NumberAdded(number), ctx).await?;
                Ok(VectorResponse {
                    numbers: self.numbers.clone(),
                })
            }
            VectorMessage::Get => {
                Ok(VectorResponse {
                    numbers: self.numbers.clone(),
                })
            }
        }
    }
}

#[async_trait]
impl PersistentActor for VectorActor {
    type Persistence = LightPersistence;
    type InitParams = ();

    fn create_initial(_params: ()) -> Self {
        Self {
            numbers: Vec::new(),
        }
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        // Apply the event by adding the number to the vector
        self.numbers.push(event.0);
        Ok(())
    }
}

#[tokio::test]
async fn test_light_persistence_duplicates_data_on_restart() {
    let (system, mut runner) = ActorSystem::create(CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    // Create and start the actor (pre_start will initialize with shared manager)
    let actor_ref = system
        .create_root_actor("vector_actor", VectorActor::initial(()))
        .await
        .unwrap();

    // Add number 3
    let response = actor_ref
        .ask(VectorMessage::Add(3))
        .await
        .unwrap();

    println!("After adding 3: {:?}", response.numbers);
    assert_eq!(response.numbers, vec![3], "Should have [3] after adding 3");

    // Stop the actor (this will trigger snapshot in pre_stop)
    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create a NEW actor with the same name (simulating restart)
    // It will use the SAME shared MemoryManager via pre_start
    let actor_ref2 = system
        .create_root_actor("vector_actor", VectorActor::initial(()))
        .await
        .unwrap();

    // Get the numbers after restart
    let response = actor_ref2
        .ask(VectorMessage::Get)
        .await
        .unwrap();

    println!("After restart: {:?}", response.numbers);

    // THIS IS THE BUG: The number 3 appears twice!
    // Expected: [3]
    // Actual: [3, 3]
    //
    // Why? Because:
    // 1. When we persisted with LightPersistence, we saved BOTH the event AND the state (which already had 3 applied)
    // 2. On recovery, we load the state (with 3) and then replay the event (adding 3 again)
    assert_eq!(
        response.numbers,
        vec![3],
        "BUG: Should have [3] after restart, but has {:?} due to event replay on already-applied state",
        response.numbers
    );
}
