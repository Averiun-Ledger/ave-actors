//! Test to investigate if Full persistence also has the duplication bug.

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

// Shared manager for testing
static SHARED_MANAGER_FULL: OnceLock<Arc<TokioMutex<MemoryManager>>> =
    OnceLock::new();

// Actor with a vector that accumulates numbers (FullPersistence version)
#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct VectorActorFull {
    numbers: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum VectorMessageFull {
    Add(i32),
    Get,
}
impl Message for VectorMessageFull {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VectorResponseFull {
    numbers: Vec<i32>,
}
impl Response for VectorResponseFull {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct NumberAddedFull(i32);
impl Event for NumberAddedFull {}

#[async_trait]
impl Actor for VectorActorFull {
    type Message = VectorMessageFull;
    type Response = VectorResponseFull;
    type Event = NumberAddedFull;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("VectorActorFull", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let manager_ref = SHARED_MANAGER_FULL.get_or_init(|| {
            Arc::new(TokioMutex::new(MemoryManager::default()))
        });

        let manager = manager_ref.lock().await.clone();

        self.start_store("vector_test_full", None, ctx, manager, None)
            .await
    }
}

#[async_trait]
impl Handler<VectorActorFull> for VectorActorFull {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: VectorMessageFull,
        ctx: &mut ActorContext<VectorActorFull>,
    ) -> Result<VectorResponseFull, ActorError> {
        match msg {
            VectorMessageFull::Add(number) => {
                self.persist(&NumberAddedFull(number), ctx).await?;
                Ok(VectorResponseFull {
                    numbers: self.numbers.clone(),
                })
            }
            VectorMessageFull::Get => Ok(VectorResponseFull {
                numbers: self.numbers.clone(),
            }),
        }
    }
}

#[async_trait]
impl PersistentActor for VectorActorFull {
    type Persistence = FullPersistence;
    type InitParams = ();

    fn create_initial(_params: ()) -> Self {
        Self {
            numbers: Vec::new(),
        }
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.numbers.push(event.0);
        Ok(())
    }
}

#[tokio::test]
async fn test_full_persistence_duplication_on_restart() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("vector_actor_full", VectorActorFull::initial(()))
        .await
        .unwrap();

    // Add number 5
    let response = actor_ref.ask(VectorMessageFull::Add(5)).await.unwrap();

    println!("After adding 5: {:?}", response.numbers);
    assert_eq!(response.numbers, vec![5], "Should have [5] after adding 5");

    // Stop the actor (FullPersistence will create snapshot on stop if there are events)
    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Restart
    let actor_ref2 = system
        .create_root_actor("vector_actor_full", VectorActorFull::initial(()))
        .await
        .unwrap();

    let response = actor_ref2.ask(VectorMessageFull::Get).await.unwrap();

    println!("After restart (FullPersistence): {:?}", response.numbers);

    // Check if FullPersistence has the same bug
    assert_eq!(
        response.numbers,
        vec![5],
        "FullPersistence: Should have [5] after restart, but has {:?}",
        response.numbers
    );
}
