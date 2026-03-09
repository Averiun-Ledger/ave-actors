//! Test to investigate why FullPersistence doesn't recover state

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
static SHARED_MANAGER_FULL_RECOVERY: OnceLock<Arc<TokioMutex<MemoryManager>>> =
    OnceLock::new();

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct CounterActor {
    count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CounterMessage {
    Increment,
    GetCount,
}
impl Message for CounterMessage {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CounterResponse {
    count: i32,
}
impl Response for CounterResponse {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct Incremented;
impl Event for Incremented {}

#[async_trait]
impl Actor for CounterActor {
    type Message = CounterMessage;
    type Response = CounterResponse;
    type Event = Incremented;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("CounterActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let manager_ref = SHARED_MANAGER_FULL_RECOVERY.get_or_init(|| {
            Arc::new(TokioMutex::new(MemoryManager::default()))
        });

        let manager = manager_ref.lock().await.clone();

        self.start_store("counter_test_full", None, ctx, manager, None)
            .await
    }
}

#[async_trait]
impl Handler<CounterActor> for CounterActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: CounterMessage,
        ctx: &mut ActorContext<CounterActor>,
    ) -> Result<CounterResponse, ActorError> {
        match msg {
            CounterMessage::Increment => {
                self.persist(&Incremented, ctx).await?;
                Ok(CounterResponse { count: self.count })
            }
            CounterMessage::GetCount => {
                Ok(CounterResponse { count: self.count })
            }
        }
    }
}

#[async_trait]
impl PersistentActor for CounterActor {
    type Persistence = FullPersistence;
    type InitParams = ();

    fn create_initial(_params: ()) -> Self {
        Self { count: 0 }
    }

    fn apply(&mut self, _event: &Self::Event) -> Result<(), ActorError> {
        println!(
            "  [APPLY] Incrementing count from {} to {}",
            self.count,
            self.count + 1
        );
        self.count += 1;
        Ok(())
    }
}

#[tokio::test]
async fn test_full_persistence_doesnt_recover_state() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    println!(
        "\n╔════════════════════════════════════════════════════════════╗"
    );
    println!("║     FullPersistence State Recovery Investigation          ║");
    println!(
        "╚════════════════════════════════════════════════════════════╝\n"
    );

    // First lifecycle: persist multiple events
    println!("🔷 FIRST LIFECYCLE: Persisting events");
    let actor_ref = system
        .create_root_actor("counter_actor", CounterActor::initial(()))
        .await
        .unwrap();

    // Persist 3 events
    for i in 1..=3 {
        let response = actor_ref.ask(CounterMessage::Increment).await.unwrap();
        println!("   Event {}: count = {}", i, response.count);
    }

    let final_count = actor_ref.ask(CounterMessage::GetCount).await.unwrap();
    println!("   Final count before stop: {}", final_count.count);
    assert_eq!(
        final_count.count, 3,
        "Should have count=3 after 3 increments"
    );

    println!("\n🛑 STOPPING ACTOR (should create snapshot)");
    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Second lifecycle: should recover state
    println!("\n🔷 SECOND LIFECYCLE: Recovering state");
    let actor_ref2 = system
        .create_root_actor("counter_actor", CounterActor::initial(()))
        .await
        .unwrap();

    let recovered_count =
        actor_ref2.ask(CounterMessage::GetCount).await.unwrap();
    println!("   Recovered count: {}", recovered_count.count);

    println!("\n📊 RESULTS:");
    if recovered_count.count == 0 {
        println!("   ❌ BUG: State NOT recovered (count=0, expected 3)");
        println!(
            "   ❌ Actor started with initial state instead of recovering"
        );
    } else if recovered_count.count == 3 {
        println!("   ✅ OK: State recovered correctly (count=3)");
    } else {
        println!(
            "   ⚠️  UNEXPECTED: count={} (expected 3)",
            recovered_count.count
        );
    }

    assert_eq!(
        recovered_count.count, 3,
        "BUG: FullPersistence should recover state. Expected count=3, got count={}",
        recovered_count.count
    );
}
