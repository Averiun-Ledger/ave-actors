//! Test specific scenario: brand new actor, ONE event, graceful stop, no recovery

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

static SHARED_MGR: OnceLock<Arc<TokioMutex<MemoryManager>>> = OnceLock::new();

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct SingleEventActor {
    data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Msg {
    SetData(String),
    GetData,
}
impl Message for Msg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Resp {
    data: String,
}
impl Response for Resp {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct DataSet(String);
impl Event for DataSet {}

#[async_trait]
impl Actor for SingleEventActor {
    type Message = Msg;
    type Response = Resp;
    type Event = DataSet;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("SingleEventActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        println!(
            "  [PRE_START] Actor starting, current data: '{}'",
            self.data
        );
        let manager_ref = SHARED_MGR.get_or_init(|| {
            Arc::new(TokioMutex::new(MemoryManager::default()))
        });
        let manager = manager_ref.lock().await.clone();
        self.start_store("single_event_test", None, ctx, manager, None)
            .await
    }

    async fn pre_stop(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        println!("  [PRE_STOP] Actor stopping, current data: '{}'", self.data);
        self.stop_store(ctx).await
    }
}

#[async_trait]
impl Handler<SingleEventActor> for SingleEventActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: Msg,
        ctx: &mut ActorContext<SingleEventActor>,
    ) -> Result<Resp, ActorError> {
        match msg {
            Msg::SetData(new_data) => {
                println!("  [HANDLER] Setting data: '{}'", new_data);
                self.persist(&DataSet(new_data), ctx).await?;
                Ok(Resp {
                    data: self.data.clone(),
                })
            }
            Msg::GetData => {
                println!("  [HANDLER] Getting data: '{}'", self.data);
                Ok(Resp {
                    data: self.data.clone(),
                })
            }
        }
    }
}

#[async_trait]
impl PersistentActor for SingleEventActor {
    type Persistence = FullPersistence;
    type InitParams = ();

    fn create_initial(_params: ()) -> Self {
        println!("  [CREATE_INITIAL] Creating actor with empty data");
        Self {
            data: String::new(),
        }
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        println!("  [APPLY] data: '{}' → '{}'", self.data, event.0);
        self.data = event.0.clone();
        Ok(())
    }
}

#[tokio::test]
async fn test_single_event_no_recovery() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Exact Scenario: New Actor, ONE Event, Stop, No Recovery ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    println!("🔷 LIFECYCLE 1: Brand new actor");
    let actor_ref = system
        .create_root_actor("my_actor", SingleEventActor::initial(()))
        .await
        .unwrap();

    println!("\n📝 Persisting ONE event");
    let resp = actor_ref
        .ask(Msg::SetData("Hello World".to_string()))
        .await
        .unwrap();
    println!("   Response data: '{}'", resp.data);
    assert_eq!(resp.data, "Hello World");

    println!("\n🛑 Stopping actor gracefully");
    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    println!("\n🔷 LIFECYCLE 2: Restart");
    let actor_ref2 = system
        .create_root_actor("my_actor", SingleEventActor::initial(()))
        .await
        .unwrap();

    println!("\n📊 Getting data after restart");
    let resp = actor_ref2.ask(Msg::GetData).await.unwrap();
    println!("   Recovered data: '{}'", resp.data);

    if resp.data.is_empty() {
        println!("\n❌ BUG CONFIRMED: Data is empty!");
        println!("   Expected: 'Hello World'");
        println!("   Got: ''");
        println!("   State was NOT recovered even though:");
        println!("   - We had 1 event");
        println!("   - We stopped gracefully");
        println!("   - No previous snapshot existed");
    } else if resp.data == "Hello World" {
        println!("\n✅ OK: Data recovered correctly");
    }

    assert_eq!(
        resp.data, "Hello World",
        "Should recover data after graceful stop"
    );
}

#[tokio::test]
async fn test_debug_event_counter_after_first_event() {
    build_tracing_subscriber();
    use ave_actors_store::store::{Store, StoreCommand, StoreResponse};

    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    println!(
        "\n╔════════════════════════════════════════════════════════════╗"
    );
    println!("║     Debug: Event Counter After Persisting First Event     ║");
    println!(
        "╚════════════════════════════════════════════════════════════╝\n"
    );

    let store = Store::<SingleEventActor>::new(
        "test",
        "debug_counter",
        memory_manager.clone(),
        None,
        SingleEventActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    println!("📝 Initial state");
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   event_counter = {}", count);
        assert_eq!(count, 0);
    }

    println!("\n📝 Persist ONE event");
    store_ref
        .ask(StoreCommand::Persist(DataSet("test".to_string())))
        .await
        .unwrap();

    println!("\n📝 Check event_counter");
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        println!("   event_counter = {}", count);
        if count == 0 {
            println!("   ❌ BUG: event_counter is still 0 after persisting!");
            println!(
                "   ❌ This means stop_store() condition (count > 0) will be FALSE"
            );
            println!("   ❌ No snapshot will be created!");
        } else if count == 1 {
            println!("   ✅ OK: event_counter incremented correctly");
            println!("   ✅ stop_store() will create snapshot");
        }
        assert_eq!(
            count, 1,
            "event_counter should be 1 after persisting 1 event"
        );
    }

    println!("\n📝 What happens in stop_store():");
    println!("   if event_counter > 0 {{ // {} > 0 ?", 1);
    println!("       → YES, will create snapshot");
    println!("   }}");
}
