//! Test if the problem is related to actor path/prefix mismatch

use ave_actors_actor::{Actor, ActorContext, ActorSystem, Handler, Message, Response, Event, Error as ActorError, ActorPath};
use ave_actors_store::store::{PersistentActor, FullPersistence};
use ave_actors_store::memory::MemoryManager;
use serde::{Serialize, Deserialize};
use borsh::{BorshSerialize, BorshDeserialize};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex as TokioMutex;

static SHARED: OnceLock<Arc<TokioMutex<MemoryManager>>> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
struct PathActor {
    value: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PathMsg(i32);
impl Message for PathMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PathResp(i32);
impl Response for PathResp {}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
struct PathEvent(i32);
impl Event for PathEvent {}

#[async_trait]
impl Actor for PathActor {
    type Message = PathMsg;
    type Response = PathResp;
    type Event = PathEvent;

    async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let path_key = ctx.path().key();
        println!("  [PRE_START] Actor path key: '{}'", path_key);

        let manager_ref = SHARED.get_or_init(|| {
            Arc::new(TokioMutex::new(MemoryManager::default()))
        });
        let manager = manager_ref.lock().await.clone();

        // Using default prefix (ctx.path().key())
        println!("  [PRE_START] Calling start_store with name='path_test', prefix=None");
        println!("  [PRE_START] This will use prefix='{}'", path_key);
        self.start_store("path_test", None, ctx, manager, None).await
    }

    async fn pre_stop(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        println!("  [PRE_STOP] Stopping, value={}", self.value);
        self.stop_store(ctx).await
    }
}

#[async_trait]
impl Handler<PathActor> for PathActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: PathMsg,
        ctx: &mut ActorContext<PathActor>,
    ) -> Result<PathResp, ActorError> {
        self.persist(&PathEvent(msg.0), ctx).await?;
        Ok(PathResp(self.value))
    }
}

#[async_trait]
impl PersistentActor for PathActor {
    type Persistence = FullPersistence;
    type InitParams = ();

    fn create_initial(_params: ()) -> Self {
        println!("  [CREATE_INITIAL]");
        Self { value: 0 }
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        println!("  [APPLY] {} + {} = {}", self.value, event.0, self.value + event.0);
        self.value += event.0;
        Ok(())
    }
}

#[tokio::test]
async fn test_path_mismatch_scenario() {
    let (system, mut runner) = ActorSystem::create(CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Scenario: Same actor name, checking path consistency    ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    println!("🔷 LIFECYCLE 1: Create actor with name 'my_path_actor'");
    let actor_ref = system
        .create_root_actor("my_path_actor", PathActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(PathMsg(42)).await.unwrap();
    println!("   Value after event: 42");

    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("\n🔷 LIFECYCLE 2: Create actor with SAME name 'my_path_actor'");
    let actor_ref2 = system
        .create_root_actor("my_path_actor", PathActor::initial(()))
        .await
        .unwrap();

    let resp = actor_ref2.ask(PathMsg(0)).await.unwrap();
    println!("   Recovered value: {}", resp.0);

    if resp.0 == 42 {
        println!("   ✅ Recovered correctly");
    } else if resp.0 == 0 {
        println!("   ❌ Started fresh (no recovery)");
    }

    assert_eq!(resp.0, 42, "Should recover value when using same actor name");
}

#[tokio::test]
async fn test_explicit_prefix_usage() {
    let (system, mut runner) = ActorSystem::create(CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Scenario: Using explicit prefix instead of path         ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    #[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
    struct PrefixActor {
        value: i32,
    }

    #[async_trait]
    impl Actor for PrefixActor {
        type Message = PathMsg;
        type Response = PathResp;
        type Event = PathEvent;

        async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            let manager_ref = SHARED.get_or_init(|| {
                Arc::new(TokioMutex::new(MemoryManager::default()))
            });
            let manager = manager_ref.lock().await.clone();

            // EXPLICIT PREFIX
            println!("  [PRE_START] Using EXPLICIT prefix='my_fixed_prefix'");
            self.start_store("prefix_test", Some("my_fixed_prefix".to_string()), ctx, manager, None).await
        }

        async fn pre_stop(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            self.stop_store(ctx).await
        }
    }

    #[async_trait]
    impl Handler<PrefixActor> for PrefixActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: PathMsg,
            ctx: &mut ActorContext<PrefixActor>,
        ) -> Result<PathResp, ActorError> {
            self.persist(&PathEvent(msg.0), ctx).await?;
            Ok(PathResp(self.value))
        }
    }

    #[async_trait]
    impl PersistentActor for PrefixActor {
        type Persistence = FullPersistence;
        type InitParams = ();

        fn create_initial(_params: ()) -> Self {
            Self { value: 0 }
        }

        fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
            self.value += event.0;
            Ok(())
        }
    }

    println!("🔷 LIFECYCLE 1: Actor name 'actor1'");
    let actor_ref = system
        .create_root_actor("actor1", PrefixActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(PathMsg(99)).await.unwrap();
    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("\n🔷 LIFECYCLE 2: DIFFERENT actor name 'actor2' but SAME prefix");
    let actor_ref2 = system
        .create_root_actor("actor2", PrefixActor::initial(()))
        .await
        .unwrap();

    let resp = actor_ref2.ask(PathMsg(0)).await.unwrap();
    println!("   Recovered value: {}", resp.0);

    if resp.0 == 99 {
        println!("   ✅ Recovered because prefix is the SAME");
    } else {
        println!("   ❌ Didn't recover");
    }

    assert_eq!(resp.0, 99, "Should recover with same explicit prefix");
}
