//! Test if the problem is related to actor path/prefix mismatch

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error as ActorError, Event,
    Handler, Message, Response,
};
use ave_actors_store::memory::MemoryManager;
use ave_actors_store::store::{FullPersistence, PersistentActor};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use test_log::test;
use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

static SHARED: OnceLock<Arc<TokioMutex<MemoryManager>>> = OnceLock::new();

// State struct
#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct PathActorState {
    value: i32,
}

#[derive(Debug)]
struct PathActor {
    state_ptr: Arc<PathActorState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PathMsg(i32);
impl Message for PathMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PathResp(i32);
impl Response for PathResp {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct PathEvent(i32);
impl Event for PathEvent {}

#[async_trait]
impl Actor for PathActor {
    type Message = PathMsg;
    type Response = PathResp;
    type Event = PathEvent;
    type SinkEvent = Self::Event;
    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("PathActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let path_key = ctx.path().key();
        println!("  [PRE_START] Actor path key: '{}'", path_key);

        let manager_ref = SHARED.get_or_init(|| {
            Arc::new(TokioMutex::new(MemoryManager::default()))
        });
        let manager = manager_ref.lock().await.clone();

        // Using default prefix (ctx.path().key())
        println!(
            "  [PRE_START] Calling start_store with name='path_test', prefix=None"
        );
        println!("  [PRE_START] This will use prefix='{}'", path_key);
        self.start_store("path_test", None, ctx, manager, None)
            .await
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
        self.persist(PathEvent(msg.0), ctx).await?;
        Ok(PathResp(self.state_ptr.value))
    }
}

#[async_trait]
impl PersistentActor for PathActor {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = PathActorState;

    fn create_initial(_params: ()) -> Self {
        println!("  [CREATE_INITIAL]");
        Self {
            state_ptr: Arc::new(PathActorState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut new_state = state;
        Arc::make_mut(&mut new_state).value += event.0;
        Ok(new_state)
    }

    fn state(&self) -> Arc<Self::State> {
        Arc::clone(&self.state_ptr)
    }

    fn set_state(&mut self, state: Arc<Self::State>) {
        self.state_ptr = state;
    }
}

#[test(tokio::test)]
async fn test_path_mismatch_scenario() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
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

    assert_eq!(
        resp.0, 42,
        "Should recover value when using same actor name"
    );
}

#[test(tokio::test)]
async fn test_explicit_prefix_usage() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    println!("\n╔═══════════════════════════════════════════════════════════╗");
    println!("║  Scenario: Using explicit prefix instead of path         ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // State struct
    #[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
    struct PrefixActorState {
        value: i32,
    }

    #[derive(Debug)]
    struct PrefixActor {
        state_ptr: Arc<PrefixActorState>,
    }

    #[async_trait]
    impl Actor for PrefixActor {
        type Message = PathMsg;
        type Response = PathResp;
        type Event = PathEvent;
    type SinkEvent = Self::Event;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("PrefixActor", id = %id)
        }

        async fn pre_start(
            &mut self,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), ActorError> {
            let manager_ref = SHARED.get_or_init(|| {
                Arc::new(TokioMutex::new(MemoryManager::default()))
            });
            let manager = manager_ref.lock().await.clone();

            // EXPLICIT PREFIX
            println!("  [PRE_START] Using EXPLICIT prefix='my_fixed_prefix'");
            self.start_store(
                "prefix_test",
                Some("my_fixed_prefix"),
                ctx,
                manager,
                None,
            )
            .await
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
            self.persist(PathEvent(msg.0), ctx).await?;
            Ok(PathResp(self.state_ptr.value))
        }
    }

    #[async_trait]
    impl PersistentActor for PrefixActor {
        type Persistence = FullPersistence;
        type InitParams = ();
        type State = PrefixActorState;

        fn create_initial(_params: ()) -> Self {
            Self {
                state_ptr: Arc::new(PrefixActorState::default()),
            }
        }

        fn apply(
            state: Arc<Self::State>,
            event: &Self::Event,
        ) -> Result<Arc<Self::State>, ActorError> {
            let mut new_state = state;
            Arc::make_mut(&mut new_state).value += event.0;
            Ok(new_state)
        }

        fn state(&self) -> Arc<Self::State> {
            Arc::clone(&self.state_ptr)
        }

        fn set_state(&mut self, state: Arc<Self::State>) {
            self.state_ptr = state;
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
