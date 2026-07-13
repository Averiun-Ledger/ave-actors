//! Regression tests for FullPersistence prefix-based recovery.
//!
//! Guarantees that an actor recovers its state when recreated with the same
//! actor name (path-derived prefix), and when recreated under a different
//! name that shares the same explicit prefix.

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

    type ChildError = ActorError;
    type ChildFault = ActorError;

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
        let manager_ref = SHARED.get_or_init(|| {
            Arc::new(TokioMutex::new(MemoryManager::default()))
        });
        let manager = manager_ref.lock().await.clone();

        // Default prefix is derived from the actor path key.
        self.start_store("path_test", None, ctx, manager, None)
            .await
    }
}

#[async_trait]
impl Handler<Self> for PathActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: PathMsg,
        ctx: &mut ActorContext<Self>,
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

    let actor_ref = system
        .create_root_actor("my_path_actor", PathActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(PathMsg(42)).await.unwrap();

    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Recreate with the same actor name so the path-derived prefix matches.
    let actor_ref2 = system
        .create_root_actor("my_path_actor", PathActor::initial(()))
        .await
        .unwrap();

    let resp = actor_ref2.ask(PathMsg(0)).await.unwrap();

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
        type ChildError = ActorError;
        type ChildFault = ActorError;

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

            // Explicit prefix, independent of the actor path.
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
    impl Handler<Self> for PrefixActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: PathMsg,
            ctx: &mut ActorContext<Self>,
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

    let actor_ref = system
        .create_root_actor("actor1", PrefixActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(PathMsg(99)).await.unwrap();
    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Different actor name but the same explicit prefix must still recover.
    let actor_ref2 = system
        .create_root_actor("actor2", PrefixActor::initial(()))
        .await
        .unwrap();

    let resp = actor_ref2.ask(PathMsg(0)).await.unwrap();

    assert_eq!(resp.0, 99, "Should recover with same explicit prefix");
}
