//! End-to-end persistence over a real SQLite file.
//!
//! The memory backend covers logic; these tests pin the production path:
//! SQLite file + WAL + native atomic batch, actor restart recovery, and
//! same-leaf isolation with the derived full-path prefix.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, ChildAction,
    Error as ActorError, Event, Handler, Message, NotPersistentActor, Response,
};
use ave_actors_sqlite::SqliteManager;
use ave_actors_store::{
    database::{Collection, DbManager},
    default_store_prefix,
    store::{FullPersistence, LightPersistence, PersistentActor},
};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// ============================================================================
// Full actor (snapshots every 2 events)
// ============================================================================

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct FullState {
    counter: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum FullMessage {
    Increment(i32),
    Get,
}

impl Message for FullMessage {}

#[derive(Debug, Clone, PartialEq)]
enum FullResponse {
    Counter(i32),
}

impl Response for FullResponse {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct FullEvent(i32);

impl Event for FullEvent {}

#[derive(Debug)]
struct FullActor {
    state: Arc<FullState>,
}

#[async_trait]
impl Actor for FullActor {
    type Message = FullMessage;
    type Response = FullResponse;
    type Event = FullEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("SqliteFullActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let db: SqliteManager = ctx
            .system()
            .get_helper("db")
            .expect("db helper should be installed");
        self.start_store("store", None, ctx, db, None).await
    }
}

#[async_trait]
impl PersistentActor for FullActor {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = FullState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(FullState::default()),
        }
    }

    fn snapshot_every() -> Option<u64> {
        Some(2)
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut new_state = Arc::clone(&state);
        Arc::make_mut(&mut new_state).counter += event.0;
        Ok(new_state)
    }

    fn state(&self) -> Arc<Self::State> {
        Arc::clone(&self.state)
    }

    fn set_state(&mut self, state: Arc<Self::State>) {
        self.state = state;
    }
}

#[async_trait]
impl Handler<Self> for FullActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: FullMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<FullResponse, ActorError> {
        match msg {
            FullMessage::Increment(delta) => {
                self.persist(FullEvent(delta), ctx).await?;
                Ok(FullResponse::Counter(self.state.counter))
            }
            FullMessage::Get => Ok(FullResponse::Counter(self.state.counter)),
        }
    }
}

// ============================================================================
// Light actor (snapshots only)
// ============================================================================

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct LightState {
    value: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LightMessage {
    Increment(i32),
    Get,
}

impl Message for LightMessage {}

#[derive(Debug, Clone, PartialEq)]
enum LightResponse {
    Value(i32),
}

impl Response for LightResponse {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct LightEvent(i32);

impl Event for LightEvent {}

#[derive(Debug)]
struct LightActor {
    state: Arc<LightState>,
}

#[async_trait]
impl Actor for LightActor {
    type Message = LightMessage;
    type Response = LightResponse;
    type Event = LightEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("SqliteLightActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let db: SqliteManager = ctx
            .system()
            .get_helper("db")
            .expect("db helper should be installed");
        self.start_store("store", None, ctx, db, None).await
    }
}

#[async_trait]
impl PersistentActor for LightActor {
    type Persistence = LightPersistence;
    type InitParams = ();
    type State = LightState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(LightState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut new_state = Arc::clone(&state);
        Arc::make_mut(&mut new_state).value += event.0;
        Ok(new_state)
    }

    fn state(&self) -> Arc<Self::State> {
        Arc::clone(&self.state)
    }

    fn set_state(&mut self, state: Arc<Self::State>) {
        self.state = state;
    }
}

#[async_trait]
impl Handler<Self> for LightActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: LightMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<LightResponse, ActorError> {
        match msg {
            LightMessage::Increment(delta) => {
                self.persist(LightEvent(delta), ctx).await?;
                Ok(LightResponse::Value(self.state.value))
            }
            LightMessage::Get => Ok(LightResponse::Value(self.state.value)),
        }
    }
}

// ============================================================================
// Parent hosting a persistent child named "counter"
// ============================================================================

#[derive(Debug, Clone)]
struct BranchParent;

impl NotPersistentActor for BranchParent {}

#[async_trait]
impl Actor for BranchParent {
    type Message = FullMessage;
    type Response = FullResponse;
    type Event = FullEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("SqliteBranchParent", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        ctx.create_child("counter", FullActor::initial(())).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Self> for BranchParent {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: FullMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<FullResponse, ActorError> {
        let child: ActorRef<FullActor> = ctx
            .get_child("counter")
            .await
            .map_err(|_| ActorError::Functional {
                description: "counter child missing".to_owned(),
            })?;
        child.ask(msg).await
    }

    async fn on_child_error(
        &mut self,
        _error: ActorError,
        _ctx: &mut ActorContext<Self>,
    ) {
    }

    async fn on_child_fault(
        &mut self,
        _error: ActorError,
        _ctx: &mut ActorContext<Self>,
    ) -> ChildAction {
        ChildAction::Stop
    }
}

// ============================================================================
// Tests
// ============================================================================

fn sqlite_manager() -> (tempfile::TempDir, SqliteManager) {
    let dir = tempfile::tempdir().expect("tempdir");
    let manager = SqliteManager::new(&PathBuf::from(dir.path()), false, None)
        .expect("sqlite manager");
    (dir, manager)
}

#[test(tokio::test)]
async fn test_sqlite_full_persistence_recovers_across_restart() {
    let (_dir, manager) = sqlite_manager();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("full-sqlite", FullActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(FullMessage::Increment(10)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(5)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(3)).await.unwrap();
    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Same name: full-path prefix matches, state recovers from snapshot +
    // replayed tail event.
    let actor_ref = system
        .create_root_actor("full-sqlite", FullActor::initial(()))
        .await
        .unwrap();
    assert_eq!(
        actor_ref.ask(FullMessage::Get).await.unwrap(),
        FullResponse::Counter(18)
    );

    // The event history survived on disk (snapshot_every does not compact).
    let prefix = default_store_prefix(&actor_ref.path());
    let events = manager
        .create_collection("store_events", &prefix)
        .unwrap()
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(events.len(), 3);

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_sqlite_light_persistence_recovers_snapshot_without_events() {
    let (_dir, manager) = sqlite_manager();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("light-sqlite", LightActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(LightMessage::Increment(7)).await.unwrap();
    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let actor_ref = system
        .create_root_actor("light-sqlite", LightActor::initial(()))
        .await
        .unwrap();
    assert_eq!(
        actor_ref.ask(LightMessage::Get).await.unwrap(),
        LightResponse::Value(7)
    );

    // LightPersistence keeps no event history, even on SQLite.
    let prefix = default_store_prefix(&actor_ref.path());
    let collection =
        manager.create_collection("store_events", &prefix).unwrap();
    assert!(collection.iter(false).unwrap().next().is_none());

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_sqlite_same_leaf_under_different_parents_is_isolated() {
    let (_dir, manager) = sqlite_manager();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let parent_a = system.create_root_actor("p1", BranchParent).await.unwrap();
    let parent_b = system.create_root_actor("p2", BranchParent).await.unwrap();

    parent_a.ask(FullMessage::Increment(10)).await.unwrap();
    parent_b.ask(FullMessage::Increment(100)).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    assert_eq!(
        parent_a.ask(FullMessage::Get).await.unwrap(),
        FullResponse::Counter(10)
    );
    assert_eq!(
        parent_b.ask(FullMessage::Get).await.unwrap(),
        FullResponse::Counter(100)
    );

    parent_a.ask_stop().await.unwrap();
    parent_b.ask_stop().await.unwrap();
}
