//! End-to-end persistence over a real SQLite file.
//!
//! The memory backend covers logic; these tests pin the production path:
//! SQLite file + WAL + native atomic batch, actor restart recovery, and
//! same-leaf isolation with the derived full-path prefix.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ChildAction, Error as ActorError,
    Event, Handler, Message, NotPersistentActor, Response, TestSystem,
};
use ave_actors_sqlite::SqliteManager;
use ave_actors_store::{
    database::{Collection, DbManager, Durability},
    default_store_prefix,
    store::PersistentActor,
};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use test_log::test;
use tracing::info_span;

// ============================================================================
// Full actor (snapshots every 2 events)
// ============================================================================

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct StandardState {
    counter: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StandardMessage {
    Increment(i32),
    Get,
}

impl Message for StandardMessage {}

#[derive(Debug, Clone, PartialEq)]
enum StandardResponse {
    Counter(i32),
}

impl Response for StandardResponse {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct StandardEvent(i32);

impl Event for StandardEvent {}

#[derive(Debug)]
struct StandardActor {
    state: Arc<StandardState>,
}

#[async_trait]
impl Actor for StandardActor {
    type Message = StandardMessage;
    type Response = StandardResponse;
    type Event = StandardEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("SqliteStandardActor", id = %id)
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
impl PersistentActor for StandardActor {
    type InitParams = ();
    type State = StandardState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(StandardState::default()),
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
impl Handler<Self> for StandardActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: StandardMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<StandardResponse, ActorError> {
        match msg {
            StandardMessage::Increment(delta) => {
                self.persist(StandardEvent(delta), ctx).await?;
                Ok(StandardResponse::Counter(self.state.counter))
            }
            StandardMessage::Get => {
                Ok(StandardResponse::Counter(self.state.counter))
            }
        }
    }
}

// ============================================================================
// Plain actor (events + snapshots, no pruning)
// ============================================================================

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct SnapshotState {
    value: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SnapshotMessage {
    Increment(i32),
    Get,
}

impl Message for SnapshotMessage {}

#[derive(Debug, Clone, PartialEq)]
enum SnapshotResponse {
    Value(i32),
}

impl Response for SnapshotResponse {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct SnapshotEvent(i32);

impl Event for SnapshotEvent {}

#[derive(Debug)]
struct SnapshotActor {
    state: Arc<SnapshotState>,
}

#[async_trait]
impl Actor for SnapshotActor {
    type Message = SnapshotMessage;
    type Response = SnapshotResponse;
    type Event = SnapshotEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("SqliteSnapshotActor", id = %id)
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
impl PersistentActor for SnapshotActor {
    type InitParams = ();
    type State = SnapshotState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(SnapshotState::default()),
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
impl Handler<Self> for SnapshotActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: SnapshotMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<SnapshotResponse, ActorError> {
        match msg {
            SnapshotMessage::Increment(delta) => {
                self.persist(SnapshotEvent(delta), ctx).await?;
                Ok(SnapshotResponse::Value(self.state.value))
            }
            SnapshotMessage::Get => {
                Ok(SnapshotResponse::Value(self.state.value))
            }
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
    type Message = StandardMessage;
    type Response = StandardResponse;
    type Event = StandardEvent;
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
        ctx.create_child("counter", StandardActor::initial(()))
            .await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Self> for BranchParent {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: StandardMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<StandardResponse, ActorError> {
        let child: ActorRef<StandardActor> = ctx
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
    let manager = SqliteManager::new(
        &PathBuf::from(dir.path()),
        Durability::Relaxed,
        None,
    )
    .expect("sqlite manager");
    (dir, manager)
}

#[test(tokio::test)]
async fn test_sqlite_persistence_recovers_across_restart() {
    let (_dir, manager) = sqlite_manager();
    let harness = TestSystem::start();
    let system = harness.system();

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("full-sqlite", StandardActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(StandardMessage::Increment(10)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(5)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(3)).await.unwrap();
    actor_ref.ask_stop().await.unwrap();

    // Same name: full-path prefix matches, state recovers from snapshot +
    // replayed tail event.
    let actor_ref = system
        .create_root_actor("full-sqlite", StandardActor::initial(()))
        .await
        .unwrap();
    assert_eq!(
        actor_ref.ask(StandardMessage::Get).await.unwrap(),
        StandardResponse::Counter(18)
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
    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_sqlite_recovers_snapshot_with_events() {
    let (_dir, manager) = sqlite_manager();
    let harness = TestSystem::start();
    let system = harness.system();

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("snapshot-sqlite", SnapshotActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(SnapshotMessage::Increment(7)).await.unwrap();
    actor_ref.ask_stop().await.unwrap();

    let actor_ref = system
        .create_root_actor("snapshot-sqlite", SnapshotActor::initial(()))
        .await
        .unwrap();
    assert_eq!(
        actor_ref.ask(SnapshotMessage::Get).await.unwrap(),
        SnapshotResponse::Value(7)
    );

    // Single persistence mode: the event that produced the
    // snapshot is retained (pruning is opt-in per actor).
    let prefix = default_store_prefix(&actor_ref.path());
    let collection =
        manager.create_collection("store_events", &prefix).unwrap();
    assert!(collection.iter(false).unwrap().next().is_some());

    actor_ref.ask_stop().await.unwrap();
    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_sqlite_same_leaf_under_different_parents_is_isolated() {
    let (_dir, manager) = sqlite_manager();
    let harness = TestSystem::start();
    let system = harness.system();

    system.add_helper("db", manager.clone());

    let parent_a = system.create_root_actor("p1", BranchParent).await.unwrap();
    let parent_b = system.create_root_actor("p2", BranchParent).await.unwrap();

    parent_a.ask(StandardMessage::Increment(10)).await.unwrap();
    parent_b.ask(StandardMessage::Increment(100)).await.unwrap();

    assert_eq!(
        parent_a.ask(StandardMessage::Get).await.unwrap(),
        StandardResponse::Counter(10)
    );
    assert_eq!(
        parent_b.ask(StandardMessage::Get).await.unwrap(),
        StandardResponse::Counter(100)
    );

    parent_a.ask_stop().await.unwrap();
    parent_b.ask_stop().await.unwrap();
    harness.shutdown().await;
}

// ============================================================================
// Prune actor (snapshots every 2 events, covered events deleted)
// ============================================================================

#[derive(Debug)]
struct PruneActor {
    state: Arc<StandardState>,
}

#[async_trait]
impl Actor for PruneActor {
    type Message = StandardMessage;
    type Response = StandardResponse;
    type Event = StandardEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("SqlitePruneActor", id = %id)
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
impl PersistentActor for PruneActor {
    type InitParams = ();
    type State = StandardState;

    fn snapshot_every() -> Option<u64> {
        Some(2)
    }

    fn prune_events_on_snapshot() -> bool {
        true
    }

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(StandardState::default()),
        }
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
impl Handler<Self> for PruneActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: StandardMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<StandardResponse, ActorError> {
        match msg {
            StandardMessage::Increment(delta) => {
                self.persist(StandardEvent(delta), ctx).await?;
                Ok(StandardResponse::Counter(self.state.counter))
            }
            StandardMessage::Get => {
                Ok(StandardResponse::Counter(self.state.counter))
            }
        }
    }
}

#[test(tokio::test)]
async fn test_sqlite_prune_compacts_history_across_restart() {
    use ave_actors_store::database::Collection;

    let (_dir, manager) = sqlite_manager();
    let harness = TestSystem::start();
    let system = harness.system();

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("prune-sqlite", PruneActor::initial(()))
        .await
        .unwrap();

    // 4 events: snapshots at 2 and 4 prune everything covered via the
    // native SQLite range delete.
    for delta in [10, 5, 3, 7] {
        actor_ref
            .ask(StandardMessage::Increment(delta))
            .await
            .unwrap();
    }
    actor_ref.ask_stop().await.unwrap();

    let prefix = default_store_prefix(&actor_ref.path());
    let events = manager
        .create_collection("store_events", &prefix)
        .unwrap()
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        events.is_empty(),
        "pruned history must not survive on disk, found {} rows",
        events.len()
    );

    // Same name: state recovers from the snapshot alone, and the log
    // keeps working with monotonic keys.
    let actor_ref = system
        .create_root_actor("prune-sqlite", PruneActor::initial(()))
        .await
        .unwrap();
    assert_eq!(
        actor_ref.ask(StandardMessage::Get).await.unwrap(),
        StandardResponse::Counter(25)
    );
    actor_ref.ask(StandardMessage::Increment(1)).await.unwrap();
    assert_eq!(
        actor_ref.ask(StandardMessage::Get).await.unwrap(),
        StandardResponse::Counter(26)
    );

    actor_ref.ask_stop().await.unwrap();
    harness.shutdown().await;
}
