//! Integration tests for persistence.
//!
//! persistence stores the event stream and snapshots periodically. These
//! tests verify that behaviour end-to-end.

#[macro_use]
mod helpers;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, ChildAction,
    Error as ActorError, Event, Handler, Message, NotPersistentActor, Response,
};
use ave_actors_store::{
    database::{Collection, DbManager},
    default_store_prefix,
    memory::MemoryManager,
    store::{PersistentActor, StoreCommand, StoreResponse},
};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct StandardActorState {
    counter: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum StandardMessage {
    Increment(i32),
    Get,
}

impl Message for StandardMessage {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct StandardEvent(i32);

impl Event for StandardEvent {}

#[derive(Debug, Clone, PartialEq)]
enum StandardResponse {
    Counter(i32),
}

impl Response for StandardResponse {}

#[derive(Debug)]
struct StandardActor {
    state: Arc<StandardActorState>,
}

#[async_trait]
impl Actor for StandardActor {
    type Message = StandardMessage;
    type Event = StandardEvent;
    type SinkEvent = Self::Event;
    type Response = StandardResponse;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("StandardActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let db: MemoryManager = ctx
            .system()
            .get_helper("db")
            .expect("db helper should be installed");
        self.start_store("store", None, ctx, db, None).await
    }
}

#[async_trait]
impl PersistentActor for StandardActor {
    type InitParams = ();
    type State = StandardActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(StandardActorState::default()),
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

#[test(tokio::test)]
async fn test_persistence_actor_recovers_from_snapshot_and_events() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", MemoryManager::default());

    let actor_ref = system
        .create_root_actor("full-recover", StandardActor::initial(()))
        .await
        .unwrap();

    // snapshot_every = 2, so after 3 events: snapshot at 2 events, 1 pending.
    actor_ref.ask(StandardMessage::Increment(10)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(5)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(3)).await.unwrap();

    let response = actor_ref.ask(StandardMessage::Get).await.unwrap();
    assert_eq!(response, StandardResponse::Counter(18));

    actor_ref.ask_stop().await.unwrap();

    let actor_ref = system
        .create_root_actor("full-recover", StandardActor::initial(()))
        .await
        .unwrap();

    let response = actor_ref.ask(StandardMessage::Get).await.unwrap();
    assert_eq!(response, StandardResponse::Counter(18));

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_persistence_actor_keeps_event_history() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("full-history", StandardActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(StandardMessage::Increment(2)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(3)).await.unwrap();

    // The store was started with name "store" and the default prefix
    // derived from the actor's full path, so the backend collections are
    // "store_events" and "store_states" under that prefix.
    let prefix = default_store_prefix(&actor_ref.path());
    let collection =
        manager.create_collection("store_events", &prefix).unwrap();
    let events: Vec<_> = collection
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let state = manager.create_state("store_states", &prefix).unwrap();

    assert_eq!(events.len(), 2, "must keep event history");
    assert!(
        ave_actors_store::database::State::get(&state).is_ok(),
        "must store at least one snapshot"
    );

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_persistence_store_command_returns_last_event() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = store_new!(
        StandardActor,
        "store",
        "full-cmd",
        manager,
        None,
        Arc::new(StandardActorState::default()),
    )
    .unwrap();
    let store_ref: ActorRef<ave_actors_store::store::Store<StandardActor>> =
        system
            .create_root_actor("full-cmd-store", store)
            .await
            .unwrap();

    store_ref
        .ask(StoreCommand::Persist {
            event: Arc::new(StandardEvent(5)),
            state: Arc::new(StandardActorState::default()),
            snapshot_every: None,
        })
        .await
        .unwrap();
    store_ref
        .ask(StoreCommand::Persist {
            event: Arc::new(StandardEvent(3)),
            state: Arc::new(StandardActorState::default()),
            snapshot_every: None,
        })
        .await
        .unwrap();

    let response = store_ref.ask(StoreCommand::LastEvent).await.unwrap();
    assert!(matches!(
        response,
        StoreResponse::LastEvent(Some(event)) if event.0 == 3
    ));

    let response = store_ref
        .ask(StoreCommand::GetEvents { from: 0, to: 1 })
        .await
        .unwrap();
    assert!(matches!(
        response,
        StoreResponse::Events(events) if events.len() == 2
    ));

    let response = store_ref.ask(StoreCommand::Recover).await.unwrap();
    assert!(matches!(
        response,
        StoreResponse::State(Some(state)) if state.counter == 8
    ));
}

// ---------------------------------------------------------------------------
// Actor with snapshot_every = 5 for threshold tests.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct StandardActorEvery5 {
    state: Arc<StandardActorState>,
}

#[async_trait]
impl Actor for StandardActorEvery5 {
    type Message = StandardMessage;
    type Event = StandardEvent;
    type SinkEvent = Self::Event;
    type Response = StandardResponse;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("StandardActorEvery5", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let db: MemoryManager = ctx
            .system()
            .get_helper("db")
            .expect("db helper should be installed");
        self.start_store("store", None, ctx, db, None).await
    }
}

#[async_trait]
impl PersistentActor for StandardActorEvery5 {
    type InitParams = ();
    type State = StandardActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(StandardActorState::default()),
        }
    }

    fn snapshot_every() -> Option<u64> {
        Some(5)
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
impl Handler<Self> for StandardActorEvery5 {
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
async fn test_persistence_actor_snapshot_every_respected() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("full-every2", StandardActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(StandardMessage::Increment(2)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(3)).await.unwrap();

    let prefix = default_store_prefix(&actor_ref.path());
    let state = manager.create_state("store_states", &prefix).unwrap();
    assert!(
        ave_actors_store::database::State::get(&state).is_ok(),
        "snapshot must be created after reaching snapshot_every"
    );

    actor_ref.ask(StandardMessage::Increment(5)).await.unwrap();

    let collection =
        manager.create_collection("store_events", &prefix).unwrap();
    let events: Vec<_> = collection
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        events.len(),
        3,
        "third event must be kept until next snapshot"
    );

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_persistence_actor_no_snapshot_before_due() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("full-every5", StandardActorEvery5::initial(()))
        .await
        .unwrap();

    actor_ref.ask(StandardMessage::Increment(1)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(2)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(3)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(4)).await.unwrap();

    let prefix = default_store_prefix(&actor_ref.path());
    let state = manager.create_state("store_states", &prefix).unwrap();
    assert!(
        ave_actors_store::database::State::get(&state).is_err(),
        "no snapshot must be created before snapshot_every"
    );

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_persistence_actor_snapshot_on_stop() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("full-stop", StandardActorEvery5::initial(()))
        .await
        .unwrap();

    actor_ref.ask(StandardMessage::Increment(1)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(2)).await.unwrap();
    actor_ref.ask(StandardMessage::Increment(3)).await.unwrap();

    actor_ref.ask_stop().await.unwrap();

    let prefix = default_store_prefix(&actor_ref.path());
    let state = manager.create_state("store_states", &prefix).unwrap();
    assert!(
        ave_actors_store::database::State::get(&state).is_ok(),
        "snapshot must be created on actor stop"
    );
}

// Non-persistent parent that hosts a persistent child named "counter".
// Two parents ("p1", "p2") yield children at `/user/p1/counter` and
// `/user/p2/counter`: same leaf, different full paths.
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
        info_span!("BranchParent", id = %id)
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
        match msg {
            StandardMessage::Increment(_) => child.ask(msg).await,
            StandardMessage::Get => child.ask(msg).await,
        }
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

#[test(tokio::test)]
async fn test_same_leaf_name_under_different_parents_is_isolated() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let parent_a = system.create_root_actor("p1", BranchParent).await.unwrap();
    let parent_b = system.create_root_actor("p2", BranchParent).await.unwrap();

    parent_a.ask(StandardMessage::Increment(10)).await.unwrap();
    parent_b.ask(StandardMessage::Increment(100)).await.unwrap();

    // Each subtree keeps its own state: previously both children shared
    // the "counter" prefix and the second write corrupted the first.
    assert_eq!(
        parent_a.ask(StandardMessage::Get).await.unwrap(),
        StandardResponse::Counter(10)
    );
    assert_eq!(
        parent_b.ask(StandardMessage::Get).await.unwrap(),
        StandardResponse::Counter(100)
    );

    // The backend holds two distinct prefixes.
    let prefix_a = default_store_prefix(&ActorPath::from("/user/p1/counter"));
    let prefix_b = default_store_prefix(&ActorPath::from("/user/p2/counter"));
    assert_ne!(prefix_a, prefix_b);
    assert_eq!(
        manager
            .create_collection("store_events", &prefix_a)
            .unwrap()
            .iter(false)
            .unwrap()
            .count(),
        1
    );
    assert_eq!(
        manager
            .create_collection("store_events", &prefix_b)
            .unwrap()
            .iter(false)
            .unwrap()
            .count(),
        1
    );

    parent_a.ask_stop().await.unwrap();
    parent_b.ask_stop().await.unwrap();
}
