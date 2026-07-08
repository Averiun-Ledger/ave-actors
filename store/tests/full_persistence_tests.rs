//! Integration tests for `FullPersistence`.
//!
//! `FullPersistence` stores the event stream and snapshots periodically. These
//! tests verify that behaviour end-to-end.

#[macro_use]
mod helpers;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, Error as ActorError,
    Event, Handler, Message, Response,
};
use ave_actors_store::{
    database::{Collection, DbManager},
    memory::MemoryManager,
    store::{FullPersistence, PersistentActor, StoreCommand, StoreResponse},
};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct FullActorState {
    counter: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum FullMessage {
    Increment(i32),
    Get,
}

impl Message for FullMessage {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct FullEvent(i32);

impl Event for FullEvent {}

#[derive(Debug, Clone, PartialEq)]
enum FullResponse {
    Counter(i32),
}

impl Response for FullResponse {}

#[derive(Debug)]
struct FullActor {
    state: Arc<FullActorState>,
}

#[async_trait]
impl Actor for FullActor {
    type Message = FullMessage;
    type Event = FullEvent;
    type SinkEvent = Self::Event;
    type Response = FullResponse;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("FullActor", id = %id)
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
impl PersistentActor for FullActor {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = FullActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(FullActorState::default()),
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

#[test(tokio::test)]
async fn test_full_persistence_actor_recovers_from_snapshot_and_events() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", MemoryManager::default());

    let actor_ref = system
        .create_root_actor("full-recover", FullActor::initial(()))
        .await
        .unwrap();

    // snapshot_every = 2, so after 3 events: snapshot at 2 events, 1 pending.
    actor_ref.ask(FullMessage::Increment(10)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(5)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(3)).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let response = actor_ref.ask(FullMessage::Get).await.unwrap();
    assert_eq!(response, FullResponse::Counter(18));

    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let actor_ref = system
        .create_root_actor("full-recover", FullActor::initial(()))
        .await
        .unwrap();

    let response = actor_ref.ask(FullMessage::Get).await.unwrap();
    assert_eq!(response, FullResponse::Counter(18));

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_full_persistence_actor_keeps_event_history() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("full-history", FullActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(FullMessage::Increment(2)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(3)).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // The store was started with name "store" and prefix equal to the actor
    // name ("full-history"), so the backend collections are "store_events" and
    // "store_states" under the "full-history" prefix.
    let collection = manager
        .create_collection("store_events", "full-history")
        .unwrap();
    let events: Vec<_> = collection
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let state = manager
        .create_state("store_states", "full-history")
        .unwrap();

    assert_eq!(events.len(), 2, "FullPersistence must keep event history");
    assert!(
        ave_actors_store::database::State::get(&state).is_ok(),
        "FullPersistence must store at least one snapshot"
    );

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_full_persistence_store_command_returns_last_event() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = store_new!(
        FullActor,
        "store",
        "full-cmd",
        manager,
        None,
        Arc::new(FullActorState::default()),
    )
    .unwrap();
    let store_ref: ActorRef<ave_actors_store::store::Store<FullActor>> = system
        .create_root_actor("full-cmd-store", store)
        .await
        .unwrap();

    store_ref
        .ask(StoreCommand::Persist(Arc::new(FullEvent(5))))
        .await
        .unwrap();
    store_ref
        .ask(StoreCommand::Persist(Arc::new(FullEvent(3))))
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
struct FullActorEvery5 {
    state: Arc<FullActorState>,
}

#[async_trait]
impl Actor for FullActorEvery5 {
    type Message = FullMessage;
    type Event = FullEvent;
    type SinkEvent = Self::Event;
    type Response = FullResponse;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("FullActorEvery5", id = %id)
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
impl PersistentActor for FullActorEvery5 {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = FullActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(FullActorState::default()),
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
impl Handler<Self> for FullActorEvery5 {
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

#[test(tokio::test)]
async fn test_full_persistence_actor_snapshot_every_respected() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("full-every2", FullActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(FullMessage::Increment(2)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(3)).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let state = manager.create_state("store_states", "full-every2").unwrap();
    assert!(
        ave_actors_store::database::State::get(&state).is_ok(),
        "snapshot must be created after reaching snapshot_every"
    );

    actor_ref.ask(FullMessage::Increment(5)).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let collection = manager
        .create_collection("store_events", "full-every2")
        .unwrap();
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
async fn test_full_persistence_actor_no_snapshot_before_due() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("full-every5", FullActorEvery5::initial(()))
        .await
        .unwrap();

    actor_ref.ask(FullMessage::Increment(1)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(2)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(3)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(4)).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let state = manager.create_state("store_states", "full-every5").unwrap();
    assert!(
        ave_actors_store::database::State::get(&state).is_err(),
        "no snapshot must be created before snapshot_every"
    );

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_full_persistence_actor_snapshot_on_stop() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("full-stop", FullActorEvery5::initial(()))
        .await
        .unwrap();

    actor_ref.ask(FullMessage::Increment(1)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(2)).await.unwrap();
    actor_ref.ask(FullMessage::Increment(3)).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let state = manager.create_state("store_states", "full-stop").unwrap();
    assert!(
        ave_actors_store::database::State::get(&state).is_ok(),
        "snapshot must be created on actor stop"
    );
}
