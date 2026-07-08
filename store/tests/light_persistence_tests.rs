//! Integration tests for `LightPersistence`.
//!
//! `LightPersistence` stores only the latest state snapshot and keeps no event
//! history. These tests verify that behaviour end-to-end.

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
    store::{LightPersistence, PersistentActor, StoreCommand, StoreResponse},
};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct LightActorState {
    counter: i32,
    numbers: Vec<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LightMessage {
    Increment(i32),
    AddNumber(i32),
    Get,
    GetNumbers,
}

impl Message for LightMessage {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct LightEvent {
    delta: i32,
    number: Option<i32>,
}

impl Event for LightEvent {}

#[derive(Debug, Clone, PartialEq)]
enum LightResponse {
    Counter(i32),
    Numbers(Vec<i32>),
}

impl Response for LightResponse {}

#[derive(Debug)]
struct LightActor {
    state: Arc<LightActorState>,
}

#[async_trait]
impl Actor for LightActor {
    type Message = LightMessage;
    type Event = LightEvent;
    type SinkEvent = Self::Event;
    type Response = LightResponse;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("LightActor", id = %id)
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
impl PersistentActor for LightActor {
    type Persistence = LightPersistence;
    type InitParams = ();
    type State = LightActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(LightActorState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut new_state = Arc::clone(&state);
        let inner = Arc::make_mut(&mut new_state);
        inner.counter += event.delta;
        if let Some(n) = event.number {
            inner.numbers.push(n);
        }
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
                self.persist(
                    LightEvent {
                        delta,
                        number: None,
                    },
                    ctx,
                )
                .await?;
                Ok(LightResponse::Counter(self.state.counter))
            }
            LightMessage::AddNumber(n) => {
                self.persist(
                    LightEvent {
                        delta: 0,
                        number: Some(n),
                    },
                    ctx,
                )
                .await?;
                Ok(LightResponse::Numbers(self.state.numbers.clone()))
            }
            LightMessage::Get => Ok(LightResponse::Counter(self.state.counter)),
            LightMessage::GetNumbers => {
                Ok(LightResponse::Numbers(self.state.numbers.clone()))
            }
        }
    }
}

#[test(tokio::test)]
async fn test_light_persistence_actor_recovers_state() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", MemoryManager::default());

    let actor_ref = system
        .create_root_actor("light-recover", LightActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(LightMessage::Increment(10)).await.unwrap();
    actor_ref.ask(LightMessage::Increment(5)).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let response = actor_ref.ask(LightMessage::Get).await.unwrap();
    assert_eq!(response, LightResponse::Counter(15));

    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let actor_ref = system
        .create_root_actor("light-recover", LightActor::initial(()))
        .await
        .unwrap();

    let response = actor_ref.ask(LightMessage::Get).await.unwrap();
    assert_eq!(response, LightResponse::Counter(15));

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_light_persistence_actor_no_event_replay() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", MemoryManager::default());

    let actor_ref = system
        .create_root_actor("light-no-replay", LightActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(LightMessage::AddNumber(3)).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let actor_ref = system
        .create_root_actor("light-no-replay", LightActor::initial(()))
        .await
        .unwrap();

    // Verify the recovered state has the original number and no duplicate
    // produced by event replay.
    let response = actor_ref.ask(LightMessage::GetNumbers).await.unwrap();
    assert_eq!(response, LightResponse::Numbers(vec![3]));

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_light_persistence_does_not_store_events() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    system.add_helper("db", manager.clone());

    let actor_ref = system
        .create_root_actor("light-no-events", LightActor::initial(()))
        .await
        .unwrap();

    actor_ref.ask(LightMessage::Increment(7)).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // The store was started with name "store" and prefix "light-no-events",
    // so the event collection is "store_events" under that prefix.
    let collection = manager
        .create_collection("store_events", "light-no-events")
        .unwrap();
    assert!(
        collection.iter(false).unwrap().next().is_none(),
        "LightPersistence must not persist events"
    );

    actor_ref.ask_stop().await.unwrap();
}

#[test(tokio::test)]
async fn test_light_persistence_store_command_has_no_events() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = store_new!(
        LightActor,
        "store",
        "light-cmd",
        manager,
        None,
        Arc::new(LightActorState::default()),
    )
    .unwrap();
    let store_ref: ActorRef<ave_actors_store::store::Store<LightActor>> =
        system
            .create_root_actor("light-cmd-store", store)
            .await
            .unwrap();

    store_ref
        .ask(StoreCommand::PersistLight(Arc::new(LightActorState {
            counter: 42,
            numbers: vec![],
        })))
        .await
        .unwrap();

    let response = store_ref.ask(StoreCommand::LastEvent).await.unwrap();
    assert!(matches!(response, StoreResponse::LastEvent(None)));

    let response = store_ref
        .ask(StoreCommand::GetEvents { from: 0, to: 0 })
        .await
        .unwrap();
    assert!(
        matches!(response, StoreResponse::Events(events) if events.is_empty())
    );

    let response = store_ref.ask(StoreCommand::Recover).await.unwrap();
    assert!(matches!(
        response,
        StoreResponse::State(Some(state)) if state.counter == 42
    ));
}
