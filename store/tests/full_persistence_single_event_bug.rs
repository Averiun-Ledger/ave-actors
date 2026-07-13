//! Regression tests for FullPersistence recovery of a single event.
//!
//! Guarantees that a brand-new actor that persists exactly one event and is
//! then stopped gracefully recovers that event on restart, and that the
//! store's event counter increments after the first persist.

#[macro_use]
mod helpers;
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

static SHARED_MGR: OnceLock<Arc<TokioMutex<MemoryManager>>> = OnceLock::new();

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct SingleEventActorState {
    data: String,
}

#[derive(Debug)]
struct SingleEventActor {
    state_ptr: Arc<SingleEventActorState>,
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
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

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
        let manager_ref = SHARED_MGR.get_or_init(|| {
            Arc::new(TokioMutex::new(MemoryManager::default()))
        });
        let manager = manager_ref.lock().await.clone();
        self.start_store("single_event_test", None, ctx, manager, None)
            .await
    }
}

#[async_trait]
impl Handler<Self> for SingleEventActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: Msg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<Resp, ActorError> {
        match msg {
            Msg::SetData(new_data) => {
                self.persist(DataSet(new_data), ctx).await?;
                Ok(Resp {
                    data: self.state_ptr.data.clone(),
                })
            }
            Msg::GetData => Ok(Resp {
                data: self.state_ptr.data.clone(),
            }),
        }
    }
}

#[async_trait]
impl PersistentActor for SingleEventActor {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = SingleEventActorState;

    fn create_initial(_params: ()) -> Self {
        Self {
            state_ptr: Arc::new(SingleEventActorState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut new_state = state;
        Arc::make_mut(&mut new_state).data.clone_from(&event.0);
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
async fn test_single_event_no_recovery() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("my_actor", SingleEventActor::initial(()))
        .await
        .unwrap();

    let resp = actor_ref
        .ask(Msg::SetData("Hello World".to_string()))
        .await
        .unwrap();
    assert_eq!(resp.data, "Hello World");

    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    let actor_ref2 = system
        .create_root_actor("my_actor", SingleEventActor::initial(()))
        .await
        .unwrap();

    let resp = actor_ref2.ask(Msg::GetData).await.unwrap();

    assert_eq!(
        resp.data, "Hello World",
        "Should recover data after graceful stop"
    );
}

#[test(tokio::test)]
async fn test_debug_event_counter_after_first_event() {
    use ave_actors_store::store::{StoreCommand, StoreResponse};

    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();

    let store = store_new!(
        SingleEventActor,
        "test",
        "debug_counter",
        memory_manager.clone(),
        None,
        Arc::new(SingleEventActorState::default()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        assert_eq!(count, 0);
    }

    store_ref
        .ask(StoreCommand::Persist(Arc::new(DataSet("test".to_string()))))
        .await
        .unwrap();

    // The counter must reflect the persisted event so that a later stop
    // snapshots the pending state.
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    if let StoreResponse::LastEventNumber(count) = result {
        assert_eq!(
            count, 1,
            "event_counter should be 1 after persisting 1 event"
        );
    }
}
