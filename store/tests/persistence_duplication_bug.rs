//! Regression test for persistence state duplication on restart.
//!
//! Guarantees that an actor does not re-apply already-applied
//! events when recovering from a snapshot after a restart.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error as ActorError, Event,
    Handler, Message, Response,
};
use ave_actors_store::memory::MemoryManager;
use ave_actors_store::store::PersistentActor;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};
use test_log::test;
use tokio::sync::Mutex as TokioMutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// Shared manager for testing
static SHARED_MANAGER: OnceLock<Arc<TokioMutex<MemoryManager>>> =
    OnceLock::new();

// State struct
#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct VectorActorStandardState {
    numbers: Vec<i32>,
}

// Actor with a vector that accumulates numbers (version)
#[derive(Debug)]
struct VectorActor {
    state_ptr: Arc<VectorActorStandardState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum VectorMessage {
    Add(i32),
    Get,
}
impl Message for VectorMessage {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VectorResponse {
    numbers: Vec<i32>,
}
impl Response for VectorResponse {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct NumberAdded(i32);
impl Event for NumberAdded {}

#[async_trait]
impl Actor for VectorActor {
    type Message = VectorMessage;
    type Response = VectorResponse;
    type Event = NumberAdded;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("VectorActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let manager_ref = SHARED_MANAGER.get_or_init(|| {
            Arc::new(TokioMutex::new(MemoryManager::default()))
        });

        let manager = manager_ref.lock().await.clone();

        self.start_store("vector_test", None, ctx, manager, None)
            .await
    }
}

#[async_trait]
impl Handler<Self> for VectorActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: VectorMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<VectorResponse, ActorError> {
        match msg {
            VectorMessage::Add(number) => {
                self.persist(NumberAdded(number), ctx).await?;
                Ok(VectorResponse {
                    numbers: self.state_ptr.numbers.clone(),
                })
            }
            VectorMessage::Get => Ok(VectorResponse {
                numbers: self.state_ptr.numbers.clone(),
            }),
        }
    }
}

#[async_trait]
impl PersistentActor for VectorActor {
    type InitParams = ();
    type State = VectorActorStandardState;

    fn create_initial(_params: ()) -> Self {
        Self {
            state_ptr: Arc::new(VectorActorStandardState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut new_state = state;
        Arc::make_mut(&mut new_state).numbers.push(event.0);
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
async fn test_persistence_duplication_on_restart() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("vector_actor", VectorActor::initial(()))
        .await
        .unwrap();

    // Add number 5
    let response = actor_ref.ask(VectorMessage::Add(5)).await.unwrap();

    assert_eq!(response.numbers, vec![5], "Should have [5] after adding 5");

    // Stop the actor (it will create snapshot on stop if there are events)
    actor_ref.ask_stop().await.unwrap();

    // Restart
    let actor_ref2 = system
        .create_root_actor("vector_actor", VectorActor::initial(()))
        .await
        .unwrap();

    let response = actor_ref2.ask(VectorMessage::Get).await.unwrap();

    assert_eq!(
        response.numbers,
        vec![5],
        "Should have [5] after restart, but has {:?}",
        response.numbers
    );
}
