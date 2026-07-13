//! In-memory state invariant for `PersistentActor::persist`.
//!
//! `apply` is a pure function that returns the next state, and the in-memory
//! state is only replaced after the event is durably persisted. This test pins
//! the guarantee that a failing `apply` (and, by extension, any persistence
//! failure) leaves the actor state untouched.

use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error as ActorError, Event,
    Handler, Message, Response,
};
use ave_actors_store::{
    memory::MemoryManager,
    store::{FullPersistence, PersistentActor},
};
use test_log::test;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(
    Debug, Clone, Default, borsh::BorshSerialize, borsh::BorshDeserialize,
)]
struct GuardState {
    value: i32,
}

#[derive(Debug)]
struct GuardActor {
    state_ptr: Arc<GuardState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum GuardMessage {
    Apply(i32),
    GetState,
}

impl Message for GuardMessage {}

#[derive(Debug, Clone, PartialEq)]
enum GuardResponse {
    Applied,
    ApplyFailed,
    State(i32),
}

impl Response for GuardResponse {}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
struct GuardEvent {
    delta: i32,
}

impl Event for GuardEvent {}

#[async_trait]
impl Actor for GuardActor {
    type Message = GuardMessage;
    type Response = GuardResponse;
    type Event = GuardEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("GuardActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        self.start_store(
            "guard_test",
            None,
            ctx,
            MemoryManager::default(),
            None,
        )
        .await
    }
}

#[async_trait]
impl Handler<Self> for GuardActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: GuardMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<GuardResponse, ActorError> {
        match msg {
            GuardMessage::Apply(delta) => {
                match self.persist(GuardEvent { delta }, ctx).await {
                    Ok(()) => Ok(GuardResponse::Applied),
                    Err(_) => Ok(GuardResponse::ApplyFailed),
                }
            }
            GuardMessage::GetState => {
                Ok(GuardResponse::State(self.state_ptr.value))
            }
        }
    }
}

#[async_trait]
impl PersistentActor for GuardActor {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = GuardState;

    fn create_initial(_: ()) -> Self {
        Self {
            state_ptr: Arc::new(GuardState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        if event.delta < 0 {
            return Err(ActorError::Functional {
                description: "negative delta rejected".to_owned(),
            });
        }
        let mut new_state = state;
        Arc::make_mut(&mut new_state).value += event.delta;
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
async fn test_state_unchanged_when_apply_fails() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("guard", GuardActor::initial(()))
        .await
        .unwrap();

    // A successful persist advances the state.
    let result = actor_ref.ask(GuardMessage::Apply(5)).await.unwrap();
    assert!(matches!(result, GuardResponse::Applied));

    // A failing `apply` must leave the in-memory state untouched.
    let result = actor_ref.ask(GuardMessage::Apply(-1)).await.unwrap();
    assert!(matches!(result, GuardResponse::ApplyFailed));

    let result = actor_ref.ask(GuardMessage::GetState).await.unwrap();
    assert!(
        matches!(result, GuardResponse::State(5)),
        "state must stay at 5 after a failed apply, got {result:?}"
    );

    // The actor keeps working after the failure.
    let result = actor_ref.ask(GuardMessage::Apply(3)).await.unwrap();
    assert!(matches!(result, GuardResponse::Applied));

    let result = actor_ref.ask(GuardMessage::GetState).await.unwrap();
    assert!(matches!(result, GuardResponse::State(8)));
}
