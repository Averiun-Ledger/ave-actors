//! Recovery against a corrupt backend.
//!
//! Injects garbage directly into the backend (bypassing the actor) and
//! verifies `pre_start` fails loudly instead of hanging, panicking, or
//! recovering fiction. Also verifies the system stays usable afterwards.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, EncryptedKey,
    Error as ActorError, Event, Handler, Message, Response, ShutdownReason,
    SystemRef,
};
use ave_actors_store::{
    database::{Collection, DbManager, State},
    default_store_prefix,
    memory::MemoryManager,
    store::PersistentActor,
};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct CorruptState {
    value: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CorruptMsg(i32);

impl Message for CorruptMsg {}

#[derive(Debug, Clone, PartialEq)]
struct CorruptResp(i32);

impl Response for CorruptResp {}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct CorruptEvent(i32);

impl Event for CorruptEvent {}

#[derive(Debug)]
struct CorruptActor {
    state: Arc<CorruptState>,
}

#[async_trait]
impl Actor for CorruptActor {
    type Message = CorruptMsg;
    type Response = CorruptResp;
    type Event = CorruptEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("CorruptActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let db: MemoryManager = ctx
            .system()
            .get_helper("db")
            .expect("db helper should be installed");
        // Optional encryption key shared by tests through a helper.
        let key: Option<EncryptedKey> = ctx.system().get_helper("key");
        self.start_store("store", None, ctx, db, key).await
    }
}

#[async_trait]
impl PersistentActor for CorruptActor {
    type InitParams = ();
    type State = CorruptState;

    fn create_initial(_: ()) -> Self {
        Self {
            state: Arc::new(CorruptState::default()),
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
impl Handler<Self> for CorruptActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: CorruptMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<CorruptResp, ActorError> {
        self.persist(CorruptEvent(msg.0), ctx).await?;
        Ok(CorruptResp(self.state.value))
    }
}

fn test_system(
    manager: MemoryManager,
) -> (SystemRef, tokio::task::JoinHandle<ShutdownReason>) {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    system.add_helper("db", manager);
    let handle = tokio::spawn(async move { runner.run().await });
    (system, handle)
}

fn prefix_for(name: &str) -> String {
    default_store_prefix(&ActorPath::from(format!("/user/{name}").as_str()))
}

#[test(tokio::test)]
async fn test_garbage_event_bytes_fail_recovery_loudly() {
    let manager = MemoryManager::default();
    let prefix = prefix_for("corrupt-event");
    let mut events =
        manager.create_collection("store_events", &prefix).unwrap();
    Collection::put(&mut events, &format!("{:020}", 0), b"not-borsh!!")
        .unwrap();

    let (system, _runner) = test_system(manager);
    let result = system
        .create_root_actor("corrupt-event", CorruptActor::initial(()))
        .await;
    assert!(
        result.is_err(),
        "garbage event bytes must fail pre_start, not recover fiction"
    );

    // The system stays usable: a clean actor starts fine.
    let clean = CorruptActor::initial(());
    assert!(system.create_root_actor("clean", clean).await.is_ok());
    system.stop_system();
}

#[test(tokio::test)]
async fn test_garbage_snapshot_bytes_fail_startup() {
    let manager = MemoryManager::default();
    let prefix = prefix_for("corrupt-snap");
    let mut states = manager.create_state("store_states", &prefix).unwrap();
    ave_actors_store::database::State::put(&mut states, b"junk-bytes").unwrap();

    let (system, _runner) = test_system(manager);
    let result = system
        .create_root_actor("corrupt-snap", CorruptActor::initial(()))
        .await;
    assert!(
        result.is_err(),
        "garbage snapshot bytes must fail pre_start"
    );
    system.stop_system();
}

#[test(tokio::test)]
async fn test_truncated_ciphertext_fails_with_validation_error() {
    let manager = MemoryManager::default();
    let prefix = prefix_for("corrupt-short");
    let mut states = manager.create_state("store_states", &prefix).unwrap();
    // Below nonce+tag minimum (40 bytes): must be rejected as invalid
    // ciphertext, never decrypted.
    ave_actors_store::database::State::put(&mut states, b"short").unwrap();

    let (system, _runner) = test_system(manager);
    system.add_helper("key", EncryptedKey::new(&[7u8; 32]).unwrap());
    let result = system
        .create_root_actor("corrupt-short", CorruptActor::initial(()))
        .await;
    assert!(result.is_err(), "truncated ciphertext must fail pre_start");
    system.stop_system();
}

#[test(tokio::test)]
async fn test_wrong_key_fails_recovery_loudly() {
    let manager = MemoryManager::default();
    let prefix = prefix_for("corrupt-key");
    let mut states = manager.create_state("store_states", &prefix).unwrap();
    // Random bytes longer than nonce+tag: decryption with any key fails
    // authentication instead of producing fiction.
    ave_actors_store::database::State::put(&mut states, &[0xABu8; 64]).unwrap();

    let (system, _runner) = test_system(manager);
    system.add_helper("key", EncryptedKey::new(&[9u8; 32]).unwrap());
    let result = system
        .create_root_actor("corrupt-key", CorruptActor::initial(()))
        .await;
    assert!(
        result.is_err(),
        "unauthentic ciphertext must fail pre_start"
    );
    system.stop_system();
}

#[test(tokio::test)]
async fn test_garbage_metadata_reports_decode_metadata() {
    let manager = MemoryManager::default();
    let prefix = prefix_for("corrupt-meta");
    let mut metadata = manager.create_state("store_metadata", &prefix).unwrap();
    State::put(&mut metadata, b"junk-bytes").unwrap();

    let (system, _runner) = test_system(manager);
    let result = system
        .create_root_actor("corrupt-meta", CorruptActor::initial(()))
        .await;
    match result {
        Err(ActorError::StoreOperation { operation, reason }) => {
            assert_eq!(
                operation, "store_init",
                "init must report its own operation"
            );
            assert!(
                reason.contains("decode_metadata"),
                "the metadata cause must survive wrapping, got: {reason}"
            );
        }
        other => panic!("expected StoreOperation error, got {other:?}"),
    }
    system.stop_system();
}
