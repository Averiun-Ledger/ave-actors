#[macro_use]
mod helpers;
use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, EncryptedKey,
    Error as ActorError, Event, Handler, Message, Response,
};
use ave_actors_store::{
    Error as StoreError, StoreOperation,
    database::{Collection, DbManager, State},
    memory::{MemoryManager, MemoryStore},
    store::{
        FullPersistence, LightPersistence, PersistentActor, Store,
        StoreCommand, StoreResponse,
    },
};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Default, Clone)]
struct FailingStateManager {
    memory: MemoryManager,
}

#[derive(Clone)]
struct FailingStateStore;

impl State for FailingStateStore {
    fn name(&self) -> &str {
        "failing_state"
    }

    fn get(&self) -> Result<Vec<u8>, StoreError> {
        Err(StoreError::EntryNotFound {
            key: "missing".to_owned(),
        })
    }

    fn put(&mut self, _data: &[u8]) -> Result<(), StoreError> {
        Err(StoreError::Store {
            operation: StoreOperation::Test,
            reason: "forced snapshot failure".to_owned(),
            source: None,
        })
    }

    fn del(&mut self) -> Result<(), StoreError> {
        Ok(())
    }

    fn purge(&mut self) -> Result<(), StoreError> {
        Ok(())
    }
}

impl DbManager<MemoryStore, FailingStateStore> for FailingStateManager {
    fn create_collection(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<MemoryStore, StoreError> {
        self.memory.create_collection(name, prefix)
    }

    fn create_state(
        &self,
        _name: &str,
        _prefix: &str,
    ) -> Result<FailingStateStore, StoreError> {
        Ok(FailingStateStore)
    }

    fn stop(self) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(Default, Clone)]
struct RangeCollection {
    data: std::collections::BTreeMap<String, Vec<u8>>,
    fail_iter: bool,
    fail_last: bool,
}

impl Collection for RangeCollection {
    fn last(&self) -> Result<Option<(String, Vec<u8>)>, StoreError> {
        if self.fail_last {
            return Err(StoreError::Store {
                operation: StoreOperation::Test,
                reason: "forced last failure".to_owned(),
                source: None,
            });
        }
        Ok(self
            .data
            .iter()
            .next_back()
            .map(|(k, v)| (k.clone(), v.clone())))
    }

    fn name(&self) -> &str {
        "range_collection"
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        self.data
            .get(key)
            .cloned()
            .ok_or_else(|| StoreError::EntryNotFound {
                key: key.to_owned(),
            })
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), StoreError> {
        self.data.insert(key.to_owned(), data.to_vec());
        Ok(())
    }

    fn del(&mut self, key: &str) -> Result<(), StoreError> {
        self.data.remove(key).map(|_| ()).ok_or_else(|| {
            StoreError::EntryNotFound {
                key: key.to_owned(),
            }
        })
    }

    fn purge(&mut self) -> Result<(), StoreError> {
        self.data.clear();
        Ok(())
    }

    fn iter<'a>(
        &'a self,
        reverse: bool,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(String, Vec<u8>), StoreError>> + 'a>,
        StoreError,
    > {
        if self.fail_iter {
            return Err(StoreError::Store {
                operation: StoreOperation::Test,
                reason: "forced iter failure".to_owned(),
                source: None,
            });
        }

        let items: Vec<_> = if reverse {
            self.data
                .iter()
                .rev()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        } else {
            self.data
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };
        Ok(Box::new(items.into_iter().map(Ok)))
    }
}

#[derive(Default, Clone)]
struct LastErrorManager {
    memory: MemoryManager,
}

impl DbManager<RangeCollection, MemoryStore> for LastErrorManager {
    fn create_collection(
        &self,
        _name: &str,
        _prefix: &str,
    ) -> Result<RangeCollection, StoreError> {
        Ok(RangeCollection {
            fail_last: true,
            ..RangeCollection::default()
        })
    }

    fn create_state(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<MemoryStore, StoreError> {
        self.memory.create_state(name, prefix)
    }

    fn stop(self) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
enum ValueMessage {
    Increment(i32),
    GetValue,
}

impl Message for ValueMessage {}

#[derive(Debug, Clone, PartialEq)]
enum ValueResponse {
    Ack,
    Value(i32),
}

impl Response for ValueResponse {}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
)]
struct ValueEvent(i32);

impl Event for ValueEvent {}

// State structs and actors

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct RollbackLightActorState {
    value: i32,
}

#[derive(Debug)]
struct RollbackLightActor {
    state_ptr: Arc<RollbackLightActorState>,
}

#[async_trait]
impl Actor for RollbackLightActor {
    type Message = ValueMessage;
    type Response = ValueResponse;
    type Event = ValueEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("RollbackLightActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        self.start_store(
            "rollback_light",
            None,
            ctx,
            FailingStateManager::default(),
            None,
        )
        .await
    }
}

#[async_trait]
impl PersistentActor for RollbackLightActor {
    type Persistence = LightPersistence;
    type InitParams = ();
    type State = RollbackLightActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state_ptr: Arc::new(RollbackLightActorState::default()),
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

#[async_trait]
impl Handler<Self> for RollbackLightActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: ValueMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<ValueResponse, ActorError> {
        match msg {
            ValueMessage::Increment(delta) => {
                self.persist(ValueEvent(delta), ctx).await?;
                Ok(ValueResponse::Ack)
            }
            ValueMessage::GetValue => {
                Ok(ValueResponse::Value(self.state_ptr.value))
            }
        }
    }
}

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct GapActorState {
    value: i32,
}

#[derive(Debug)]
struct GapActor {
    state_ptr: Arc<GapActorState>,
}

#[async_trait]
impl Actor for GapActor {
    type Message = ValueMessage;
    type Response = ValueResponse;
    type Event = ValueEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("GapActor", id = %id)
    }
}

#[async_trait]
impl PersistentActor for GapActor {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = GapActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state_ptr: Arc::new(GapActorState::default()),
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

#[async_trait]
impl Handler<Self> for GapActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: ValueMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<ValueResponse, ActorError> {
        match msg {
            ValueMessage::Increment(delta) => {
                let mut new_state = Arc::clone(&self.state_ptr);
                Arc::make_mut(&mut new_state).value += delta;
                self.state_ptr = new_state;
                Ok(ValueResponse::Ack)
            }
            ValueMessage::GetValue => {
                Ok(ValueResponse::Value(self.state_ptr.value))
            }
        }
    }
}

#[test(tokio::test)]
async fn test_persistent_actor_rolls_back_state_when_store_persist_fails() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref: ActorRef<RollbackLightActor> = system
        .create_root_actor("rollback-light", RollbackLightActor::initial(()))
        .await
        .unwrap();

    let err = actor_ref.ask(ValueMessage::Increment(5)).await.unwrap_err();
    assert!(matches!(err, ActorError::StoreOperation { .. }));

    let value = actor_ref.ask(ValueMessage::GetValue).await.unwrap();
    assert_eq!(value, ValueResponse::Value(0));
}

#[test(tokio::test)]
async fn test_light_persistence_rolls_back_snapshot_failure() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = store_new!(
        RollbackLightActor,
        "rollback_store",
        "prefix",
        FailingStateManager::default(),
        None,
        Arc::new(RollbackLightActorState::default()),
    )
    .unwrap();

    let store_ref: ActorRef<Store<RollbackLightActor>> = system
        .create_root_actor("rollback-store", store)
        .await
        .unwrap();

    let response = store_ref
        .ask(StoreCommand::PersistLight(Arc::new(
            RollbackLightActorState { value: 5 },
        )))
        .await;
    assert!(matches!(response, Err(ActorError::StoreOperation { .. })));

    // With LightPersistence as snapshot-only, a snapshot failure leaves nothing
    // persisted. The logical event counter is rolled back as well.
    let counter = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    assert!(matches!(counter, StoreResponse::LastEventNumber(0)));

    let recovered = store_ref.ask(StoreCommand::Recover).await.unwrap();
    assert!(matches!(recovered, StoreResponse::State(None)));
}

#[test(tokio::test)]
async fn test_recover_fails_when_event_log_has_gap() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = store_new!(
        GapActor,
        "gap_store",
        "prefix",
        manager.clone(),
        None,
        Arc::new(GapActorState::default()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<GapActor>> =
        system.create_root_actor("gap-store", store).await.unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::Persist(Arc::new(ValueEvent(1))))
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));
    assert!(matches!(
        store_ref
            .ask(StoreCommand::Persist(Arc::new(ValueEvent(2))))
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    let mut collection = manager
        .create_collection("gap_store_events", "prefix")
        .unwrap();
    Collection::del(&mut collection, "00000000000000000000").unwrap();

    let recovered = store_ref.ask(StoreCommand::Recover).await;
    assert!(matches!(recovered, Err(ActorError::StoreOperation { .. })));
}

#[test]
fn test_memory_store_keeps_prefixes_isolated() {
    let manager = MemoryManager::default();

    let mut state_actor1 = manager.create_state("state", "actor1").unwrap();
    let mut state_actor10 = manager.create_state("state", "actor10").unwrap();
    State::put(&mut state_actor1, b"one").unwrap();
    State::put(&mut state_actor10, b"ten").unwrap();
    State::purge(&mut state_actor1).unwrap();
    assert_eq!(State::get(&state_actor10).unwrap(), b"ten");

    let mut coll_actor1 =
        manager.create_collection("events", "actor1").unwrap();
    let mut coll_actor10 =
        manager.create_collection("events", "actor10").unwrap();
    Collection::put(&mut coll_actor1, "0001", b"one").unwrap();
    Collection::put(&mut coll_actor10, "0001", b"ten").unwrap();

    let actor1_items: Vec<_> = coll_actor1
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(actor1_items, vec![("0001".to_owned(), b"one".to_vec())]);

    Collection::purge(&mut coll_actor1).unwrap();
    assert_eq!(Collection::get(&coll_actor10, "0001").unwrap(), b"ten");
}

#[test]
fn test_get_by_range_reports_requested_missing_key() {
    let mut collection = RangeCollection::default();
    collection.put("a", b"1").unwrap();
    collection.put("b", b"2").unwrap();

    let result = collection.get_by_range(Some("missing"), 1);
    assert_eq!(
        result,
        Err(StoreError::EntryNotFound {
            key: "missing".to_owned(),
        })
    );
}

#[test]
fn test_get_by_range_propagates_iter_initialization_error() {
    let collection = RangeCollection {
        fail_iter: true,
        ..RangeCollection::default()
    };

    let result = collection.get_by_range(None, 1);
    assert!(matches!(
        result,
        Err(StoreError::Store {
            operation: StoreOperation::Test,
            ..
        })
    ));
}

#[test]
fn test_store_new_propagates_collection_last_error() {
    let result = store_new!(
        GapActor,
        "last_error_store",
        "prefix",
        LastErrorManager::default(),
        None,
        Arc::new(GapActorState::default()),
    );

    assert!(matches!(
        result,
        Err(StoreError::Store {
            operation: StoreOperation::Test,
            ..
        })
    ));
}

#[test(tokio::test)]
async fn test_recover_falls_back_when_metadata_state_is_missing() {
    let manager = MemoryManager::default();

    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = store_new!(
        GapActor,
        "metadata_fallback_store",
        "prefix",
        manager.clone(),
        None,
        Arc::new(GapActorState::default()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<GapActor>> = system
        .create_root_actor("metadata-fallback-store", store)
        .await
        .unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: Arc::new(ValueEvent(4)),
                state: Arc::new(GapActorState { value: 4 }),
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    let mut metadata = manager
        .create_state("metadata_fallback_store_metadata", "prefix")
        .unwrap();
    State::purge(&mut metadata).unwrap();

    let (system2, mut runner2) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner2.run().await });

    let restarted = store_new!(
        GapActor,
        "metadata_fallback_store",
        "prefix",
        manager,
        None,
        Arc::new(GapActorState::default()),
    )
    .unwrap();
    let restarted_ref: ActorRef<Store<GapActor>> = system2
        .create_root_actor("metadata-fallback-store-2", restarted)
        .await
        .unwrap();

    match restarted_ref.ask(StoreCommand::Recover).await.unwrap() {
        StoreResponse::State(Some(state)) => assert_eq!(state.value, 4),
        _ => panic!("expected recovery via snapshot fallback without metadata"),
    }
}

#[test(tokio::test)]
async fn test_recover_fails_when_encrypted_pending_event_is_corrupted() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let encrypt_key = EncryptedKey::new(&[9u8; 32]).unwrap();
    let store = store_new!(
        GapActor,
        "encrypted_gap_store",
        "prefix",
        manager.clone(),
        Some(encrypt_key),
        Arc::new(GapActorState::default()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<GapActor>> = system
        .create_root_actor("encrypted-gap-store", store)
        .await
        .unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: Arc::new(ValueEvent(2)),
                state: Arc::new(GapActorState { value: 2 }),
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));
    assert!(matches!(
        store_ref
            .ask(StoreCommand::Persist(Arc::new(ValueEvent(3))))
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    let mut collection = manager
        .create_collection("encrypted_gap_store_events", "prefix")
        .unwrap();
    Collection::put(&mut collection, "00000000000000000001", b"broken")
        .unwrap();

    let recovered = store_ref.ask(StoreCommand::Recover).await;
    assert!(matches!(recovered, Err(ActorError::StoreOperation { .. })));
}

#[test(tokio::test)]
async fn test_persist_full_event_requests_snapshot_only_when_due() {
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = store_new!(
        GapActor,
        "persist_full_event_store",
        "prefix",
        manager,
        None,
        Arc::new(GapActorState::default()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<GapActor>> = system
        .create_root_actor("persist-full-event-store", store)
        .await
        .unwrap();

    // First event: no snapshot yet (event_counter will be 1, not multiple of 2)
    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: Arc::new(ValueEvent(2)),
                state: Arc::new(GapActorState { value: 2 }),
                snapshot_every: Some(2),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    // Second event: snapshot is triggered inline (event_counter = 2, multiple of 2)
    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: Arc::new(ValueEvent(3)),
                state: Arc::new(GapActorState { value: 5 }),
                snapshot_every: Some(2),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    // Verify snapshot happened by recovering
    let recovered = store_ref.ask(StoreCommand::Recover).await.unwrap();
    match recovered {
        StoreResponse::State(Some(state)) => {
            assert_eq!(
                state.value, 5,
                "snapshot should have been created inline"
            );
        }
        _ => panic!("expected recovered state after inline snapshot"),
    }
}

#[test]
fn test_store_new_fails_when_last_event_key_is_corrupted() {
    let manager = MemoryManager::default();
    let mut collection = manager
        .create_collection("corrupted_key_store_events", "prefix")
        .unwrap();
    Collection::put(&mut collection, "not-a-number", b"broken").unwrap();

    let result = store_new!(
        GapActor,
        "corrupted_key_store",
        "prefix",
        manager,
        None,
        Arc::new(GapActorState::default()),
    );

    assert!(matches!(
        result,
        Err(StoreError::Store {
            operation: StoreOperation::ParseEventKey,
            ..
        })
    ));
}
