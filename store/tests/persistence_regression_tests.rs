use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, EncryptedKey,
    Error as ActorError, Event, Handler, Message, Response,
    build_tracing_subscriber,
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
use std::sync::{Arc, Mutex};
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

    fn stop(&mut self) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(Default, Clone)]
struct FailingCompactionManager {
    memory: MemoryManager,
}

#[derive(Clone)]
struct FailingCompactionCollection {
    inner: MemoryStore,
}

impl Collection for FailingCompactionCollection {
    fn last(&self) -> Result<Option<(String, Vec<u8>)>, StoreError> {
        Collection::last(&self.inner)
    }

    fn name(&self) -> &str {
        Collection::name(&self.inner)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        Collection::get(&self.inner, key)
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), StoreError> {
        Collection::put(&mut self.inner, key, data)
    }

    fn del(&mut self, _key: &str) -> Result<(), StoreError> {
        Err(StoreError::Store {
            operation: StoreOperation::Test,
            reason: "forced compaction failure".to_owned(),
        })
    }

    fn purge(&mut self) -> Result<(), StoreError> {
        Collection::purge(&mut self.inner)
    }

    fn iter<'a>(
        &'a self,
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = (String, Vec<u8>)> + 'a>, StoreError> {
        self.inner.iter(reverse)
    }
}

impl DbManager<FailingCompactionCollection, MemoryStore>
    for FailingCompactionManager
{
    fn create_collection(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<FailingCompactionCollection, StoreError> {
        Ok(FailingCompactionCollection {
            inner: self.memory.create_collection(name, prefix)?,
        })
    }

    fn create_state(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<MemoryStore, StoreError> {
        self.memory.create_state(name, prefix)
    }

    fn stop(&mut self) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(Clone)]
struct LoggingCollection {
    inner: MemoryStore,
    deleted_keys: Arc<Mutex<Vec<String>>>,
}

impl Collection for LoggingCollection {
    fn last(&self) -> Result<Option<(String, Vec<u8>)>, StoreError> {
        Collection::last(&self.inner)
    }

    fn name(&self) -> &str {
        Collection::name(&self.inner)
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, StoreError> {
        Collection::get(&self.inner, key)
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), StoreError> {
        Collection::put(&mut self.inner, key, data)
    }

    fn del(&mut self, key: &str) -> Result<(), StoreError> {
        self.deleted_keys.lock().unwrap().push(key.to_owned());
        Collection::del(&mut self.inner, key)
    }

    fn purge(&mut self) -> Result<(), StoreError> {
        Collection::purge(&mut self.inner)
    }

    fn iter<'a>(
        &'a self,
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = (String, Vec<u8>)> + 'a>, StoreError> {
        Collection::iter(&self.inner, reverse)
    }
}

#[derive(Clone)]
struct LoggingCompactionManager {
    memory: MemoryManager,
    deleted_keys: Arc<Mutex<Vec<String>>>,
}

impl Default for LoggingCompactionManager {
    fn default() -> Self {
        Self {
            memory: MemoryManager::default(),
            deleted_keys: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl DbManager<LoggingCollection, MemoryStore> for LoggingCompactionManager {
    fn create_collection(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<LoggingCollection, StoreError> {
        Ok(LoggingCollection {
            inner: self.memory.create_collection(name, prefix)?,
            deleted_keys: self.deleted_keys.clone(),
        })
    }

    fn create_state(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<MemoryStore, StoreError> {
        self.memory.create_state(name, prefix)
    }

    fn stop(&mut self) -> Result<(), StoreError> {
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
        self.data
            .remove(key)
            .map(|_| ())
            .ok_or_else(|| StoreError::EntryNotFound {
                key: key.to_owned(),
            })
    }

    fn purge(&mut self) -> Result<(), StoreError> {
        self.data.clear();
        Ok(())
    }

    fn iter<'a>(
        &'a self,
        reverse: bool,
    ) -> Result<Box<dyn Iterator<Item = (String, Vec<u8>)> + 'a>, StoreError> {
        if self.fail_iter {
            return Err(StoreError::Store {
                operation: StoreOperation::Test,
                reason: "forced iter failure".to_owned(),
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
        Ok(Box::new(items.into_iter()))
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

    fn stop(&mut self) -> Result<(), StoreError> {
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

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Default,
)]
struct RollbackLightActor {
    value: i32,
}

#[async_trait]
impl Actor for RollbackLightActor {
    type Message = ValueMessage;
    type Response = ValueResponse;
    type Event = ValueEvent;

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

    fn create_initial(_: ()) -> Self {
        Self { value: 0 }
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.value += event.0;
        Ok(())
    }
}

#[async_trait]
impl Handler<RollbackLightActor> for RollbackLightActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: ValueMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<ValueResponse, ActorError> {
        match msg {
            ValueMessage::Increment(delta) => {
                self.persist(&ValueEvent(delta), ctx).await?;
                Ok(ValueResponse::Ack)
            }
            ValueMessage::GetValue => Ok(ValueResponse::Value(self.value)),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Default,
)]
struct GapActor {
    value: i32,
}

#[async_trait]
impl Actor for GapActor {
    type Message = ValueMessage;
    type Response = ValueResponse;
    type Event = ValueEvent;

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

    fn create_initial(_: ()) -> Self {
        Self { value: 0 }
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.value += event.0;
        Ok(())
    }
}

#[async_trait]
impl Handler<GapActor> for GapActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: ValueMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<ValueResponse, ActorError> {
        match msg {
            ValueMessage::Increment(delta) => {
                self.value += delta;
                Ok(ValueResponse::Ack)
            }
            ValueMessage::GetValue => Ok(ValueResponse::Value(self.value)),
        }
    }
}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    Default,
)]
struct CompactingActor {
    value: i32,
}

#[async_trait]
impl Actor for CompactingActor {
    type Message = ValueMessage;
    type Response = ValueResponse;
    type Event = ValueEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("CompactingActor", id = %id)
    }
}

#[async_trait]
impl PersistentActor for CompactingActor {
    type Persistence = FullPersistence;
    type InitParams = ();

    fn create_initial(_: ()) -> Self {
        Self { value: 0 }
    }

    fn compact_on_snapshot() -> bool {
        true
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.value += event.0;
        Ok(())
    }
}

#[async_trait]
impl Handler<CompactingActor> for CompactingActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: ValueMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<ValueResponse, ActorError> {
        match msg {
            ValueMessage::Increment(delta) => {
                self.value += delta;
                Ok(ValueResponse::Ack)
            }
            ValueMessage::GetValue => Ok(ValueResponse::Value(self.value)),
        }
    }
}

#[tokio::test]
async fn test_persistent_actor_rolls_back_state_when_store_persist_fails() {
    build_tracing_subscriber();
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

#[tokio::test]
async fn test_light_persistence_rolls_back_written_event_when_snapshot_fails() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<RollbackLightActor>::new(
        "rollback_store",
        "prefix",
        FailingStateManager::default(),
        None,
        RollbackLightActor::create_initial(()),
    )
    .unwrap();

    let store_ref: ActorRef<Store<RollbackLightActor>> = system
        .create_root_actor("rollback-store", store)
        .await
        .unwrap();

    let response = store_ref
        .ask(StoreCommand::PersistLight(
            ValueEvent(5),
            RollbackLightActor { value: 5 },
        ))
        .await;
    assert!(matches!(response, Err(ActorError::StoreOperation { .. })));

    let counter = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    assert!(matches!(counter, StoreResponse::LastEventNumber(0)));

    let recovered = store_ref.ask(StoreCommand::Recover).await.unwrap();
    assert!(matches!(recovered, StoreResponse::State(None)));
}

#[tokio::test]
async fn test_recover_fails_when_event_log_has_gap() {
    build_tracing_subscriber();
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<GapActor>::new(
        "gap_store",
        "prefix",
        manager.clone(),
        None,
        GapActor::create_initial(()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<GapActor>> =
        system.create_root_actor("gap-store", store).await.unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::Persist(ValueEvent(1)))
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));
    assert!(matches!(
        store_ref
            .ask(StoreCommand::Persist(ValueEvent(2)))
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

    let actor1_items: Vec<_> = coll_actor1.iter(false).unwrap().collect();
    assert_eq!(actor1_items, vec![("0001".to_owned(), b"one".to_vec())]);

    Collection::purge(&mut coll_actor1).unwrap();
    assert_eq!(Collection::get(&coll_actor10, "0001").unwrap(), b"ten");
}

#[tokio::test]
async fn test_full_persistence_compacts_events_after_snapshot_when_enabled() {
    build_tracing_subscriber();
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CompactingActor>::new(
        "compact_store",
        "prefix",
        manager.clone(),
        None,
        CompactingActor::create_initial(()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<CompactingActor>> = system
        .create_root_actor("compact-store", store)
        .await
        .unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(2),
                actor: CompactingActor { value: 2 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));
    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(3),
                actor: CompactingActor { value: 5 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    let collection = manager
        .create_collection("compact_store_events", "prefix")
        .unwrap();
    assert!(collection.iter(false).unwrap().next().is_none());

    let recovered = store_ref.ask(StoreCommand::Recover).await.unwrap();
    match recovered {
        StoreResponse::State(Some(state)) => assert_eq!(state.value, 5),
        _ => panic!("expected recovered compacted state"),
    }
}

#[tokio::test]
async fn test_compacted_snapshot_preserves_event_counter_after_restart() {
    build_tracing_subscriber();
    let manager = MemoryManager::default();

    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CompactingActor>::new(
        "compact_restart_store",
        "prefix",
        manager.clone(),
        None,
        CompactingActor::create_initial(()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<CompactingActor>> = system
        .create_root_actor("compact-restart-store", store)
        .await
        .unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(2),
                actor: CompactingActor { value: 2 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));
    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(3),
                actor: CompactingActor { value: 5 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    let compacted_events = manager
        .create_collection("compact_restart_store_events", "prefix")
        .unwrap();
    assert!(compacted_events.iter(false).unwrap().next().is_none());

    let (system2, mut runner2) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner2.run().await });

    let restarted_store = Store::<CompactingActor>::new(
        "compact_restart_store",
        "prefix",
        manager.clone(),
        None,
        CompactingActor::create_initial(()),
    )
    .unwrap();
    let restarted_ref: ActorRef<Store<CompactingActor>> = system2
        .create_root_actor("compact-restart-store-2", restarted_store)
        .await
        .unwrap();

    match restarted_ref.ask(StoreCommand::Recover).await.unwrap() {
        StoreResponse::State(Some(state)) => assert_eq!(state.value, 5),
        _ => panic!("expected recovered state from compacted snapshot"),
    }

    assert!(matches!(
        restarted_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(4),
                actor: CompactingActor { value: 9 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    let restarted_ref2: ActorRef<Store<CompactingActor>> = system2
        .create_root_actor(
            "compact-restart-store-3",
            Store::<CompactingActor>::new(
                "compact_restart_store",
                "prefix",
                manager,
                None,
                CompactingActor::create_initial(()),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    match restarted_ref2.ask(StoreCommand::Recover).await.unwrap() {
        StoreResponse::State(Some(state)) => assert_eq!(state.value, 9),
        _ => panic!("expected recovered state after post-restart persist"),
    }
}

#[tokio::test]
async fn test_compaction_watermark_is_persisted_across_restart() {
    build_tracing_subscriber();
    let manager = LoggingCompactionManager::default();

    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CompactingActor>::new(
        "logging_compact_store",
        "prefix",
        manager.clone(),
        None,
        CompactingActor::create_initial(()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<CompactingActor>> = system
        .create_root_actor("logging-compact-store", store)
        .await
        .unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(2),
                actor: CompactingActor { value: 2 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    let (system2, mut runner2) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner2.run().await });

    let restarted_store = Store::<CompactingActor>::new(
        "logging_compact_store",
        "prefix",
        manager.clone(),
        None,
        CompactingActor::create_initial(()),
    )
    .unwrap();
    let restarted_ref: ActorRef<Store<CompactingActor>> = system2
        .create_root_actor("logging-compact-store-2", restarted_store)
        .await
        .unwrap();

    assert!(matches!(
        restarted_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(3),
                actor: CompactingActor { value: 5 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    let deleted_keys = manager.deleted_keys.lock().unwrap().clone();
    assert_eq!(
        deleted_keys,
        vec!["00000000000000000000".to_owned(), "00000000000000000001".to_owned()]
    );
}

#[tokio::test]
async fn test_repeated_compaction_only_deletes_newly_covered_events() {
    build_tracing_subscriber();
    let manager = LoggingCompactionManager::default();

    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CompactingActor>::new(
        "repeated_compact_store",
        "prefix",
        manager.clone(),
        None,
        CompactingActor::create_initial(()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<CompactingActor>> = system
        .create_root_actor("repeated-compact-store", store)
        .await
        .unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(1),
                actor: CompactingActor { value: 1 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));
    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(2),
                actor: CompactingActor { value: 3 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));
    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(3),
                actor: CompactingActor { value: 6 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    let deleted_keys = manager.deleted_keys.lock().unwrap().clone();
    assert_eq!(
        deleted_keys,
        vec![
            "00000000000000000000".to_owned(),
            "00000000000000000001".to_owned(),
            "00000000000000000002".to_owned(),
        ]
    );

    let collection = manager
        .memory
        .create_collection("repeated_compact_store_events", "prefix")
        .unwrap();
    assert!(collection.iter(false).unwrap().next().is_none());

    match store_ref.ask(StoreCommand::Recover).await.unwrap() {
        StoreResponse::State(Some(state)) => assert_eq!(state.value, 6),
        _ => panic!("expected recovered state after repeated compaction"),
    }
}

#[test]
fn test_get_by_range_reports_requested_missing_key() {
    let mut collection = RangeCollection::default();
    collection.put("a", b"1").unwrap();
    collection.put("b", b"2").unwrap();

    let result = collection.get_by_range(Some("missing".to_owned()), 1);
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
    let result = Store::<GapActor>::new(
        "last_error_store",
        "prefix",
        LastErrorManager::default(),
        None,
        GapActor::create_initial(()),
    );

    assert!(matches!(
        result,
        Err(StoreError::Store {
            operation: StoreOperation::Test,
            ..
        })
    ));
}

#[tokio::test]
async fn test_recover_falls_back_when_metadata_state_is_missing() {
    build_tracing_subscriber();
    let manager = MemoryManager::default();

    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CompactingActor>::new(
        "metadata_fallback_store",
        "prefix",
        manager.clone(),
        None,
        CompactingActor::create_initial(()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<CompactingActor>> = system
        .create_root_actor("metadata-fallback-store", store)
        .await
        .unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(4),
                actor: CompactingActor { value: 4 },
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

    let restarted = Store::<CompactingActor>::new(
        "metadata_fallback_store",
        "prefix",
        manager,
        None,
        CompactingActor::create_initial(()),
    )
    .unwrap();
    let restarted_ref: ActorRef<Store<CompactingActor>> = system2
        .create_root_actor("metadata-fallback-store-2", restarted)
        .await
        .unwrap();

    match restarted_ref.ask(StoreCommand::Recover).await.unwrap() {
        StoreResponse::State(Some(state)) => assert_eq!(state.value, 4),
        _ => panic!("expected recovery via snapshot fallback without metadata"),
    }
}

#[tokio::test]
async fn test_recover_fails_when_encrypted_pending_event_is_corrupted() {
    build_tracing_subscriber();
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let encrypt_key = EncryptedKey::new(&[9u8; 32]).unwrap();
    let store = Store::<GapActor>::new(
        "encrypted_gap_store",
        "prefix",
        manager.clone(),
        Some(encrypt_key),
        GapActor::create_initial(()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<GapActor>> = system
        .create_root_actor("encrypted-gap-store", store)
        .await
        .unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFull {
                event: ValueEvent(2),
                actor: GapActor { value: 2 },
                snapshot_every: Some(1),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));
    assert!(matches!(
        store_ref
            .ask(StoreCommand::Persist(ValueEvent(3)))
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

#[tokio::test]
async fn test_snapshot_compaction_failure_is_best_effort() {
    build_tracing_subscriber();
    let manager = FailingCompactionManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CompactingActor>::new(
        "best_effort_compact_store",
        "prefix",
        manager.clone(),
        None,
        CompactingActor::create_initial(()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<CompactingActor>> = system
        .create_root_actor("best-effort-compact-store", store)
        .await
        .unwrap();

    let response = store_ref
        .ask(StoreCommand::PersistFull {
            event: ValueEvent(7),
            actor: CompactingActor { value: 7 },
            snapshot_every: Some(1),
        })
        .await
        .unwrap();
    assert!(matches!(response, StoreResponse::Persisted));

    match store_ref.ask(StoreCommand::Recover).await.unwrap() {
        StoreResponse::State(Some(state)) => assert_eq!(state.value, 7),
        _ => panic!("expected recovered state after best-effort compaction"),
    }

    let events = store_ref
        .ask(StoreCommand::GetEvents { from: 0, to: 0 })
        .await
        .unwrap();
    assert!(matches!(
        events,
        StoreResponse::Events(ref values) if values == &vec![ValueEvent(7)]
    ));
}

#[tokio::test]
async fn test_persist_full_event_requests_snapshot_only_when_due() {
    build_tracing_subscriber();
    let manager = MemoryManager::default();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<GapActor>::new(
        "persist_full_event_store",
        "prefix",
        manager,
        None,
        GapActor::create_initial(()),
    )
    .unwrap();
    let store_ref: ActorRef<Store<GapActor>> = system
        .create_root_actor("persist-full-event-store", store)
        .await
        .unwrap();

    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFullEvent {
                event: ValueEvent(2),
                snapshot_every: Some(2),
            })
            .await
            .unwrap(),
        StoreResponse::Persisted
    ));

    assert!(matches!(
        store_ref
            .ask(StoreCommand::PersistFullEvent {
                event: ValueEvent(3),
                snapshot_every: Some(2),
            })
            .await
            .unwrap(),
        StoreResponse::SnapshotRequired
    ));

    assert!(matches!(
        store_ref
            .ask(StoreCommand::Snapshot(GapActor { value: 5 }))
            .await
            .unwrap(),
        StoreResponse::Snapshotted
    ));
}

#[test]
fn test_store_new_fails_when_last_event_key_is_corrupted() {
    let manager = MemoryManager::default();
    let mut collection = manager
        .create_collection("corrupted_key_store_events", "prefix")
        .unwrap();
    Collection::put(&mut collection, "not-a-number", b"broken").unwrap();

    let result = Store::<GapActor>::new(
        "corrupted_key_store",
        "prefix",
        manager,
        None,
        GapActor::create_initial(()),
    );

    assert!(matches!(
        result,
        Err(StoreError::Store {
            operation: StoreOperation::ParseEventKey,
            ..
        })
    ));
}
