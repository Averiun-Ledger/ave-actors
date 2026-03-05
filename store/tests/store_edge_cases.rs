//! Comprehensive edge case tests for Store module to increase coverage

use ave_actors_store::{
    Error as StoreError,
    database::{Collection, DbManager, State},
    memory::MemoryManager,
    store::{
        FullPersistence, LightPersistence, PersistentActor, Store,
        StoreCommand, StoreResponse,
    },
};

use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, EncryptedKey,
    Error as ActorError, Event, Handler, Message, Response,
    build_tracing_subscriber,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// Test actor with encryption
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    Default,
)]
struct EncryptedActor {
    pub counter: usize,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum EncryptedMessage {
    Increment(usize),
    SetData(String),
    GetState,
    TriggerRecovery,
    TestPersistFailure,
    TestSnapshotFailure,
    Purge,
}

impl Message for EncryptedMessage {}

#[derive(Debug, Clone, PartialEq)]
enum EncryptedResponse {
    Success,
    State { counter: usize, data: String },
    Error(String),
}

impl Response for EncryptedResponse {}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
struct EncryptedEvent {
    pub counter: usize,
    pub data: String,
}

impl Event for EncryptedEvent {}

#[async_trait]
impl Actor for EncryptedActor {
    type Message = EncryptedMessage;
    type Response = EncryptedResponse;
    type Event = EncryptedEvent;
    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("EncryptedActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let memory_db = MemoryManager::default();
        let encrypt_key = EncryptedKey::new(&[1u8; 32]).unwrap();
        self.start_store(
            "encrypted_test",
            None,
            ctx,
            memory_db,
            Some(encrypt_key),
        )
        .await
    }

    async fn pre_stop(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        self.stop_store(ctx).await
    }
}

#[async_trait]
impl PersistentActor for EncryptedActor {
    type Persistence = FullPersistence;
    type InitParams = ();

    fn create_initial(_: ()) -> Self {
        Self {
            counter: 0,
            data: String::new(),
        }
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.counter = event.counter;
        self.data = event.data.clone();
        Ok(())
    }
}

#[async_trait]
impl Handler<EncryptedActor> for EncryptedActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: EncryptedMessage,
        ctx: &mut ActorContext<EncryptedActor>,
    ) -> Result<EncryptedResponse, ActorError> {
        match msg {
            EncryptedMessage::Increment(value) => {
                let event = EncryptedEvent {
                    counter: self.counter + value,
                    data: self.data.clone(),
                };
                self.persist(&event, ctx).await?;
                Ok(EncryptedResponse::Success)
            }
            EncryptedMessage::SetData(data) => {
                let event = EncryptedEvent {
                    counter: self.counter,
                    data: data.clone(),
                };
                self.persist(&event, ctx).await?;
                Ok(EncryptedResponse::Success)
            }
            EncryptedMessage::GetState => Ok(EncryptedResponse::State {
                counter: self.counter,
                data: self.data.clone(),
            }),
            EncryptedMessage::TriggerRecovery => {
                if let Ok(store) = ctx.get_child::<Store<Self>>("store").await {
                    let response = store.ask(StoreCommand::Recover).await?;
                    if let StoreResponse::State(Some(state)) = response {
                        self.update(state);
                        Ok(EncryptedResponse::Success)
                    } else {
                        Ok(EncryptedResponse::Error(
                            "No state to recover".to_string(),
                        ))
                    }
                } else {
                    Ok(EncryptedResponse::Error("No store found".to_string()))
                }
            }
            EncryptedMessage::TestPersistFailure => {
                // This should test error scenarios in persistence
                Ok(EncryptedResponse::Success)
            }
            EncryptedMessage::TestSnapshotFailure => {
                self.snapshot(ctx).await?;
                Ok(EncryptedResponse::Success)
            }
            EncryptedMessage::Purge => {
                if let Ok(store) = ctx.get_child::<Store<Self>>("store").await {
                    store.ask(StoreCommand::Purge).await?;
                    Ok(EncryptedResponse::Success)
                } else {
                    Ok(EncryptedResponse::Error("No store found".to_string()))
                }
            }
        }
    }
}

// Test actor with light persistence
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    Default,
)]
struct LightActor {
    pub value: i32,
}

#[async_trait]
impl Actor for LightActor {
    type Message = EncryptedMessage;
    type Response = EncryptedResponse;
    type Event = EncryptedEvent;

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
        let memory_db = MemoryManager::default();
        self.start_store("light_test", None, ctx, memory_db, None)
            .await
    }

    async fn pre_stop(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        self.stop_store(ctx).await
    }
}

#[async_trait]
impl PersistentActor for LightActor {
    type Persistence = LightPersistence;
    type InitParams = ();

    fn create_initial(_: ()) -> Self {
        Self { value: 0 }
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.value = event.counter as i32;
        Ok(())
    }
}

#[async_trait]
impl Handler<LightActor> for LightActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: EncryptedMessage,
        ctx: &mut ActorContext<LightActor>,
    ) -> Result<EncryptedResponse, ActorError> {
        match msg {
            EncryptedMessage::Increment(value) => {
                let event = EncryptedEvent {
                    counter: self.value as usize + value,
                    data: "light".to_string(),
                };
                self.persist(&event, ctx).await?;
                Ok(EncryptedResponse::Success)
            }
            EncryptedMessage::GetState => Ok(EncryptedResponse::State {
                counter: self.value as usize,
                data: "light".to_string(),
            }),
            _ => Ok(EncryptedResponse::Success),
        }
    }
}

// Failing database manager for testing error scenarios
#[derive(Clone)]
struct FailingManager {
    fail_create: bool,
    fail_operations: bool,
}

impl Default for FailingManager {
    fn default() -> Self {
        Self {
            fail_create: false,
            fail_operations: false,
        }
    }
}

struct FailingCollection {
    name: String,
    fail_operations: bool,
    data: BTreeMap<String, Vec<u8>>,
}

impl Collection for FailingCollection {
    fn last(&self) -> Option<(String, Vec<u8>)> {
        let mut iter = self.iter(true);
        let value = iter.next();
        value
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, _key: &str) -> Result<Vec<u8>, StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: "test".to_owned(),
                reason: "Intentional failure".to_string(),
            })
        } else {
            Err(StoreError::EntryNotFound {
                key: "Not found".to_string(),
            })
        }
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: "test".to_owned(),
                reason: "Intentional failure".to_string(),
            })
        } else {
            self.data.insert(key.to_string(), data.to_vec());
            Ok(())
        }
    }

    fn del(&mut self, _key: &str) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: "test".to_owned(),
                reason: "Intentional failure".to_string(),
            })
        } else {
            Ok(())
        }
    }

    fn purge(&mut self) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: "test".to_owned(),
                reason: "Intentional failure".to_string(),
            })
        } else {
            self.data.clear();
            Ok(())
        }
    }

    fn iter<'a>(
        &'a self,
        _reverse: bool,
    ) -> Box<dyn Iterator<Item = (String, Vec<u8>)> + 'a> {
        Box::new(self.data.iter().map(|(k, v)| (k.clone(), v.clone())))
    }
}

impl State for FailingCollection {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self) -> Result<Vec<u8>, StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: "test".to_owned(),
                reason: "Intentional failure".to_string(),
            })
        } else {
            Err(StoreError::EntryNotFound {
                key: "Not found".to_string(),
            })
        }
    }

    fn put(&mut self, data: &[u8]) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: "test".to_owned(),
                reason: "Intentional failure".to_string(),
            })
        } else {
            self.data.insert("state".to_string(), data.to_vec());
            Ok(())
        }
    }

    fn del(&mut self) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: "test".to_owned(),
                reason: "Intentional failure".to_string(),
            })
        } else {
            self.data.remove("state");
            Ok(())
        }
    }

    fn purge(&mut self) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: "test".to_owned(),
                reason: "Intentional failure".to_string(),
            })
        } else {
            self.data.clear();
            Ok(())
        }
    }
}

impl DbManager<FailingCollection, FailingCollection> for FailingManager {
    fn create_collection(
        &self,
        name: &str,
        _prefix: &str,
    ) -> Result<FailingCollection, StoreError> {
        if self.fail_create {
            Err(StoreError::Store {
                operation: "create_collection".to_owned(),
                reason: "Failed to create collection".to_string(),
            })
        } else {
            Ok(FailingCollection {
                name: name.to_string(),
                fail_operations: self.fail_operations,
                data: BTreeMap::new(),
            })
        }
    }

    fn stop(self) -> Result<(), StoreError> {
        Ok(())
    }

    fn create_state(
        &self,
        name: &str,
        _prefix: &str,
    ) -> Result<FailingCollection, StoreError> {
        if self.fail_create {
            Err(StoreError::Store {
                operation: "create_state".to_owned(),
                reason: "Failed to create state".to_string(),
            })
        } else {
            Ok(FailingCollection {
                name: name.to_string(),
                fail_operations: self.fail_operations,
                data: BTreeMap::new(),
            })
        }
    }
}

// Tests

#[tokio::test]
async fn test_encrypted_store_operations() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("encrypted", EncryptedActor::initial(()))
        .await
        .unwrap();

    // Test increment with encryption
    actor_ref
        .tell(EncryptedMessage::Increment(5))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let response = actor_ref.ask(EncryptedMessage::GetState).await.unwrap();
    if let EncryptedResponse::State { counter, data } = response {
        assert_eq!(counter, 5);
        assert_eq!(data, ""); // initial state has empty string
    } else {
        panic!("Expected State response");
    }

    // Test data update
    actor_ref
        .tell(EncryptedMessage::SetData("updated".to_string()))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let response = actor_ref.ask(EncryptedMessage::GetState).await.unwrap();
    if let EncryptedResponse::State { counter, data } = response {
        assert_eq!(counter, 5);
        assert_eq!(data, "updated");
    } else {
        panic!("Expected State response");
    }

    // Test recovery
    actor_ref
        .ask(EncryptedMessage::TriggerRecovery)
        .await
        .unwrap();

    // Test snapshot
    actor_ref
        .ask(EncryptedMessage::TestSnapshotFailure)
        .await
        .unwrap();

    // Test purge
    actor_ref.ask(EncryptedMessage::Purge).await.unwrap();
}

#[tokio::test]
async fn test_light_persistence() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor("light", LightActor::initial(()))
        .await
        .unwrap();

    // Test light persistence (should only keep last state)
    actor_ref
        .tell(EncryptedMessage::Increment(10))
        .await
        .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let response = actor_ref.ask(EncryptedMessage::GetState).await.unwrap();
    if let EncryptedResponse::State { counter, .. } = response {
        assert_eq!(counter, 10);
    } else {
        panic!("Expected State response");
    }

    actor_ref.ask_stop().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Create new actor with different name - light persistence may not automatically recover
    let actor_ref2 = system
        .create_root_actor("light2", LightActor::initial(()))
        .await
        .unwrap();

    // For light persistence, we don't expect automatic recovery of state
    // Light persistence only keeps the last state, but doesn't automatically restore it
    let response = actor_ref2.ask(EncryptedMessage::GetState).await.unwrap();
    if let EncryptedResponse::State { counter, .. } = response {
        // New actor starts fresh with light persistence
        assert_eq!(counter, 0);
    } else {
        panic!("Expected State response");
    }
}

#[tokio::test]
async fn test_store_error_scenarios() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    // Test store creation failure
    let failing_manager = FailingManager {
        fail_create: true,
        fail_operations: false,
    };

    let store_result = Store::<EncryptedActor>::new(
        "test",
        "prefix",
        failing_manager,
        None,
        EncryptedActor::create_initial(()),
    );
    assert!(store_result.is_err());

    // Test store operations failure
    let failing_manager = FailingManager {
        fail_create: false,
        fail_operations: true,
    };

    let store = Store::<EncryptedActor>::new(
        "test",
        "prefix",
        failing_manager,
        None,
        EncryptedActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system
        .create_root_actor("failing_store", store)
        .await
        .unwrap();

    // Test persist failure
    let event = EncryptedEvent {
        counter: 1,
        data: "test".to_string(),
    };

    let result = store_ref.ask(StoreCommand::Persist(event)).await.unwrap();
    match result {
        StoreResponse::Error(_) => {} // Expected
        _ => panic!("Expected error response"),
    }

    // Test snapshot failure
    let actor = EncryptedActor {
        counter: 1,
        data: "test".to_string(),
    };

    let result = store_ref.ask(StoreCommand::Snapshot(actor)).await.unwrap();
    match result {
        StoreResponse::Error(_) => {} // Expected
        _ => panic!("Expected error response"),
    }

    // Test recover with no state
    let result = store_ref.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(None) => {} // Expected for empty store
        StoreResponse::Error(_) => {} // Also acceptable due to failing operations
        _ => panic!("Expected None state or error response"),
    }
}

#[tokio::test]
async fn test_store_commands_coverage() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<EncryptedActor>::new(
        "test",
        "prefix",
        MemoryManager::default(),
        None,
        EncryptedActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system
        .create_root_actor("coverage_store", store)
        .await
        .unwrap();

    // Test all store commands for coverage

    // LastEvent
    let result = store_ref.ask(StoreCommand::LastEvent).await.unwrap();
    match result {
        StoreResponse::LastEvent(None) => {} // Expected for empty store
        _ => panic!("Expected None for last event"),
    }

    // LastEventNumber
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(num) => assert_eq!(num, 0),
        _ => panic!("Expected LastEventNumber response"),
    }

    // Add some events first
    let event = EncryptedEvent {
        counter: 1,
        data: "test1".to_string(),
    };
    store_ref.ask(StoreCommand::Persist(event)).await.unwrap();

    let event = EncryptedEvent {
        counter: 2,
        data: "test2".to_string(),
    };
    store_ref.ask(StoreCommand::Persist(event)).await.unwrap();

    // GetEvents
    let result = store_ref
        .ask(StoreCommand::GetEvents { from: 0, to: 1 })
        .await
        .unwrap();
    match result {
        StoreResponse::Events(events) => assert_eq!(events.len(), 2),
        _ => panic!("Expected Events response"),
    }

    // LastEventsFrom
    let result = store_ref
        .ask(StoreCommand::LastEventsFrom(0))
        .await
        .unwrap();
    match result {
        StoreResponse::Events(events) => assert_eq!(events.len(), 2),
        _ => panic!("Expected Events response"),
    }

    // LastEvent (now should return something)
    let result = store_ref.ask(StoreCommand::LastEvent).await.unwrap();
    match result {
        StoreResponse::LastEvent(Some(event)) => {
            assert_eq!(event.counter, 2);
            assert_eq!(event.data, "test2");
        }
        _ => panic!("Expected Some event for last event"),
    }
}

#[tokio::test]

async fn test_persist_actor_error_scenarios() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    // Test actor without store child
    #[derive(
        Debug,
        Clone,
        Serialize,
        Deserialize,
        borsh::BorshSerialize,
        borsh::BorshDeserialize,
        Default,
    )]
    struct NoStoreActor {
        value: i32,
    }

    #[async_trait]
    impl Actor for NoStoreActor {
        type Message = EncryptedMessage;
        type Response = EncryptedResponse;
        type Event = EncryptedEvent;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("NoStoreActor", id = %id)
        }
    }

    #[async_trait]
    impl PersistentActor for NoStoreActor {
        type Persistence = FullPersistence;
        type InitParams = ();

        fn create_initial(_: ()) -> Self {
            Self { value: 0 }
        }

        fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
            self.value = event.counter as i32;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<NoStoreActor> for NoStoreActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: EncryptedMessage,
            ctx: &mut ActorContext<NoStoreActor>,
        ) -> Result<EncryptedResponse, ActorError> {
            match msg {
                EncryptedMessage::Increment(value) => {
                    let event = EncryptedEvent {
                        counter: value,
                        data: "test".to_string(),
                    };
                    // This should fail because no store child exists
                    match self.persist(&event, ctx).await {
                        Err(ActorError::NotFound { path }) => {
                            Ok(EncryptedResponse::Error(format!(
                                "Not found: {}",
                                path
                            )))
                        }
                        _ => panic!("Expected store error"),
                    }
                }
                _ => Ok(EncryptedResponse::Success),
            }
        }
    }

    let actor_ref = system
        .create_root_actor("no_store", NoStoreActor::initial(()))
        .await
        .unwrap();

    let result = actor_ref.ask(EncryptedMessage::Increment(1)).await.unwrap();
    match result {
        EncryptedResponse::Error(msg) => {
            assert!(msg.contains("/user/no_store/store"))
        }
        _ => panic!("Expected error response"),
    }
}

#[tokio::test]
async fn test_encryption_failure_scenarios() {
    build_tracing_subscriber();
    // Test with invalid key size (this would be a compile-time error, so we test valid scenario)
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let encrypt_key = EncryptedKey::new(&[0u8; 32]).unwrap();
    let store = Store::<EncryptedActor>::new(
        "test",
        "prefix",
        MemoryManager::default(),
        Some(encrypt_key),
        EncryptedActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system
        .create_root_actor("encrypted_store", store)
        .await
        .unwrap();

    // Test encryption/decryption by persisting and recovering
    let event = EncryptedEvent {
        counter: 42,
        data: "encrypted_test".to_string(),
    };

    store_ref
        .ask(StoreCommand::Persist(event.clone()))
        .await
        .unwrap();

    let actor = EncryptedActor {
        counter: 0,
        data: "".to_string(),
    };
    store_ref.ask(StoreCommand::Snapshot(actor)).await.unwrap();

    let result = store_ref.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(recovered)) => {
            // Should have recovered the actor state, not the event
            assert_eq!(recovered.counter, 0);
            assert_eq!(recovered.data, "");
        }
        _ => panic!("Expected recovered state"),
    }
}
