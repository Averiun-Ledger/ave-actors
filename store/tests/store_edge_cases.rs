//! Comprehensive edge case tests for Store module to increase coverage

#[macro_use]
mod helpers;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, EncryptedKey,
    Error as ActorError, Event, Handler, Message, Response,
};
use ave_actors_store::{
    Error as StoreError, StoreOperation,
    database::{Collection, DbManager, State},
    memory::MemoryManager,
    store::{
        FullPersistence, LightPersistence, PersistentActor, Store,
        StoreCommand, StoreResponse,
    },
};
use test_log::test;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// State struct for encrypted actor
#[derive(
    Debug, Clone, Default, borsh::BorshSerialize, borsh::BorshDeserialize,
)]
struct EncryptedActorState {
    pub counter: usize,
    pub data: String,
}

// Test actor with encryption
#[derive(Debug)]
struct EncryptedActor {
    state_ptr: Arc<EncryptedActorState>,
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
    type SinkEvent = Self::Event;

    type ChildError = ActorError;
    type ChildFault = ActorError;

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
}

#[async_trait]
impl PersistentActor for EncryptedActor {
    type Persistence = FullPersistence;
    type InitParams = ();
    type State = EncryptedActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state_ptr: Arc::new(EncryptedActorState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut new_state = state;
        Arc::make_mut(&mut new_state).counter = event.counter;
        Arc::make_mut(&mut new_state).data.clone_from(&event.data);
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
impl Handler<Self> for EncryptedActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: EncryptedMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<EncryptedResponse, ActorError> {
        match msg {
            EncryptedMessage::Increment(value) => {
                let event = EncryptedEvent {
                    counter: self.state_ptr.counter + value,
                    data: self.state_ptr.data.clone(),
                };
                self.persist(event, ctx).await?;
                Ok(EncryptedResponse::Success)
            }
            EncryptedMessage::SetData(data) => {
                let event = EncryptedEvent {
                    counter: self.state_ptr.counter,
                    data: data.clone(),
                };
                self.persist(event, ctx).await?;
                Ok(EncryptedResponse::Success)
            }
            EncryptedMessage::GetState => Ok(EncryptedResponse::State {
                counter: self.state_ptr.counter,
                data: self.state_ptr.data.clone(),
            }),
            EncryptedMessage::TriggerRecovery => {
                if let Ok(store) = ctx.get_child::<Store<Self>>("store").await {
                    let response = store.ask(StoreCommand::Recover).await?;
                    if let StoreResponse::State(Some(state)) = response {
                        self.set_state(state);
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

// State struct for light actor
#[derive(
    Debug, Clone, Default, borsh::BorshSerialize, borsh::BorshDeserialize,
)]
struct LightActorState {
    pub value: i32,
}

// Test actor with light persistence
#[derive(Debug)]
struct LightActor {
    state_ptr: Arc<LightActorState>,
}

#[async_trait]
impl Actor for LightActor {
    type Message = EncryptedMessage;
    type Response = EncryptedResponse;
    type Event = EncryptedEvent;
    type SinkEvent = Self::Event;
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
        let memory_db = MemoryManager::default();
        self.start_store("light_test", None, ctx, memory_db, None)
            .await
    }
}

#[async_trait]
impl PersistentActor for LightActor {
    type Persistence = LightPersistence;
    type InitParams = ();
    type State = LightActorState;

    fn create_initial(_: ()) -> Self {
        Self {
            state_ptr: Arc::new(LightActorState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
        let mut new_state = state;
        Arc::make_mut(&mut new_state).value = event.counter as i32;
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
impl Handler<Self> for LightActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: EncryptedMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<EncryptedResponse, ActorError> {
        match msg {
            EncryptedMessage::Increment(value) => {
                let event = EncryptedEvent {
                    counter: self.state_ptr.value as usize + value,
                    data: "light".to_string(),
                };
                self.persist(event, ctx).await?;
                Ok(EncryptedResponse::Success)
            }
            EncryptedMessage::GetState => Ok(EncryptedResponse::State {
                counter: self.state_ptr.value as usize,
                data: "light".to_string(),
            }),
            _ => Ok(EncryptedResponse::Success),
        }
    }
}

// Failing database manager for testing error scenarios
#[derive(Clone, Default)]
struct FailingManager {
    fail_create: bool,
    fail_operations: bool,
}

struct FailingCollection {
    name: String,
    fail_operations: bool,
    data: BTreeMap<String, Vec<u8>>,
}

impl Collection for FailingCollection {
    fn last(&self) -> Result<Option<(String, Vec<u8>)>, StoreError> {
        let mut iter = self.iter(true)?;
        iter.next().transpose()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, _key: &str) -> Result<Vec<u8>, StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: StoreOperation::Test,
                reason: "Intentional failure".to_string(),
                source: None,
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
                operation: StoreOperation::Test,
                reason: "Intentional failure".to_string(),
                source: None,
            })
        } else {
            self.data.insert(key.to_string(), data.to_vec());
            Ok(())
        }
    }

    fn del(&mut self, _key: &str) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: StoreOperation::Test,
                reason: "Intentional failure".to_string(),
                source: None,
            })
        } else {
            Ok(())
        }
    }

    fn purge(&mut self) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: StoreOperation::Test,
                reason: "Intentional failure".to_string(),
                source: None,
            })
        } else {
            self.data.clear();
            Ok(())
        }
    }

    fn iter<'a>(
        &'a self,
        _reverse: bool,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(String, Vec<u8>), StoreError>> + 'a>,
        StoreError,
    > {
        Ok(Box::new(
            self.data.iter().map(|(k, v)| Ok((k.clone(), v.clone()))),
        ))
    }
}

impl State for FailingCollection {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self) -> Result<Vec<u8>, StoreError> {
        Err(StoreError::EntryNotFound {
            key: "Not found".to_string(),
        })
    }

    fn put(&mut self, data: &[u8]) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: StoreOperation::Test,
                reason: "Intentional failure".to_string(),
                source: None,
            })
        } else {
            self.data.insert("state".to_string(), data.to_vec());
            Ok(())
        }
    }

    fn del(&mut self) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: StoreOperation::Test,
                reason: "Intentional failure".to_string(),
                source: None,
            })
        } else {
            self.data.remove("state");
            Ok(())
        }
    }

    fn purge(&mut self) -> Result<(), StoreError> {
        if self.fail_operations {
            Err(StoreError::Store {
                operation: StoreOperation::Test,
                reason: "Intentional failure".to_string(),
                source: None,
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
                operation: StoreOperation::CreateCollection,
                reason: "Failed to create collection".to_string(),
                source: None,
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
                operation: StoreOperation::CreateState,
                reason: "Failed to create state".to_string(),
                source: None,
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

#[test(tokio::test)]
async fn test_encrypted_store_operations() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
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

#[test(tokio::test)]
async fn test_light_persistence() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
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

    // A second actor with a different name uses a different persistence
    // prefix (derived from its path), so it must not see the first actor's
    // state. LightPersistence does recover by prefix; the fresh start here
    // comes from the distinct prefix, not from the persistence strategy.
    let actor_ref2 = system
        .create_root_actor("light2", LightActor::initial(()))
        .await
        .unwrap();

    let response = actor_ref2.ask(EncryptedMessage::GetState).await.unwrap();
    if let EncryptedResponse::State { counter, .. } = response {
        // Distinct prefix -> no prior snapshot -> initial state (counter 0).
        assert_eq!(counter, 0);
    } else {
        panic!("Expected State response");
    }
}

#[test(tokio::test)]
async fn test_store_error_scenarios() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    // Test store creation failure
    let failing_manager = FailingManager {
        fail_create: true,
        fail_operations: false,
    };

    let store_result = store_new!(
        EncryptedActor,
        "test",
        "prefix",
        failing_manager,
        None,
        Arc::new(EncryptedActorState::default()),
    );
    assert!(store_result.is_err());

    // Test store operations failure
    let failing_manager = FailingManager {
        fail_create: false,
        fail_operations: true,
    };

    let store = store_new!(
        EncryptedActor,
        "test",
        "prefix",
        failing_manager,
        None,
        Arc::new(EncryptedActorState::default()),
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

    let result = store_ref.ask(StoreCommand::Persist(Arc::new(event))).await;
    assert!(matches!(result, Err(ActorError::StoreOperation { .. })));

    // Test snapshot failure
    let state = Arc::new(EncryptedActorState {
        counter: 1,
        data: "test".to_string(),
    });

    let result = store_ref.ask(StoreCommand::Snapshot(state)).await;
    assert!(matches!(result, Err(ActorError::StoreOperation { .. })));

    // Test recover with no state
    let result = store_ref.ask(StoreCommand::Recover).await;
    assert!(matches!(
        result,
        Ok(StoreResponse::State(None)) | Err(ActorError::StoreOperation { .. })
    ));
}

#[test(tokio::test)]
async fn test_store_commands_coverage() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = store_new!(
        EncryptedActor,
        "test",
        "prefix",
        MemoryManager::default(),
        None,
        Arc::new(EncryptedActorState::default()),
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

    // LastEventsFrom on an empty store must return an empty list, not a
    // gap error (consistent with GetEvents).
    let result = store_ref
        .ask(StoreCommand::LastEventsFrom(0))
        .await
        .unwrap();
    match result {
        StoreResponse::Events(events) => assert!(events.is_empty()),
        _ => panic!("Expected empty Events for empty store"),
    }

    // Add some events first
    let event = EncryptedEvent {
        counter: 1,
        data: "test1".to_string(),
    };
    store_ref
        .ask(StoreCommand::Persist(Arc::new(event)))
        .await
        .unwrap();

    let event = EncryptedEvent {
        counter: 2,
        data: "test2".to_string(),
    };
    store_ref
        .ask(StoreCommand::Persist(Arc::new(event)))
        .await
        .unwrap();

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

#[test(tokio::test)]

async fn test_persist_actor_error_scenarios() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    // Test actor without store child
    #[derive(
        Debug, Clone, Default, borsh::BorshSerialize, borsh::BorshDeserialize,
    )]
    struct NoStoreActorState {
        value: i32,
    }

    #[derive(Debug)]
    struct NoStoreActor {
        state_ptr: Arc<NoStoreActorState>,
    }

    #[async_trait]
    impl Actor for NoStoreActor {
        type Message = EncryptedMessage;
        type Response = EncryptedResponse;
        type Event = EncryptedEvent;
        type SinkEvent = Self::Event;
        type ChildError = ActorError;
        type ChildFault = ActorError;

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
        type State = NoStoreActorState;

        fn create_initial(_: ()) -> Self {
            Self {
                state_ptr: Arc::new(NoStoreActorState::default()),
            }
        }

        fn apply(
            state: Arc<Self::State>,
            event: &Self::Event,
        ) -> Result<Arc<Self::State>, ActorError> {
            let mut new_state = state;
            Arc::make_mut(&mut new_state).value = event.counter as i32;
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
    impl Handler<Self> for NoStoreActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: EncryptedMessage,
            ctx: &mut ActorContext<Self>,
        ) -> Result<EncryptedResponse, ActorError> {
            match msg {
                EncryptedMessage::Increment(value) => {
                    let event = EncryptedEvent {
                        counter: value,
                        data: "test".to_string(),
                    };
                    // This should fail because no store child exists
                    match self.persist(event, ctx).await {
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

#[test(tokio::test)]
async fn test_encryption_failure_scenarios() {
    // Test with invalid key size (this would be a compile-time error, so we test valid scenario)
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let encrypt_key = EncryptedKey::new(&[0u8; 32]).unwrap();
    let store = store_new!(
        EncryptedActor,
        "test",
        "prefix",
        MemoryManager::default(),
        Some(encrypt_key),
        Arc::new(EncryptedActorState::default()),
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
        .ask(StoreCommand::Persist(Arc::new(event.clone())))
        .await
        .unwrap();

    let state = Arc::new(EncryptedActorState {
        counter: 0,
        data: "".to_string(),
    });
    store_ref.ask(StoreCommand::Snapshot(state)).await.unwrap();

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
