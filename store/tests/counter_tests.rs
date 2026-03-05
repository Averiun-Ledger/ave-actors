//! Comprehensive tests for event_counter and state_counter behavior
//!
//! These tests verify that event_counter and state_counter work like vector.len():
//! - event_counter = 0 means no events stored
//! - event_counter = 1 means one event at position 0
//! - state_counter = event_counter after snapshot means all events are applied

use ave_actors_store::{
    memory::MemoryManager,
    store::{
        FullPersistence, PersistentActor, Store, StoreCommand, StoreResponse,
    },
};

use ave_actors_actor::{
    Actor, ActorContext, ActorSystem, EncryptedKey, Error as ActorError, Event,
    Handler, Message, Response, build_tracing_subscriber,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    Default,
)]
struct CounterTestActor {
    value: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CounterMessage {
    Add(i32),
    GetValue,
}

impl Message for CounterMessage {}

#[derive(Debug, Clone, PartialEq)]
enum CounterResponse {
    Success,
    Value(i32),
}

impl Response for CounterResponse {}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
struct CounterEvent {
    delta: i32,
}

impl Event for CounterEvent {}

#[async_trait]
impl Actor for CounterTestActor {
    type Message = CounterMessage;
    type Response = CounterResponse;
    type Event = CounterEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("CounterTestActor", id = %id)
    }
}

#[async_trait]
impl PersistentActor for CounterTestActor {
    type Persistence = FullPersistence;
    type InitParams = ();

    fn create_initial(_: ()) -> Self {
        Self { value: 0 }
    }

    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
        self.value += event.delta;
        Ok(())
    }
}

#[async_trait]
impl Handler<CounterTestActor> for CounterTestActor {
    async fn handle_message(
        &mut self,
        _sender: ave_actors_actor::ActorPath,
        msg: CounterMessage,
        _ctx: &mut ActorContext<CounterTestActor>,
    ) -> Result<CounterResponse, ActorError> {
        match msg {
            CounterMessage::Add(_) => Ok(CounterResponse::Success),
            CounterMessage::GetValue => Ok(CounterResponse::Value(self.value)),
        }
    }
}

/// Test that event_counter starts at 0 for a new store
#[tokio::test]
async fn test_event_counter_starts_at_zero() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CounterTestActor>::new(
        "counter_test",
        "test_zero",
        MemoryManager::default(),
        None,
        CounterTestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Check that LastEventNumber returns 0 for empty store
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => {
            assert_eq!(count, 0, "Empty store should have event_counter = 0")
        }
        _ => panic!("Expected LastEventNumber response"),
    }
}

/// Test that event_counter = 1 after persisting first event
#[tokio::test]

async fn test_event_counter_after_first_event() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CounterTestActor>::new(
        "counter_test",
        "test_first",
        MemoryManager::default(),
        None,
        CounterTestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Persist first event
    let event = CounterEvent { delta: 10 };
    store_ref.ask(StoreCommand::Persist(event)).await.unwrap();

    // Check event_counter = 1
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => {
            assert_eq!(
                count, 1,
                "After first event, event_counter should be 1"
            );
        }
        _ => panic!("Expected LastEventNumber response"),
    }

    // Verify the event is at position 0
    let result = store_ref
        .ask(StoreCommand::GetEvents { from: 0, to: 0 })
        .await
        .unwrap();
    match result {
        StoreResponse::Events(events) => {
            assert_eq!(
                events.len(),
                1,
                "Should retrieve 1 event from position 0"
            );
            assert_eq!(events[0].delta, 10);
        }
        _ => panic!("Expected Events response"),
    }
}

/// Test event_counter increments correctly for multiple events
#[tokio::test]

async fn test_event_counter_multiple_events() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CounterTestActor>::new(
        "counter_test",
        "test_multiple",
        MemoryManager::default(),
        None,
        CounterTestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Persist 5 events
    for i in 1..=5 {
        let event = CounterEvent { delta: i };
        store_ref.ask(StoreCommand::Persist(event)).await.unwrap();
    }

    // Check event_counter = 5
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => {
            assert_eq!(count, 5, "After 5 events, event_counter should be 5");
        }
        _ => panic!("Expected LastEventNumber response"),
    }

    // Verify all events are at positions 0-4
    let result = store_ref
        .ask(StoreCommand::GetEvents { from: 0, to: 4 })
        .await
        .unwrap();
    match result {
        StoreResponse::Events(events) => {
            assert_eq!(
                events.len(),
                5,
                "Should retrieve 5 events from positions 0-4"
            );
            for (i, event) in events.iter().enumerate() {
                assert_eq!(event.delta, (i + 1) as i32);
            }
        }
        _ => panic!("Expected Events response"),
    }
}

/// Test state_counter after snapshot
#[tokio::test]

async fn test_state_counter_after_snapshot() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CounterTestActor>::new(
        "counter_test",
        "test_snapshot",
        MemoryManager::default(),
        None,
        CounterTestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Persist 3 events
    let mut actor = CounterTestActor { value: 0 };
    for i in 1..=3 {
        let event = CounterEvent { delta: i * 10 };
        store_ref
            .ask(StoreCommand::Persist(event.clone()))
            .await
            .unwrap();
        actor.apply(&event).unwrap();
    }

    // At this point: event_counter = 3, value = 60

    // Take snapshot
    store_ref
        .ask(StoreCommand::Snapshot(actor.clone()))
        .await
        .unwrap();

    // Recover and verify state_counter = event_counter
    let result = store_ref.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(recovered)) => {
            assert_eq!(
                recovered.value, 60,
                "Recovered state should have all events applied"
            );
        }
        _ => panic!("Expected recovered state"),
    }
}

/// Test recovery with events after snapshot
#[tokio::test]

async fn test_recovery_with_events_after_snapshot() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CounterTestActor>::new(
        "counter_test",
        "test_recovery",
        MemoryManager::default(),
        None,
        CounterTestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Persist 2 events and take snapshot
    let mut actor = CounterTestActor { value: 0 };
    for i in 1..=2 {
        let event = CounterEvent { delta: i * 10 };
        store_ref
            .ask(StoreCommand::Persist(event.clone()))
            .await
            .unwrap();
        actor.apply(&event).unwrap();
    }
    // value = 30, event_counter = 2
    store_ref
        .ask(StoreCommand::Snapshot(actor.clone()))
        .await
        .unwrap();
    // state_counter = 2

    // Persist 3 more events
    for i in 3..=5 {
        let event = CounterEvent { delta: i * 10 };
        store_ref
            .ask(StoreCommand::Persist(event.clone()))
            .await
            .unwrap();
        actor.apply(&event).unwrap();
    }
    // value = 150, event_counter = 5

    // Recover should apply events from position 2, 3, 4
    let result = store_ref.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(recovered)) => {
            assert_eq!(
                recovered.value, 150,
                "Recovered state should have snapshot (30) + events 3,4,5 (120) = 150"
            );
        }
        _ => panic!("Expected recovered state"),
    }
}

/// Test that recovery without snapshot works correctly
#[tokio::test]

async fn test_recovery_without_snapshot() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CounterTestActor>::new(
        "counter_test",
        "test_no_snapshot",
        MemoryManager::default(),
        None,
        CounterTestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Persist events without snapshot
    for i in 1..=3 {
        let event = CounterEvent { delta: i * 10 };
        store_ref.ask(StoreCommand::Persist(event)).await.unwrap();
    }

    // Recover should now replay events even without snapshot (bug fix)
    let result = store_ref.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(state)) => {
            // With the fix, should recover from events: 10 + 20 + 30 = 60
            assert_eq!(
                state.value, 60,
                "Should recover state by replaying events"
            );
        }
        StoreResponse::State(None) => {
            panic!("Expected state recovery from events, got None");
        }
        _ => panic!("Unexpected response type"),
    }
}

/// Test edge case: snapshot at event_counter = 0
#[tokio::test]

async fn test_snapshot_at_zero() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CounterTestActor>::new(
        "counter_test",
        "test_snap_zero",
        MemoryManager::default(),
        None,
        CounterTestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Take snapshot without any events
    let actor = CounterTestActor { value: 0 };
    store_ref
        .ask(StoreCommand::Snapshot(actor.clone()))
        .await
        .unwrap();

    // Verify event_counter is still 0
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => {
            assert_eq!(
                count, 0,
                "event_counter should remain 0 after snapshot"
            );
        }
        _ => panic!("Expected LastEventNumber response"),
    }

    // Recovery should work
    let result = store_ref.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(recovered)) => {
            assert_eq!(recovered.value, 0);
        }
        _ => panic!("Expected recovered state"),
    }
}

/// Test LastEventsFrom with different positions
#[tokio::test]

async fn test_last_events_from_positions() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CounterTestActor>::new(
        "counter_test",
        "test_last_from",
        MemoryManager::default(),
        None,
        CounterTestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Persist 5 events at positions 0-4
    for i in 1..=5 {
        let event = CounterEvent { delta: i * 10 };
        store_ref.ask(StoreCommand::Persist(event)).await.unwrap();
    }

    // LastEventsFrom(0) should return all 5 events
    let result = store_ref
        .ask(StoreCommand::LastEventsFrom(0))
        .await
        .unwrap();
    match result {
        StoreResponse::Events(events) => {
            assert_eq!(
                events.len(),
                5,
                "LastEventsFrom(0) should return 5 events"
            );
        }
        _ => panic!("Expected Events response"),
    }

    // LastEventsFrom(2) should return events from positions 2, 3, 4
    let result = store_ref
        .ask(StoreCommand::LastEventsFrom(2))
        .await
        .unwrap();
    match result {
        StoreResponse::Events(events) => {
            assert_eq!(
                events.len(),
                3,
                "LastEventsFrom(2) should return 3 events"
            );
            assert_eq!(events[0].delta, 30); // position 2
            assert_eq!(events[1].delta, 40); // position 3
            assert_eq!(events[2].delta, 50); // position 4
        }
        _ => panic!("Expected Events response"),
    }

    // LastEventsFrom(5) should return empty (no events at position 5 or beyond)
    let result = store_ref
        .ask(StoreCommand::LastEventsFrom(5))
        .await
        .unwrap();
    match result {
        StoreResponse::Events(events) => {
            assert_eq!(
                events.len(),
                0,
                "LastEventsFrom(5) should return 0 events"
            );
        }
        _ => panic!("Expected Events response"),
    }
}

/// Test complex scenario: multiple snapshots and recoveries
#[tokio::test]

async fn test_multiple_snapshots_and_recoveries() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let store = Store::<CounterTestActor>::new(
        "counter_test",
        "test_multi_snap",
        MemoryManager::default(),
        None,
        CounterTestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    let mut actor = CounterTestActor { value: 0 };

    // Cycle 1: 2 events + snapshot
    for i in 1..=2 {
        let event = CounterEvent { delta: i };
        store_ref
            .ask(StoreCommand::Persist(event.clone()))
            .await
            .unwrap();
        actor.apply(&event).unwrap();
    }
    // value = 3, event_counter = 2
    store_ref
        .ask(StoreCommand::Snapshot(actor.clone()))
        .await
        .unwrap();
    // state_counter = 2

    // Cycle 2: 3 more events + snapshot
    for i in 3..=5 {
        let event = CounterEvent { delta: i };
        store_ref
            .ask(StoreCommand::Persist(event.clone()))
            .await
            .unwrap();
        actor.apply(&event).unwrap();
    }
    // value = 15, event_counter = 5
    store_ref
        .ask(StoreCommand::Snapshot(actor.clone()))
        .await
        .unwrap();
    // state_counter = 5

    // Cycle 3: 2 more events (no snapshot)
    for i in 6..=7 {
        let event = CounterEvent { delta: i };
        store_ref
            .ask(StoreCommand::Persist(event.clone()))
            .await
            .unwrap();
        actor.apply(&event).unwrap();
    }
    // value = 28, event_counter = 7

    // Recovery should use snapshot from cycle 2 and apply events from cycle 3
    let result = store_ref.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(recovered)) => {
            assert_eq!(
                recovered.value, 28,
                "Should recover snapshot (15) + events 6,7 (13) = 28"
            );
        }
        _ => panic!("Expected recovered state"),
    }
}

/// Test that event_counter works correctly with encryption
#[tokio::test]

async fn test_event_counter_with_encryption() {
    build_tracing_subscriber();
    let (system, mut runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let encrypt_key =
        EncryptedKey::new(b"0123456789abcdef0123456789abcdef").unwrap();

    let store = Store::<CounterTestActor>::new(
        "counter_test",
        "test_encrypted",
        MemoryManager::default(),
        Some(encrypt_key),
        CounterTestActor::create_initial(()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Persist 3 encrypted events
    for i in 1..=3 {
        let event = CounterEvent { delta: i * 10 };
        store_ref.ask(StoreCommand::Persist(event)).await.unwrap();
    }

    // Verify event_counter = 3
    let result = store_ref.ask(StoreCommand::LastEventNumber).await.unwrap();
    match result {
        StoreResponse::LastEventNumber(count) => {
            assert_eq!(
                count, 3,
                "Encrypted store should have event_counter = 3"
            );
        }
        _ => panic!("Expected LastEventNumber response"),
    }

    // Verify events can be retrieved and decrypted
    let result = store_ref
        .ask(StoreCommand::GetEvents { from: 0, to: 2 })
        .await
        .unwrap();
    match result {
        StoreResponse::Events(events) => {
            assert_eq!(events.len(), 3);
            assert_eq!(events[0].delta, 10);
            assert_eq!(events[1].delta, 20);
            assert_eq!(events[2].delta, 30);
        }
        _ => panic!("Expected Events response"),
    }
}
