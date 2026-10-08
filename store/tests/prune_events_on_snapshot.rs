//! Pruning of covered events (`prune_events_on_snapshot`).
//!
//! With pruning on, every snapshot deletes the events it covers, so
//! storage stays flat (one snapshot plus at most `snapshot_every`
//! pending events) instead of keeping a history nobody replays.
//! Counters stay monotonic: recovery replays by key range, so reusing
//! keys would replay the wrong events.

#[macro_use]
mod helpers;
use ave_actors_store::{
    memory::MemoryManager,
    store::{PersistentActor, StoreCommand, StoreResponse},
};

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorSystem, Error as ActorError, Event, Handler,
    Message, Response,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(
    Debug, Clone, Default, borsh::BorshSerialize, borsh::BorshDeserialize,
)]
struct TestActorState {
    value: i32,
}

#[derive(Debug)]
struct PruneActor {
    state_ptr: Arc<TestActorState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestMessage;
impl Message for TestMessage {}

#[derive(Debug, Clone)]
struct TestResponse;
impl Response for TestResponse {}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
struct TestEvent {
    delta: i32,
}
impl Event for TestEvent {}

#[async_trait]
impl Actor for PruneActor {
    type Message = TestMessage;
    type Response = TestResponse;
    type Event = TestEvent;
    type SinkEvent = Self::Event;
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("PruneActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for PruneActor {
    async fn handle_message(
        &mut self,
        _sender: ave_actors_actor::ActorPath,
        _msg: TestMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<TestResponse, ActorError> {
        Ok(TestResponse)
    }
}

#[async_trait]
impl PersistentActor for PruneActor {
    type InitParams = ();
    type State = TestActorState;

    fn prune_events_on_snapshot() -> bool {
        true
    }

    fn create_initial(_: ()) -> Self {
        Self {
            state_ptr: Arc::new(TestActorState::default()),
        }
    }

    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError> {
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

async fn persist_deltas(
    store: &ave_actors_actor::ActorRef<
        ave_actors_store::store::Store<PruneActor>,
    >,
    from: i32,
    to: i32,
    running: &mut i32,
) {
    for i in from..=to {
        *running += i;
        store
            .ask(StoreCommand::Persist {
                event: Arc::new(TestEvent { delta: i }),
                state: Arc::new(TestActorState { value: *running }),
                snapshot_every: Some(2),
            })
            .await
            .unwrap();
    }
}

fn event_count(
    response: ave_actors_store::store::StoreResponse<PruneActor>,
) -> usize {
    match response {
        StoreResponse::Events(events) => events.len(),
        other => panic!("expected Events, got {other:?}"),
    }
}

#[test(tokio::test)]
async fn test_prune_keeps_storage_flat() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();
    let store = store_new!(
        PruneActor,
        "test",
        "prune_flat",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // 5 events, snapshots at 2 and 4 prune [0..2) then [0..4):
    // only the last uncovered event may remain.
    let mut running = 0;
    persist_deltas(&store_ref, 1, 5, &mut running).await;

    let result = store_ref
        .ask(StoreCommand::GetEvents { from: 0, to: 10 })
        .await
        .unwrap();
    assert_eq!(
        event_count(result),
        1,
        "only the event past the last snapshot coverage may remain"
    );

    // Counters stay monotonic: keys are positional in recovery and
    // must never be reused.
    let result = store_ref.ask(StoreCommand::NextEventNumber).await.unwrap();
    match result {
        StoreResponse::NextEventNumber(count) => assert_eq!(count, 5),
        other => panic!("expected NextEventNumber, got {other:?}"),
    }
}

#[test(tokio::test)]
async fn test_prune_recovery_replays_across_boundary() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();
    let store = store_new!(
        PruneActor,
        "test",
        "prune_recover",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    let mut running = 0;
    persist_deltas(&store_ref, 1, 5, &mut running).await;
    drop(store_ref);

    // Fresh store over the same backend: snapshot (value 10 at
    // coverage 4) plus the one surviving event (delta 5).
    let store2 = store_new!(
        PruneActor,
        "test",
        "prune_recover",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();
    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();
    match result {
        StoreResponse::State(Some(state)) => {
            assert_eq!(
                state.value, 15,
                "snapshot(10) + surviving event(5) must fold to 15"
            );
        }
        other => panic!("expected recovered state, got {other:?}"),
    }
}

/// Crash-mid-prune residue must be harmless: leftover events below
/// the snapshot coverage are skipped by key-range replay, never
/// double-applied. Simulates the interrupted delete by putting a
/// covered event back by hand.
#[test(tokio::test)]
async fn test_prune_partial_residue_is_ignored() {
    use ave_actors_store::database::{Collection, DbManager};

    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();
    let store = store_new!(
        PruneActor,
        "test",
        "prune_residue",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // Snapshot at 2 and 4 prunes everything below coverage.
    let mut running = 0;
    persist_deltas(&store_ref, 1, 4, &mut running).await;
    drop(store_ref);

    // Smuggle a covered event (key 1, delta 2) back in, as if the
    // prune delete had died halfway through it.
    let mut events = memory_manager
        .create_collection("test_events", "prune_residue")
        .unwrap();
    let mut row = vec![b'A', b'V', b'E', 0x00];
    row.extend_from_slice(&1u32.to_le_bytes());
    row.extend_from_slice(&borsh::to_vec(&TestEvent { delta: 2 }).unwrap());
    events.put("00000000000000000001", &row).unwrap();

    let store2 = store_new!(
        PruneActor,
        "test",
        "prune_residue",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref2 = system.create_root_actor("store2", store2).await.unwrap();
    let result = store_ref2.ask(StoreCommand::Recover).await.unwrap();
    match result {
        // Snapshot(10) replays nothing below coverage: the smuggled
        // delta-2 must NOT apply a second time (10, not 12).
        StoreResponse::State(Some(state)) => {
            assert_eq!(
                state.value, 10,
                "covered leftover must be skipped, not re-applied"
            );
        }
        other => panic!("expected recovered state, got {other:?}"),
    }
}

/// A manual `Snapshot` command prunes too, not just the automatic
/// cadence: same function, same guarantee.
#[test(tokio::test)]
async fn test_prune_on_manual_snapshot() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let memory_manager = MemoryManager::default();
    let store = store_new!(
        PruneActor,
        "test",
        "prune_manual",
        memory_manager.clone(),
        None,
        Arc::new(TestActorState::default()),
    )
    .unwrap();
    let store_ref = system.create_root_actor("store", store).await.unwrap();

    // No auto-snapshot on this path: pass a huge cadence, then snap
    // by hand over the running state.
    let mut running = 0;
    for i in 1..=3 {
        running += i;
        store_ref
            .ask(StoreCommand::Persist {
                event: Arc::new(TestEvent { delta: i }),
                state: Arc::new(TestActorState { value: running }),
                snapshot_every: Some(1_000_000),
            })
            .await
            .unwrap();
    }
    let result = store_ref
        .ask(StoreCommand::GetEvents { from: 0, to: 10 })
        .await
        .unwrap();
    assert_eq!(event_count(result), 3);

    store_ref
        .ask(StoreCommand::Snapshot(Arc::new(TestActorState {
            value: running,
        })))
        .await
        .unwrap();
    let result = store_ref
        .ask(StoreCommand::GetEvents { from: 0, to: 10 })
        .await
        .unwrap();
    assert_eq!(
        event_count(result),
        0,
        "manual snapshot must prune everything it covers"
    );
}
