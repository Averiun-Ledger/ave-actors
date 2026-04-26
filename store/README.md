# ave-actors-store

Event-sourced persistence for actors built on [`ave-actors-actor`](https://crates.io/crates/ave-actors-actor). Actors record state changes as an immutable event log and recover their state on restart by replaying events and loading snapshots.

This crate is part of the [ave-actors](https://github.com/Averiun-Ledger/ave-actors) workspace.

API documentation is available on [docs.rs](https://docs.rs/ave-actors-store).

---

## Install

```toml
[dependencies]
ave-actors-store = "0.5.0"
ave-actors-actor = "0.5.0"
async-trait = "0.1"
borsh = { version = "1", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tokio-util = "0.7"
tracing = "0.1"
```

---

## Core concepts

| Concept | Description |
|---|---|
| `PersistentActor` | Trait extending `Actor` with event sourcing — implement `apply` and call `persist` |
| `LightPersistence` | Strategy: stores event + state snapshot on every write (fast recovery) |
| `FullPersistence` | Strategy: stores events only, replays on recovery (smaller footprint, full audit trail) |
| `InitializedActor<A>` | Required wrapper returned by `PersistentActor::initial(params)` |
| `DbManager<C, S>` | Backend factory trait — implement to plug in a custom database |
| `Collection` | Ordered key-value storage for the event log |
| `State` | Single-value storage for state snapshots |

---

## Main API

| API | Receives | Returns | Purpose |
|---|---|---|---|
| `PersistentActor::create_initial` | `InitParams` | `Self` | Builds the base state used on first start and recovery without a snapshot |
| `PersistentActor::initial` | `InitParams` | `InitializedActor<Self>` | Wraps the actor so the actor system accepts it as persistent |
| `PersistentActor::apply` | `&Event` | `Result<(), ActorError>` | Applies one event to in-memory state |
| `PersistentActor::start_store` | store `name`, optional `prefix`, `ActorContext`, backend manager, optional `EncryptedKey` | `Result<(), ActorError>` | Opens the backend and recovers persisted state into the actor |
| `PersistentActor::persist` | `&Event`, `ActorContext` | `Result<(), ActorError>` | Applies and durably records one event, rolling back memory on failure |
| `PersistentActor::snapshot` | `ActorContext` | `Result<(), ActorError>` | Forces an immediate snapshot of the current actor state |

---

## Quick start

```rust,ignore
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, Error as ActorError, Event, Handler,
    Message, Response,
};
use ave_actors_store::{
    store::{FullPersistence, PersistentActor},
    memory::MemoryManager,
};
use async_trait::async_trait;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use tracing::info_span;

// --- Events ---

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
enum CounterEvent {
    Incremented(i32),
}

impl Event for CounterEvent {}

// --- Messages & responses ---

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CounterMsg { Increment(i32), GetValue }

#[derive(Debug, Clone, PartialEq)]
enum CounterResp { Ok, Value(i32) }

impl Message for CounterMsg {}
impl Response for CounterResp {}

// --- Actor ---

#[derive(Debug, Clone, Default, BorshSerialize, BorshDeserialize)]
struct Counter { value: i32 }

#[async_trait]
impl Actor for Counter {
    type Message  = CounterMsg;
    type Event    = CounterEvent;
    type Response = CounterResp;

    fn get_span(id: &str, _parent: Option<tracing::Span>) -> tracing::Span {
        info_span!("Counter", id)
    }

    async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        // start_store creates the "store" child actor, opens the backend,
        // and recovers any previously persisted state into `self`.
        self.start_store("counter", None, ctx, MemoryManager::default(), None).await
    }
}

#[async_trait]
impl Handler<Counter> for Counter {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: CounterMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<CounterResp, ActorError> {
        match msg {
            CounterMsg::Increment(n) => {
                // persist applies the event to self and saves it durably.
                // If persistence fails, the in-memory state is rolled back.
                self.persist(&CounterEvent::Incremented(n), ctx).await?;
                Ok(CounterResp::Ok)
            }
            CounterMsg::GetValue => Ok(CounterResp::Value(self.value)),
        }
    }
}

// --- PersistentActor ---

impl PersistentActor for Counter {
    type Persistence = FullPersistence;
    type InitParams  = ();

    fn create_initial(_params: ()) -> Self {
        Self::default()
    }

    fn apply(&mut self, event: &CounterEvent) -> Result<(), ActorError> {
        match event {
            CounterEvent::Incremented(n) => self.value += n,
        }
        Ok(())
    }
}
```

Create and use the actor:

```rust,ignore
use ave_actors_actor::{ActorSystem, ActorRef};
use ave_actors_store::store::PersistentActor;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let graceful = CancellationToken::new();
    let crash    = CancellationToken::new();
    let (system, mut runner) = ActorSystem::create(graceful, crash);

    // Use PersistentActor::initial() — the only accepted way to create a persistent actor.
    let counter: ActorRef<Counter> = system
        .create_root_actor("counter", Counter::initial(()))
        .await
        .unwrap();

    counter.ask(CounterMsg::Increment(10)).await.unwrap();
    let resp = counter.ask(CounterMsg::GetValue).await.unwrap();
    assert_eq!(resp, CounterResp::Value(10));

    system.stop_system();
    runner.run().await;
}
```

---

## Persistence strategies

| Strategy | Write | Recovery | When to use |
|---|---|---|---|
| `LightPersistence` | Event + state snapshot | Load snapshot (no replay) | When recovery speed matters most |
| `FullPersistence` | Event only | Load last snapshot + replay remaining events | When storage efficiency or a full audit trail matters |

For `FullPersistence`, snapshots are taken automatically every N events (default: 100). Override to tune:

```rust,ignore
fn snapshot_every() -> Option<u64> {
    Some(50) // snapshot every 50 events
}
```

Set `compact_on_snapshot() -> bool` to `true` to delete events already covered by a snapshot and reduce storage use:

```rust,ignore
fn compact_on_snapshot() -> bool {
    true
}
```

---

## Encryption

Pass an [`EncryptedKey`] to `start_store` to encrypt all events and snapshots at rest using XChaCha20-Poly1305:

```rust,ignore
use ave_actors_actor::EncryptedKey;

async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
    let raw_key: [u8; 32] = /* load from secure storage */;
    let key = EncryptedKey::new(&raw_key).map_err(|e| ActorError::Helper {
        name: "key".into(),
        reason: e.to_string(),
    })?;
    self.start_store("my-actor", None, ctx, MemoryManager::default(), Some(key)).await
}
```

The key itself is held in memory encrypted via ASCON AEAD (see `ave-actors-actor`'s `EncryptedKey`).

---

## Storage backends

| Crate | Backend | Use case |
|---|---|---|
| `ave-actors-store` (built-in) | `MemoryManager` | Tests and ephemeral state |
| `ave-actors-sqlite` | SQLite (via `rusqlite`) | Single-node, embedded persistence |
| `ave-actors-rocksdb` | RocksDB | High-throughput, multi-actor workloads |

All three backends implement `DbManager<C, S>` and can be swapped without changing actor code.

---

## Implementing a custom backend

Implement `DbManager`, `Collection`, and `State` from `ave_actors_store::database`:

```rust,ignore
use ave_actors_store::database::{DbManager, Collection, State};

struct MyManager { /* ... */ }
struct MyStore   { /* ... */ }

impl DbManager<MyStore, MyStore> for MyManager {
    fn create_collection(&self, name: &str, prefix: &str) -> Result<MyStore, Error> { /* ... */ }
    fn create_state(&self, name: &str, prefix: &str)      -> Result<MyStore, Error> { /* ... */ }
}

impl Collection for MyStore { /* ... */ }
impl State for MyStore      { /* ... */ }
```

Use the `test_store_trait!` macro from `ave_actors_store` to verify your implementation:

```rust,ignore
ave_actors_store::test_store_trait! {
    my_backend_tests: MyManager: MyStore
}
```
