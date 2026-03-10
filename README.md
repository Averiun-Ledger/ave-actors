# ave-actors

Open-source actor framework for Rust with async message passing, supervision, event broadcasting, and event-sourced persistence.

This repository is the public home of the `ave-actors` workspace. It includes the core actor runtime, the persistence layer, and database backends for SQLite and RocksDB.

## What it provides

- Typed actors built on Tokio
- `tell`, `ask`, timeout-aware requests, and graceful shutdown
- Parent/child actor hierarchies with supervision strategies
- Event broadcasting with subscriber sinks
- Event-sourced persistence with snapshots
- Pluggable storage backends
- Optional at-rest encryption for persisted data

## Workspace crates

| Crate | Purpose |
|---|---|
| [`ave-actors`](.) | Aggregated crate that re-exports the main public API |
| [`ave-actors-actor`](./actor) | Actor runtime, actor system, paths, supervision, retries, sinks |
| [`ave-actors-store`](./store) | Event-sourced persistence layer and backend traits |
| [`ave-actors-sqlite`](./databases/sqlite_db) | SQLite backend for persistent actors |
| [`ave-actors-rocksdb`](./databases/rocksdb_db) | RocksDB backend for persistent actors |

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `sqlite` | Yes | Enables the SQLite backend and store re-exports |
| `rocksdb` | No | Enables the RocksDB backend |
| `export-sqlite` | No | Re-exports `rusqlite` |
| `export-rocksdb` | No | Re-exports `rocksdb` |

## Quick start

The root crate re-exports the actor API, so you can start with a simple actor without importing subcrates directly.

```rust,ignore
use ave_actors::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, Handler, Message,
    NotPersistentActor, Response,
};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct Ping;

#[derive(Clone)]
struct Pong;

impl Message for Ping {}
impl Response for Pong {}

#[derive(Clone)]
struct MyActor;

impl NotPersistentActor for MyActor {}

#[async_trait]
impl Actor for MyActor {
    type Message = Ping;
    type Event = ();
    type Response = Pong;

    fn get_span(id: &str, _parent: Option<tracing::Span>) -> tracing::Span {
        tracing::info_span!("MyActor", id)
    }
}

#[async_trait]
impl Handler<MyActor> for MyActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: Ping,
        _ctx: &mut ActorContext<MyActor>,
    ) -> Result<Pong, ave_actors::ActorError> {
        Ok(Pong)
    }
}

#[tokio::main]
async fn main() {
    let graceful = CancellationToken::new();
    let crash = CancellationToken::new();
    let (system, mut runner) = ActorSystem::create(graceful, crash);

    let actor_ref: ActorRef<MyActor> =
        system.create_root_actor("my-actor", MyActor).await.unwrap();

    let _reply = actor_ref.ask(Ping).await.unwrap();

    system.stop_system();
    runner.run().await;
}
```

## Persistent actors

If you need event sourcing, use the persistence layer from the root crate or directly from [`ave-actors-store`](./store).

With the default `sqlite` feature, the root crate already re-exports:

- `PersistentActor`
- `LightPersistence`
- `FullPersistence`
- `DbManager`, `Collection`, `State`
- `SqliteManager`

For RocksDB:

```toml
[dependencies]
ave-actors = { version = "0.9.0", features = ["rocksdb"] }
```

If you prefer finer control, depend on subcrates directly:

```toml
[dependencies]
ave-actors-actor = "0.4.0"
ave-actors-store = "0.4.0"
ave-actors-sqlite = "0.3.0"
```

## Which crate should I use?

- Use `ave-actors` if you want the simplest entry point and re-exports.
- Use `ave-actors-actor` if you only need the actor runtime.
- Use `ave-actors-store` if you are implementing persistence or a custom backend.
- Use `ave-actors-sqlite` for embedded single-node persistence.
- Use `ave-actors-rocksdb` for higher write throughput or larger persistent workloads.

## Documentation map

- Actor runtime: [`actor/README.md`](./actor/README.md)
- Persistence layer: [`store/README.md`](./store/README.md)
- SQLite backend: [`databases/sqlite_db/README.md`](./databases/sqlite_db/README.md)
- RocksDB backend: [`databases/rocksdb_db/README.md`](./databases/rocksdb_db/README.md)

## Development

Build the whole workspace:

```bash
cargo build --workspace
```

Run all tests:

```bash
cargo test --workspace
```

Format the workspace:

```bash
cargo fmt --all
```

## Open source

`ave-actors` is free and open-source software. You can use it, study it, modify it, and redistribute it under the terms of the Apache License 2.0.

## License

This project is a fork of [rush-rs](https://github.com/kore-ledger/rush-rs), originally developed by Kore Ledger, SL, modified in 2025 by Averiun Ledger, SL, and distributed under the same Apache-2.0 license.