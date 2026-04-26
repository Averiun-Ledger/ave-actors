# ave-actors-rocksdb

RocksDB backend for [`ave-actors-store`](https://crates.io/crates/ave-actors-store). It stores actor events and snapshots in column families and exposes the `DbManager`, `Collection`, and `State` contracts expected by the store crate.

This crate is part of the [ave-actors](https://github.com/Averiun-Ledger/ave-actors) workspace.

API documentation is available on [docs.rs](https://docs.rs/ave-actors-rocksdb).

## Install

```toml
[dependencies]
ave-actors-rocksdb = { version = "0.4.0", features = ["rocksdb"] }
ave-actors-store = "0.5.0"
```

## Features

| Feature | Description |
|---|---|
| `rocksdb` | Enables `RocksDbManager` and `RocksDbStore` |
| `export-rocksdb` | Re-exports `rocksdb` |

## Quick start

```rust
use ave_actors_rocksdb::RocksDbManager;
use ave_actors_store::{
    config::{MachineProfile, MachineSpec},
    database::{Collection, DbManager, State},
};

fn main() -> Result<(), ave_actors_store::Error> {
    let path = std::env::temp_dir().join("ave-actors-rocksdb-docs");
    let manager = RocksDbManager::new(
        &path,
        true,
        Some(MachineSpec::Profile(MachineProfile::Medium)),
    )?;

    let mut events = manager.create_collection("events", "actor-1")?;
    Collection::put(&mut events, "00000000000000000001", b"event-1")?;
    assert_eq!(
        Collection::get(&events, "00000000000000000001")?,
        b"event-1"
    );

    let mut state = manager.create_state("snapshots", "actor-1")?;
    State::put(&mut state, b"snapshot-v1")?;
    assert_eq!(State::get(&state)?, b"snapshot-v1");

    Ok(())
}
```

## Main types

| Type | Purpose |
|---|---|
| `RocksDbManager` | Opens the database directory, creates column families, and returns store handles |
| `RocksDbStore` as `Collection` | Ordered key-value store for events |
| `RocksDbStore` as `State` | Single-value store for snapshots |

## API contract

### `RocksDbManager::new`

```text
pub fn new(
    path: &PathBuf,
    durability: bool,
    spec: Option<MachineSpec>,
) -> Result<RocksDbManager, Error>
```

- Receives a database directory, a durability flag, and an optional machine sizing profile.
- Creates the directory if needed and opens all existing column families.
- Returns a configured `RocksDbManager`.
- Returns `Error::CreateStore` if the directory or database cannot be created.

### `DbManager::create_collection`

```text
fn create_collection(&self, name: &str, prefix: &str) -> Result<RocksDbStore, Error>
```

- Receives the column family name and the logical prefix used to isolate one actor's event keys.
- Returns a `RocksDbStore` implementing `Collection`.
- Returns `Error::CreateStore` if the column family cannot be created.

### `DbManager::create_state`

```text
fn create_state(&self, name: &str, prefix: &str) -> Result<RocksDbStore, Error>
```

- Receives the column family name and the prefix used to isolate one actor's snapshot.
- Returns a `RocksDbStore` implementing `State`.
- Returns `Error::CreateStore` if the column family cannot be created.

## `Collection` operations

| Method | Receives | Returns | Notes |
|---|---|---|---|
| `get(&self, key)` | Event key as `&str` | `Result<Vec<u8>, Error>` | Reads raw bytes from `prefix.key` |
| `put(&mut self, key, data)` | Event key and raw bytes | `Result<(), Error>` | Replaces existing values |
| `del(&mut self, key)` | Event key | `Result<(), Error>` | Returns `Error::EntryNotFound` if the key does not exist |
| `last(&self)` | Nothing | `Result<Option<(String, Vec<u8>)>, Error>` | Returns the last key/value pair for this prefix |
| `iter(&self, reverse)` | `bool` for descending order | `Result<Box<dyn Iterator<...>>, Error>` | Yields keys without the prefix |
| `purge(&mut self)` | Nothing | `Result<(), Error>` | Deletes every entry in the current prefix range |

## `State` operations

| Method | Receives | Returns | Notes |
|---|---|---|---|
| `get(&self)` | Nothing | `Result<Vec<u8>, Error>` | Returns the current snapshot bytes |
| `put(&mut self, data)` | Raw bytes | `Result<(), Error>` | Replaces the previous snapshot |
| `del(&mut self)` | Nothing | `Result<(), Error>` | Returns `Error::EntryNotFound` if the snapshot does not exist |
| `purge(&mut self)` | Nothing | `Result<(), Error>` | Removes only the exact state key |

## Error model

| Error | Meaning |
|---|---|
| `Error::CreateStore` | The manager, directory, or column family could not be created |
| `Error::EntryNotFound` | The requested event or snapshot does not exist |
| `Error::Get` | A read failed for an existing logical key |
| `Error::Store` | A backend operation failed, such as column family access, write, delete, purge, or flush |

## Behavior notes

- Collections are ordered lexicographically by key. Zero-padded sequence numbers are recommended if keys represent event numbers.
- Collection keys are stored as `prefix.key`; state values use the exact `prefix` as their key.
- `durability = true` enables synchronous writes through RocksDB `WriteOptions::set_sync(true)`.
- `RocksDbManager::stop()` flushes the WAL and then flushes each column family memtable.
