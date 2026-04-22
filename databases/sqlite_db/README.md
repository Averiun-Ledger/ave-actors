# ave-actors-sqlite

SQLite backend for [`ave-actors-store`](https://crates.io/crates/ave-actors-store). It stores actor events and snapshots in a local `database.db` file and exposes the `DbManager`, `Collection`, and `State` contracts expected by the store crate.

This crate is part of the [ave-actors](https://github.com/Averiun-Ledger/ave-actors) workspace.

API documentation is available on [docs.rs](https://docs.rs/ave-actors-sqlite).

## Install

```toml
[dependencies]
ave-actors-sqlite = { version = "0.5.1", features = ["sqlite"] }
ave-actors-store = "0.4.1"
```

## Features

| Feature | Description |
|---|---|
| `sqlite` | Enables `SqliteManager` and `SqliteCollection` |
| `export-sqlite` | Re-exports `rusqlite` |

## Quick start

```rust
use ave_actors_sqlite::SqliteManager;
use ave_actors_store::{
    config::{MachineProfile, MachineSpec},
    database::{Collection, DbManager, State},
};

fn main() -> Result<(), ave_actors_store::Error> {
    let path = std::env::temp_dir().join("ave-actors-sqlite-docs");
    let manager = SqliteManager::new(
        &path,
        true,
        Some(MachineSpec::Profile(MachineProfile::Small)),
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
| `SqliteManager` | Opens the database directory, creates tables, and returns store handles |
| `SqliteCollection` | Ordered key-value store for events |
| `SqliteCollection` as `State` | Single-value store for snapshots |

## API contract

### `SqliteManager::new`

```text
pub fn new(
    path: &PathBuf,
    durability: bool,
    spec: Option<MachineSpec>,
) -> Result<SqliteManager, Error>
```

- Receives a directory path, a durability flag, and an optional machine sizing profile.
- Creates the directory if needed and opens `<path>/database.db`.
- Returns a configured `SqliteManager`.
- Returns `Error::CreateStore` if the directory or database cannot be created.

### `DbManager::create_collection`

```text
fn create_collection(&self, identifier: &str, prefix: &str) -> Result<SqliteCollection, Error>
```

- Receives the SQLite table name and the logical prefix used to isolate one actor's keys.
- Returns a `SqliteCollection` implementing `Collection`.
- Returns `Error::CreateStore` if the table name is invalid or the table cannot be created.

SQLite identifiers are validated with this pattern:

```text
[A-Za-z_][A-Za-z0-9_]*
```

### `DbManager::create_state`

```text
fn create_state(&self, identifier: &str, prefix: &str) -> Result<SqliteCollection, Error>
```

- Receives the SQLite table name and the prefix used to isolate one actor's snapshot.
- Returns a `SqliteCollection` implementing `State`.
- Returns `Error::CreateStore` if the table name is invalid or the table cannot be created.

## `Collection` operations

| Method | Receives | Returns | Notes |
|---|---|---|---|
| `get(&self, key)` | Event key as `&str` | `Result<Vec<u8>, Error>` | Returns raw bytes for `prefix.key` |
| `put(&mut self, key, data)` | Event key and raw bytes | `Result<(), Error>` | Replaces existing values |
| `del(&mut self, key)` | Event key | `Result<(), Error>` | Returns `Error::EntryNotFound` if the key does not exist |
| `last(&self)` | Nothing | `Result<Option<(String, Vec<u8>)>, Error>` | Returns the last key/value pair for this prefix |
| `iter(&self, reverse)` | `bool` for descending order | `Result<Box<dyn Iterator<...>>, Error>` | Yields keys without the prefix |
| `purge(&mut self)` | Nothing | `Result<(), Error>` | Deletes every row under the current prefix |

## `State` operations

| Method | Receives | Returns | Notes |
|---|---|---|---|
| `get(&self)` | Nothing | `Result<Vec<u8>, Error>` | Returns the current snapshot bytes |
| `put(&mut self, data)` | Raw bytes | `Result<(), Error>` | Replaces the previous snapshot |
| `del(&mut self)` | Nothing | `Result<(), Error>` | Returns `Error::EntryNotFound` if the snapshot does not exist |
| `purge(&mut self)` | Nothing | `Result<(), Error>` | Succeeds even if the state is already empty |

## Error model

| Error | Meaning |
|---|---|
| `Error::CreateStore` | The manager, directory, or table could not be created |
| `Error::EntryNotFound` | The requested event or snapshot does not exist |
| `Error::Get` | A read failed for an existing logical key |
| `Error::Store` | A backend operation failed, such as insert, delete, purge, or WAL maintenance |

## Behavior notes

- Collections are ordered lexicographically by key. Zero-padded sequence numbers are recommended if keys represent event numbers.
- Each store handle uses dedicated read and write connections.
- `SqliteManager::stop()` runs `PRAGMA optimize` and checkpoints the WAL before shutdown.
