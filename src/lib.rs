

//! Core library for the Ave-Actors framework.
//! Provides the foundational components for building actor-based applications.
//! This library includes the core actor model, message passing, and persistence layers.
//! It is designed to be modular and extensible, allowing developers to build custom actors and message types.

pub use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, ChildAction,
    CustomIntervalStrategy, Error as ActorError, Event, 
    FixedIntervalStrategy, Handler, Message, NoIntervalStrategy,
    Response, RetryActor, RetryMessage, RetryStrategy, Sink,
    Strategy, Subscriber, SupervisionStrategy, SystemEvent, SystemRef,
    SystemRunner, EncryptedKey, NotPersistentActor
};

#[cfg(any(feature = "rocksdb", feature = "sqlite"))]
pub use ave_actors_store::{
    Error as StoreError,
    database::{Collection, DbManager, State},
    store::{PersistentActor, FullPersistence, LightPersistence, Store, StoreCommand, StoreResponse}, 
    config::*
};

#[cfg(feature = "rocksdb")]
pub use ave_actors_rocksdb::{RocksDbManager, RocksDbStore};

#[cfg(feature = "export-rocksdb")]
pub use ave_actors_rocksdb::rocksdb;

#[cfg(feature = "sqlite")]
pub use ave_actors_sqlite::{SqliteCollection, SqliteManager};

#[cfg(feature = "export-sqlite")]
pub use ave_actors_sqlite::rusqlite;
