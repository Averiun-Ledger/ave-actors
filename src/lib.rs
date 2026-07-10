#![doc = include_str!("../README.md")]

pub use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, ActorSystemConfig,
    ChildAction, CustomIntervalStrategy, EncryptedKey, Error as ActorError,
    Event, ExponentialBackoffStrategy, Handler, IntervalStrategy, IntoActor,
    Message, NoIntervalStrategy, NotPersistentActor, OverflowStrategy,
    ParentRef, Response, RetryActor, RetryMessage, RetryPolicy, RetryStrategy,
    ShutdownReason, Sink, SinkEntry, Strategy, Subscriber, SupervisionStrategy,
    SystemEvent, SystemRef, SystemRunner, TimerKey,
};

#[cfg(any(feature = "rocksdb", feature = "sqlite"))]
pub use ave_actors_store::{
    Error as StoreError, StoreOperation,
    config::*,
    database::{Collection, DbManager, State},
    store::{
        FullPersistence, InitializedActor, LightPersistence, PersistentActor,
        Store, StoreCommand, StoreResponse,
    },
};

#[cfg(feature = "rocksdb")]
pub use ave_actors_rocksdb::{RocksDbManager, RocksDbStore};

#[cfg(feature = "export-rocksdb")]
pub use ave_actors_rocksdb::rocksdb;

#[cfg(feature = "sqlite")]
pub use ave_actors_sqlite::{SqliteCollection, SqliteManager};

#[cfg(feature = "export-sqlite")]
pub use ave_actors_sqlite::rusqlite;

#[cfg(feature = "prometheus")]
pub mod prometheus {
    pub use prometheus_client::encoding::text::encode;
    pub use prometheus_client::registry::Registry;

    use ave_actors_actor::metrics::ActorMetrics;
    use ave_actors_actor::{ActorSystem, SystemRef, SystemRunner};
    use ave_actors_store::metrics::{STORE_METRICS_HELPER, StoreMetrics};
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    /// Creates an actor system and registers actor + store metrics in the provided registry.
    pub fn create_system_with_registry(
        graceful: CancellationToken,
        crash: CancellationToken,
        registry: &mut Registry,
    ) -> (SystemRef, SystemRunner) {
        let (mut system, runner) = ActorSystem::create(graceful, crash);
        register(registry, &mut system);
        (system, runner)
    }

    /// Registers actor and store metrics in `registry` and installs the store metrics helper.
    pub fn register(registry: &mut Registry, system: &mut SystemRef) {
        let actor_metrics = Arc::new(ActorMetrics::new());
        actor_metrics.register_into(registry);
        system.set_actor_metrics(Some(actor_metrics));

        let store_metrics = Arc::new(StoreMetrics::new());
        store_metrics.register_into(registry);
        system.add_helper(STORE_METRICS_HELPER, store_metrics);
    }

    /// Convenience helper that encodes a registry into a string.
    pub fn encode_registry(
        registry: &Registry,
    ) -> Result<String, std::fmt::Error> {
        let mut body = String::new();
        encode(&mut body, registry)?;
        Ok(body)
    }
}
