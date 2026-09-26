//! Event-sourced persistence via [`PersistentActor`].
//!
//! This module provides a Copy-on-Write style persistence trait where actor
//! state is managed as `Arc<State>`, eliminating deep clones on the hot path.

use crate::{
    database::{BatchOp, BatchWrite, Collection, DbManager, State},
    error::{Error, StoreOperation},
};

use ave_actors_actor::{
    Actor, ActorContext, ActorPath, EncryptedKey, Error as ActorError, Event,
    Handler, IntoActor, Message, Response,
};

use async_trait::async_trait;

use borsh::{BorshDeserialize, BorshSerialize};

use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit},
};
use getrandom::fill as fill_random;

use tracing::{debug, error, info_span, warn};

use std::fmt::Debug;
use std::sync::Arc;
#[cfg(feature = "prometheus")]
use std::time::Instant;

/// Nonce size for XChaCha20-Poly1305 encryption.
const NONCE_SIZE: usize = 24;

fn store_error(operation: StoreOperation, reason: impl ToString) -> Error {
    Error::Store {
        operation,
        reason: reason.to_string(),
        source: None,
    }
}

fn store_error_with_source(
    operation: StoreOperation,
    reason: impl ToString,
    source: ActorError,
) -> Error {
    Error::Store {
        operation,
        reason: reason.to_string(),
        source: Some(source),
    }
}

fn actor_store_error(
    operation: StoreOperation,
    reason: impl ToString,
) -> ActorError {
    ActorError::StoreOperation {
        operation: operation.to_string(),
        reason: reason.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Persistence types
// ---------------------------------------------------------------------------

/// Selects the persistence strategy used by a [`PersistentActor`].
///
/// `Light` persists only the latest state snapshot for fast recovery; `Full`
/// persists every event and reconstructs state by replay, trading recovery
/// speed for a complete audit trail.
#[derive(Debug, Clone)]
pub enum PersistenceType {
    /// Only the latest state snapshot is persisted; no events are stored.
    /// Recovery loads the snapshot directly.
    Light,
    /// Only events are stored; state is reconstructed by replaying them.
    Full,
}

/// Marker type that selects [`PersistenceType::Light`] for a [`PersistentActor`].
pub struct LightPersistence;

/// Marker type that selects [`PersistenceType::Full`] for a [`PersistentActor`].
pub struct FullPersistence;

/// Type-level selector that maps a marker type to a [`PersistenceType`] value.
pub trait Persistence {
    /// Returns the runtime persistence mode represented by this marker type.
    fn get_persistence() -> PersistenceType;
}

impl Persistence for LightPersistence {
    fn get_persistence() -> PersistenceType {
        PersistenceType::Light
    }
}

impl Persistence for FullPersistence {
    fn get_persistence() -> PersistenceType {
        PersistenceType::Full
    }
}

// ---------------------------------------------------------------------------
// InitializedActor
// ---------------------------------------------------------------------------

/// Wrapper that guarantees a [`PersistentActor`] was constructed via
/// [`PersistentActor::initial`].
#[derive(Debug)]
pub struct InitializedActor<A>(A);

impl<A> InitializedActor<A> {
    pub(crate) const fn new(actor: A) -> Self {
        Self(actor)
    }
}

impl<A> IntoActor<A> for InitializedActor<A>
where
    A: PersistentActor,
    A::Event: BorshSerialize + BorshDeserialize,
{
    fn into_actor(self) -> A {
        self.0
    }
}

// ---------------------------------------------------------------------------
// PersistentActor
// ---------------------------------------------------------------------------

/// Extends [`Actor`] with event-sourced state persistence.
///
/// Behaviour and state are separated: the actor struct implements message
/// handling, while the associated `State` type is maintained as an `Arc`
/// and manipulated through the pure [`apply`](PersistentActor::apply)
/// function.
#[async_trait]
pub trait PersistentActor: Actor + Handler<Self> + Debug
where
    Self::State:
        BorshSerialize + BorshDeserialize + Send + Sync + Debug + 'static,
    Self::Event: BorshSerialize + BorshDeserialize,
{
    /// The persistence strategy ([`LightPersistence`] or [`FullPersistence`]).
    type Persistence: Persistence;

    /// Parameters passed to [`create_initial`](PersistentActor::create_initial).
    type InitParams;

    /// The immutable state type managed by this actor.
    type State;

    /// Creates the actor in its default initial state from the given parameters.
    ///
    /// The actor is responsible for holding an `Arc<Self::State>` internally;
    /// this method should initialise that field.
    fn create_initial(params: Self::InitParams) -> Self;

    /// Returns an [`InitializedActor`] wrapping the actor's initial state.
    fn initial(params: Self::InitParams) -> InitializedActor<Self>
    where
        Self: Sized,
    {
        InitializedActor::new(Self::create_initial(params))
    }

    /// Applies `event` to `state` and returns the new state.
    ///
    /// This method must be deterministic. It receives an `Arc` and returns an
    /// `Arc`; on the success path no deep clone is required.  Users can use
    /// [`Arc::make_mut`](std::sync::Arc::make_mut) to perform cheap in-place
    /// mutations when no other references exist.
    fn apply(
        state: Arc<Self::State>,
        event: &Self::Event,
    ) -> Result<Arc<Self::State>, ActorError>;

    /// Snapshot cadence for `FullPersistence`.
    ///
    /// - `None`: snapshots are only manual or done during store shutdown.
    /// - `Some(n)`: after every `n` persisted events since the last snapshot,
    ///   the store snapshots the current actor state automatically.
    ///
    /// `Some(0)` is invalid and causes actor creation to fail with
    /// [`ActorError::InvalidConfiguration`].
    ///
    /// Default: `Some(100)`.
    fn snapshot_every() -> Option<u64> {
        Some(100)
    }

    /// Returns the current actor state.
    fn state(&self) -> Arc<Self::State>;

    /// Replaces the current actor state.
    fn set_state(&mut self, state: Arc<Self::State>);

    /// Applies `event` to the in-memory state and durably persists it.
    ///
    /// The in-memory state is only replaced after the event has been durably
    /// persisted. [`apply`](PersistentActor::apply) is a pure function that
    /// returns the next state and never mutates the actor, so on any failure
    /// (encoding, persistence, or an unexpected store response) the current
    /// state is left untouched.
    async fn persist(
        &mut self,
        event: Self::Event,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let store = ctx.get_child::<Store<Self>>("store").await?;

        let new_state = Self::apply(self.state(), &event)?;

        let response = match Self::Persistence::get_persistence() {
            PersistenceType::Light => {
                let state = Arc::clone(&new_state);
                store.ask(StoreCommand::PersistLight(state)).await.map_err(
                    |e| actor_store_error(StoreOperation::PersistLight, e),
                )?
            }
            PersistenceType::Full => store
                .ask(StoreCommand::PersistFull {
                    event: Arc::new(event),
                    state: Arc::clone(&new_state),
                    snapshot_every: Self::snapshot_every(),
                })
                .await
                .map_err(|e| {
                    actor_store_error(StoreOperation::PersistFull, e)
                })?,
        };

        match response {
            StoreResponse::Persisted => {
                self.set_state(new_state);
                Ok(())
            }
            _ => Err(ActorError::UnexpectedResponse {
                path: ActorPath::from(format!("{}/store", ctx.path().key())),
                expected: "StoreResponse::Persisted".to_owned(),
            }),
        }
    }

    /// Sends the current state to the child `store` actor to be saved as a
    /// snapshot.
    async fn snapshot(
        &self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        self.snapshot_state(self.state(), ctx).await
    }

    /// Sends an explicit state to the child `store` actor to be saved as a
    /// snapshot.
    ///
    /// This helper is used internally by [`persist`](PersistentActor::persist).
    /// For `LightPersistence` it is the only persistence write; for
    /// `FullPersistence` it complements the event log. In both cases the
    /// snapshot reflects the already-applied state without requiring an
    /// in-place mutation of `self`.
    async fn snapshot_state(
        &self,
        state: Arc<Self::State>,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let store = ctx.get_child::<Store<Self>>("store").await?;
        store
            .ask(StoreCommand::Snapshot(state))
            .await
            .map_err(|e| actor_store_error(StoreOperation::Snapshot, e))?;
        Ok(())
    }

    /// Creates the child `Store` actor, opens the storage backend, and
    /// recovers any persisted state.
    ///
    /// When `prefix` is `None`, it defaults to
    /// [`default_store_prefix`] of the actor's full path, so sibling
    /// subtrees with equal leaf names never share backend tables.
    /// Pass an explicit prefix only to share state across actors on
    /// purpose.
    ///
    /// Call this from [`pre_start`](Actor::pre_start).
    async fn start_store<C: Collection, S: crate::database::State>(
        &mut self,
        name: &str,
        prefix: Option<&str>,
        ctx: &mut ActorContext<Self>,
        manager: impl DbManager<C, S>,
        key_box: Option<EncryptedKey>,
    ) -> Result<(), ActorError> {
        if let Some(snapshot_every) = Self::snapshot_every()
            && snapshot_every == 0
        {
            return Err(ActorError::InvalidConfiguration {
                component: "actor persistence".to_owned(),
                reason: "snapshot_every cannot be Some(0)".to_owned(),
            });
        }

        let default_prefix;
        let prefix = match prefix {
            Some(prefix) => prefix,
            None => {
                default_prefix = default_store_prefix(ctx.path());
                default_prefix.as_str()
            }
        };

        #[cfg(feature = "prometheus")]
        let store = {
            let metrics = ctx
                .system()
                .get_helper::<Arc<crate::metrics::StoreMetrics>>(
                    crate::metrics::STORE_METRICS_HELPER,
                );
            Store::<Self>::new(
                name,
                prefix,
                manager,
                key_box,
                self.state(),
                metrics,
                Arc::from(ctx.path().to_string()),
            )
        };
        #[cfg(not(feature = "prometheus"))]
        let store =
            Store::<Self>::new(name, prefix, manager, key_box, self.state());

        let store = store.map_err(|e| match e {
            Error::InvalidConfiguration { component, reason } => {
                ActorError::InvalidConfiguration { component, reason }
            }
            other => actor_store_error(StoreOperation::StoreInit, other),
        })?;
        let store = ctx.create_child("store", store).await?;
        let response = store.ask(StoreCommand::Recover).await?;

        if let StoreResponse::State(Some(state)) = response {
            self.set_state(state);
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// Internal child actor that manages event and snapshot persistence for a
/// [`PersistentActor`].
pub struct Store<A>
where
    A: PersistentActor,
    A::Event: BorshSerialize + BorshDeserialize,
{
    /// Next free event index.
    event_counter: u64,
    /// Number of events already included in the latest snapshot.
    state_counter: u64,
    /// Collection for storing events with sequence numbers as keys.
    events: Box<dyn Collection>,
    /// Storage for the latest state snapshot.
    states: Box<dyn State>,
    /// Storage for log metadata used to resume after snapshots.
    metadata: Box<dyn State>,
    /// Encrypted password for data encryption (XChaCha20-Poly1305).
    key_box: Option<EncryptedKey>,
    /// Initial state to use when recovering without a snapshot.
    initial_state: Arc<A::State>,
    /// Fencing generation owned by this store instance (see [`Store::new`]).
    fence_id: u64,
    /// Backend state holding the fencing generation (`{name}_fence`).
    fence: Box<dyn State>,
    /// Atomic multi-write handle when the backend supports it. `Some`
    /// guarantees all-or-nothing batches, so multi-write paths skip
    /// compensation; `None` keeps sequential writes with rollback.
    batch: Option<Box<dyn BatchWrite>>,
    /// Prefix scoping this store's keys, needed to build batches.
    batch_prefix: String,
    /// Backend collection name holding the event log (`{name}_events`).
    batch_events: String,
    /// Backend state name holding snapshots (`{name}_states`).
    batch_states: String,
    /// Backend state name holding log metadata (`{name}_metadata`).
    batch_metadata: String,
    /// Actor path of the persistent actor that owns this store, used as a
    /// Prometheus label.
    #[cfg(feature = "prometheus")]
    actor_path: Arc<str>,
    /// Optional Prometheus metrics collection for the store.
    #[cfg(feature = "prometheus")]
    metrics: Option<Arc<crate::metrics::StoreMetrics>>,
}

impl<A> ave_actors_actor::NotPersistentActor for Store<A>
where
    A: PersistentActor,
    A::Event: BorshSerialize + BorshDeserialize,
{
}

/// Metadata persisted alongside snapshots to resume event replay correctly.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct StoreMetadata {
    next_event_index: u64,
    state_counter: u64,
}

/// Snapshot returned by [`Store::get_state`].
struct StateSnapshot<S> {
    state: Arc<S>,
    counter: u64,
}

fn validate_store_name(name: &str) -> Result<(), Error> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(Error::InvalidConfiguration {
            component: "store name".to_owned(),
            reason: "store name must not be empty".to_owned(),
        });
    };

    let valid_start = first == '_' || first.is_ascii_alphabetic();
    let valid_rest = chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric());

    if valid_start && valid_rest {
        Ok(())
    } else {
        Err(Error::InvalidConfiguration {
            component: "store name".to_owned(),
            reason: format!(
                "store name '{name}' is invalid: allowed pattern is [A-Za-z_][A-Za-z0-9_]*"
            ),
        })
    }
}

fn validate_store_prefix(prefix: &str) -> Result<(), Error> {
    if prefix.is_empty() {
        return Err(Error::InvalidConfiguration {
            component: "store prefix".to_owned(),
            reason: "store prefix must not be empty".to_owned(),
        });
    }

    let valid = prefix
        .chars()
        .all(|ch| ch == '_' || ch == '-' || ch.is_ascii_alphanumeric());

    if valid {
        Ok(())
    } else {
        Err(Error::InvalidConfiguration {
            component: "store prefix".to_owned(),
            reason: format!(
                "store prefix '{prefix}' is invalid: allowed characters are [A-Za-z0-9_-]"
            ),
        })
    }
}

/// Generates a fencing generation unique enough to disambiguate live
/// `Store` instances sharing a prefix.
///
/// Cryptographic randomness first; on failure (no entropy source) falls
/// back to time+pid+counter, which still separates instances in practice.
/// Collisions only matter between two *live* writers, where even the
/// fallback diverges.
fn fresh_generation() -> u64 {
    let mut bytes = [0u8; 8];
    if fill_random(&mut bytes).is_ok() {
        return u64::from_ne_bytes(bytes);
    }
    use std::sync::atomic::{AtomicU64, Ordering};
    static FALLBACK_COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos
        ^ (std::process::id() as u64).wrapping_mul(0x9E3779B97F4A7C15)
        ^ FALLBACK_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Maximum length of a derived store prefix before the tail is hashed.
const MAX_DERIVED_PREFIX_LEN: usize = 200;

/// Derives the default store prefix from an actor's full path.
///
/// Used when [`PersistentActor::start_store`] is called with
/// `prefix = None`. Every segment contributes, so two actors that share
/// only their leaf name (e.g. `/user/a/counter` and `/user/b/counter`)
/// get distinct prefixes (`user__a__counter` vs `user__b__counter`) and
/// never share backend tables.
///
/// Only `[A-Za-z0-9_-]` characters are produced, satisfying
/// `validate_store_prefix`. Overlong paths keep a head slice plus a
/// deterministic FNV-1a hash of the full path, so the mapping is stable
/// across restarts.
pub fn default_store_prefix(path: &ActorPath) -> String {
    let trimmed = path.to_string();
    let trimmed = trimmed.trim_start_matches('/');
    if trimmed.is_empty() {
        return "root".to_owned();
    }
    let mut prefix = trimmed.replace('/', "__");
    if prefix.len() > MAX_DERIVED_PREFIX_LEN {
        let hash = fnv1a64(trimmed.as_bytes());
        prefix.truncate(MAX_DERIVED_PREFIX_LEN - 17);
        prefix.push_str(&format!("_{hash:016x}"));
    }
    prefix
}

fn fnv1a64(data: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

impl<A> Store<A>
where
    A: PersistentActor,
    A::Event: BorshSerialize + BorshDeserialize,
{
    /// Creates and initializes the store, opening the three backend stores
    /// (events, state, metadata).
    pub fn new<C, S>(
        name: &str,
        prefix: &str,
        manager: impl DbManager<C, S>,
        key_box: Option<EncryptedKey>,
        initial_state: Arc<A::State>,
        #[cfg(feature = "prometheus")] metrics: Option<
            Arc<crate::metrics::StoreMetrics>,
        >,
        #[cfg(feature = "prometheus")] actor_path: Arc<str>,
    ) -> Result<Self, Error>
    where
        C: Collection + 'static,
        S: State + 'static,
    {
        validate_store_name(name)?;
        validate_store_prefix(prefix)?;

        let batch_events = format!("{}_events", name);
        let batch_states = format!("{}_states", name);
        let batch_metadata = format!("{}_metadata", name);
        let fence_name = format!("{}_fence", name);
        let events = manager.create_collection(&batch_events, prefix)?;
        let states = manager.create_state(&batch_states, prefix)?;
        let metadata = manager.create_state(&batch_metadata, prefix)?;
        let fence = manager.create_state(&fence_name, prefix)?;
        let batch = manager.batch_writer();

        // Fencing: this instance owns the prefix from now on. Any older
        // live Store on the same handles fails its next fenced write
        // instead of interleaving events or purging another owner's state.
        // A missing fence (fresh or pre-fencing database) is adopted
        // silently. Adoption is best-effort: a backend that cannot persist
        // the fence will fail its data writes loudly anyway, and startup
        // must not fail for a store whose reads still work.
        let fence_id = fresh_generation();
        let mut fence = fence;
        if let Err(e) = Self::adopt_fence(&mut fence, fence_id) {
            error!(
                error = %e,
                "Fence adoption failed; continuing without fencing"
            );
        }

        let mut store = Self {
            event_counter: 0,
            state_counter: 0,
            events: Box::new(events),
            states: Box::new(states),
            metadata: Box::new(metadata),
            key_box,
            initial_state,
            fence_id,
            fence: Box::new(fence),
            batch,
            batch_prefix: prefix.to_owned(),
            batch_events,
            batch_states,
            batch_metadata,
            #[cfg(feature = "prometheus")]
            actor_path,
            #[cfg(feature = "prometheus")]
            metrics,
        };

        let last_event_counter = store
            .events
            .last()?
            .map(|(key, _)| {
                key.parse::<u64>()
                    .map_err(|e| store_error(StoreOperation::ParseEventKey, e))
                    .and_then(|n| {
                        n.checked_add(1).ok_or_else(|| {
                            store_error(
                                StoreOperation::ParseEventKey,
                                "event key overflow",
                            )
                        })
                    })
            })
            .transpose()?
            .unwrap_or(0);

        let snapshot_counter =
            store.get_state()?.map(|s| s.counter).unwrap_or(0);

        if let Some(metadata) = store.get_metadata()? {
            store.event_counter =
                last_event_counter.max(metadata.next_event_index);
            store.state_counter = metadata.state_counter;
        } else {
            store.event_counter = last_event_counter.max(snapshot_counter);
            store.state_counter = snapshot_counter;
        }

        debug!(
            "Initializing Store with event_counter: {}, state_counter: {}",
            store.event_counter, store.state_counter
        );

        #[cfg(feature = "prometheus")]
        store.record_pending_events();

        Ok(store)
    }

    /// Test-only helper that creates a [`Store`] with a default actor path and,
    /// when the `prometheus` feature is enabled, no metrics.
    #[cfg(test)]
    pub fn test_new<C, S>(
        name: &str,
        prefix: &str,
        manager: impl DbManager<C, S>,
        key_box: Option<EncryptedKey>,
        initial_state: Arc<A::State>,
    ) -> Result<Self, Error>
    where
        C: Collection + 'static,
        S: State + 'static,
    {
        #[cfg(feature = "prometheus")]
        {
            Store::new(
                name,
                prefix,
                manager,
                key_box,
                initial_state,
                None,
                Arc::from("/test"),
            )
        }
        #[cfg(not(feature = "prometheus"))]
        {
            Store::new(name, prefix, manager, key_box, initial_state)
        }
    }

    const fn pending_events_since_snapshot(&self) -> u64 {
        self.event_counter.saturating_sub(self.state_counter)
    }

    #[cfg(feature = "prometheus")]
    fn record_command_metrics(
        &self,
        start: Instant,
        duration_operation: &'static str,
        error_operation: &'static str,
        result: &Result<(), &Error>,
    ) {
        if let Some(metrics) = self.metrics.as_ref() {
            let duration = start.elapsed().as_secs_f64();
            metrics.observe_operation_duration(
                &self.actor_path,
                duration_operation,
                duration,
            );
            if result.is_err() {
                metrics.inc_errors(&self.actor_path, error_operation);
            }
        }
    }

    #[cfg(feature = "prometheus")]
    fn record_pending_events(&self) {
        // The pending gauge is inherently per-instance: only actors opting
        // in via `detailed_metrics` export it, otherwise per-path series
        // would grow without bound.
        if A::detailed_metrics()
            && let Some(metrics) = &self.metrics
        {
            let pending = self.pending_events_since_snapshot();
            metrics.set_pending_events_u64(&self.actor_path, pending);
        }
    }

    fn get_metadata(&self) -> Result<Option<StoreMetadata>, Error> {
        let data = match self.metadata.get() {
            Ok(data) => data,
            Err(Error::EntryNotFound { .. }) => return Ok(None),
            Err(err) => return Err(err),
        };

        let bytes = self.maybe_decrypt(data)?;

        match borsh::from_slice::<StoreMetadata>(&bytes) {
            Ok(metadata) => Ok(Some(metadata)),
            Err(e) => {
                error!(error = %e, "Can't decode metadata: incompatible format");
                Err(store_error(
                    StoreOperation::DecodeState,
                    format!("Metadata format is incompatible: {e}"),
                ))
            }
        }
    }

    fn persist_metadata(&mut self) -> Result<(), Error> {
        let bytes =
            self.encode_metadata_bytes(self.event_counter, self.state_counter)?;

        self.metadata.put(&bytes)
    }

    /// Overwrites the fencing generation with `id`, adopting ownership of
    /// the prefix. A missing fence (fresh or pre-fencing database) is
    /// created; any other backend error fails startup loudly.
    fn adopt_fence(fence: &mut impl State, id: u64) -> Result<(), Error> {
        match fence.get() {
            Ok(_) | Err(Error::EntryNotFound { .. }) => {}
            Err(e) => return Err(e),
        }
        let bytes = borsh::to_vec(&id)
            .map_err(|e| store_error(StoreOperation::StoreInit, e))?;
        fence
            .put(&bytes)
            .map_err(|e| store_error(StoreOperation::StoreInit, e))
    }

    /// Verifies this instance still owns the prefix.
    ///
    /// Call before every mutating write (but not plain snapshots, whose
    /// residue is recovery-benign). A missing fence is re-adopted; a
    /// generation mismatch or corrupt value fails with `operation`.
    fn check_fence(&mut self, operation: StoreOperation) -> Result<(), Error> {
        match self.fence.get() {
            Ok(bytes) => {
                let current: u64 = borsh::from_slice(&bytes).map_err(|e| {
                    store_error(operation, format!("fence decode failed: {e}"))
                })?;
                if current != self.fence_id {
                    error!(
                        expected = self.fence_id,
                        found = current,
                        "Store fenced by a newer generation"
                    );
                    return Err(store_error(
                        operation,
                        "store fenced by a newer generation: another live \
                         Store owns this prefix",
                    ));
                }
                Ok(())
            }
            Err(Error::EntryNotFound { .. }) => {
                let bytes = borsh::to_vec(&self.fence_id)
                    .map_err(|e| store_error(operation, e))?;
                self.fence
                    .put(&bytes)
                    .map_err(|e| store_error(operation, e))
            }
            Err(e) => Err(e),
        }
    }

    fn encode_event_bytes<E: BorshSerialize>(
        &self,
        event: &E,
    ) -> Result<Vec<u8>, Error> {
        let data = borsh::to_vec(event).map_err(|e| {
            error!("Can't encode event: {}", e);
            store_error(StoreOperation::EncodeEvent, e)
        })?;

        self.maybe_encrypt(&data)
    }

    fn encode_snapshot_bytes(
        &self,
        state: &A::State,
        counter: u64,
    ) -> Result<Vec<u8>, Error> {
        let data = borsh::to_vec(&(state, counter)).map_err(|e| {
            error!("Can't encode state: {}", e);
            store_error(StoreOperation::EncodeActor, e)
        })?;

        self.maybe_encrypt(&data)
    }

    fn encode_metadata_bytes(
        &self,
        next_event_index: u64,
        state_counter: u64,
    ) -> Result<Vec<u8>, Error> {
        let metadata = StoreMetadata {
            next_event_index,
            state_counter,
        };
        let data = borsh::to_vec(&metadata).map_err(|e| {
            error!("Can't encode metadata: {}", e);
            store_error(StoreOperation::EncodeActor, e)
        })?;

        self.maybe_encrypt(&data)
    }

    fn persist<E>(&mut self, event: &E) -> Result<(), Error>
    where
        E: Event + BorshSerialize + BorshDeserialize,
    {
        debug!("Persisting event: {:?}", event);

        self.check_fence(StoreOperation::Persist)?;

        let bytes = self.encode_event_bytes(event)?;

        let next_event_number = self.event_counter;

        debug!(
            "Persisting event {} at index {}",
            std::any::type_name::<E>(),
            next_event_number
        );

        let result = self
            .events
            .put(&format!("{:020}", next_event_number), &bytes);

        if result.is_ok() {
            self.event_counter =
                self.event_counter.checked_add(1).ok_or_else(|| {
                    store_error(
                        StoreOperation::Persist,
                        "event counter overflow",
                    )
                })?;
            debug!(
                "Successfully persisted event, event_counter now: {}",
                self.event_counter
            );
        }

        result
    }

    fn persist_light_state(&mut self, state: &A::State) -> Result<(), Error> {
        debug!("Persisting light snapshot");

        self.check_fence(StoreOperation::PersistLight)?;

        self.event_counter =
            self.event_counter.checked_add(1).ok_or_else(|| {
                store_error(
                    StoreOperation::PersistLight,
                    "event counter overflow",
                )
            })?;
        debug!(
            "Incremented event_counter to {} before snapshot",
            self.event_counter
        );

        if let Err(e) = self.snapshot(state) {
            error!(error = %e, "Snapshot failed during light persistence");
            self.event_counter -= 1;
            debug!(
                "Rolled back event_counter to {} after snapshot failure",
                self.event_counter
            );
            return Err(store_error(StoreOperation::Snapshot, e));
        }

        debug!(
            "Successfully persisted light snapshot, event_counter now: {}",
            self.event_counter
        );
        Ok(())
    }

    fn persist_full_state(
        &mut self,
        event: &A::Event,
        state: &A::State,
        snapshot_every: Option<u64>,
    ) -> Result<(), Error> {
        // Prospective check: after this event, pending would be
        // `pending + 1`. Equivalent to the old post-persist check
        // whenever `event_counter >= state_counter`.
        let due = snapshot_every.is_some_and(|every| {
            self.pending_events_since_snapshot() + 1 >= every
        });
        if !due {
            return self.persist(event);
        }

        self.check_fence(StoreOperation::PersistFull)?;

        let next = self.event_counter.checked_add(1).ok_or_else(|| {
            store_error(StoreOperation::PersistFull, "event counter overflow")
        })?;

        // Atomic path: event + snapshot + metadata in one all-or-nothing
        // batch. No compensation needed on error: either everything is
        // durable or nothing is, so counters stay untouched.
        if let Some(batch) = &self.batch {
            let event_bytes = self.encode_event_bytes(event)?;
            let snapshot_bytes = self.encode_snapshot_bytes(state, next)?;
            let metadata_bytes = self.encode_metadata_bytes(next, next)?;
            let key = format!("{:020}", self.event_counter);
            batch
                .write_batch(
                    &self.batch_prefix,
                    &[
                        BatchOp::PutEvent {
                            collection: &self.batch_events,
                            key: &key,
                            data: &event_bytes,
                        },
                        BatchOp::PutState {
                            store: &self.batch_states,
                            data: &snapshot_bytes,
                        },
                        BatchOp::PutState {
                            store: &self.batch_metadata,
                            data: &metadata_bytes,
                        },
                    ],
                )
                .map_err(|e| store_error(StoreOperation::PersistFull, e))?;
            self.event_counter = next;
            self.state_counter = next;
            #[cfg(feature = "prometheus")]
            self.record_pending_events();
            return Ok(());
        }

        // Sequential fallback with compensation (see below): the event is
        // already durable at this point, so a snapshot failure must remove
        // it again. Otherwise the log would contain an event the actor
        // never applied to its in-memory state (it only calls `set_state`
        // on success).
        self.persist(event)?;
        debug_assert!(self.event_counter > 0);
        let written_index = self.event_counter - 1;

        if let Err(snapshot_err) = self.snapshot(state) {
            error!(error = %snapshot_err, "Snapshot failed during full persistence; rolling back event {written_index}");
            let key = format!("{:020}", written_index);
            if let Err(rollback_err) = self.events.del(&key) {
                error!(
                    error = %rollback_err,
                    "Failed to roll back event {written_index} after \
                     snapshot failure; event log holds an event the actor \
                     did not apply"
                );
            } else {
                self.event_counter -= 1;
            }
            return Err(snapshot_err);
        }
        Ok(())
    }

    fn last_event(&self) -> Result<Option<A::Event>, Error> {
        self.events
            .last()?
            .map(|(_, data)| self.maybe_decrypt(data))
            .transpose()?
            .map(|data| {
                borsh::from_slice(&data).map_err(|e| {
                    error!("Can't decode event: {}", e);
                    store_error(StoreOperation::DecodeEvent, e)
                })
            })
            .transpose()
    }

    fn get_state(&self) -> Result<Option<StateSnapshot<A::State>>, Error> {
        let data = match self.states.get() {
            Ok(data) => data,
            Err(Error::EntryNotFound { .. }) => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        let bytes = self.maybe_decrypt(data)?;

        let (state, counter): (A::State, u64) = borsh::from_slice(&bytes)
            .map_err(|e| {
                error!("Can't decode state: {}", e);
                store_error(StoreOperation::DecodeState, e)
            })?;

        Ok(Some(StateSnapshot {
            state: Arc::new(state),
            counter,
        }))
    }

    fn events(&self, from: u64, to: u64) -> Result<Vec<A::Event>, Error> {
        if from > to {
            return Ok(Vec::new());
        }

        let from_key = format!("{:020}", from);
        let to_key = format!("{:020}", to);
        let expected = (to - from).checked_add(1).ok_or_else(|| {
            store_error(
                StoreOperation::GetEventsRange,
                "event range overflow: [from..=to] too large",
            )
        })? as usize;
        // Bound a single recovery read so a corrupt counter (e.g.
        // metadata.next_event_index = u64::MAX) cannot OOM the actor
        // by reserving gigabytes up front. Grow incrementally instead
        // of `with_capacity(expected)`; gap detection below still
        // reports truncated logs.
        let mut events = Vec::new();
        events.try_reserve(expected.min(1024)).map_err(|_| {
            store_error(
                StoreOperation::GetEventsRange,
                "failed to reserve event buffer",
            )
        })?;

        let iter = self
            .events
            .iter_range(&from_key, &to_key, false)
            .map_err(|e| store_error(StoreOperation::GetEventsRange, e))?;

        for item in iter {
            let (_, data) = item
                .map_err(|e| store_error(StoreOperation::GetEventsRange, e))?;

            let data = self.maybe_decrypt(data)?;

            let event: A::Event = borsh::from_slice(&data).map_err(|e| {
                error!("Can't decode event: {}", e);
                store_error(StoreOperation::DecodeEvent, e)
            })?;

            events.push(event);
        }

        if events.len() != expected {
            return Err(store_error(
                StoreOperation::GetEventsRange,
                format!(
                    "event log gap detected: expected {} events in \
                     range [{}..={}], found {}",
                    expected,
                    from,
                    to,
                    events.len()
                ),
            ));
        }
        Ok(events)
    }

    fn query_events(&self, from: u64, to: u64) -> Result<Vec<A::Event>, Error> {
        // O(1)-ish emptiness probe: `last()` seeks the end instead of
        // cloning the whole collection like `iter(false)` does.
        let empty_events = self.events.last()?.is_none();

        if from > to || from >= self.event_counter || empty_events {
            return Ok(Vec::new());
        }

        let upper = to.min(self.event_counter.saturating_sub(1));
        self.events(from, upper)
    }

    fn snapshot(&mut self, state: &A::State) -> Result<(), Error> {
        debug!("Snapshotting state");

        let next_state_counter = self.event_counter;

        let bytes = self.encode_snapshot_bytes(state, next_state_counter)?;

        // Keep the previous snapshot bytes so a later metadata failure
        // can restore them: without this, a durable snapshot newer than
        // the event log would survive the rollback below. A real read
        // error (not absence) aborts here: nothing was written yet, and
        // mistaking it for "no previous snapshot" could delete data in
        // the restore path.
        let prev_bytes = match self.states.get() {
            Ok(bytes) => Some(bytes),
            Err(Error::EntryNotFound { .. }) => None,
            Err(e) => return Err(e),
        };

        self.states.put(&bytes)?;
        let prev_state_counter = self.state_counter;
        self.state_counter = next_state_counter;
        #[cfg(feature = "prometheus")]
        self.record_pending_events();
        if let Err(e) = self.persist_metadata() {
            self.state_counter = prev_state_counter;
            // Best-effort restore of the previous snapshot bytes.
            let restore = match prev_bytes {
                Some(prev) => self.states.put(&prev),
                None => self.states.del().or_else(|del_err| {
                    // No previous snapshot existed; absence is also fine.
                    match del_err {
                        Error::EntryNotFound { .. } => Ok(()),
                        other => Err(other),
                    }
                }),
            };
            if let Err(restore_err) = restore {
                error!(
                    error = %restore_err,
                    "Failed to restore previous snapshot after metadata \
                     failure; snapshot store may hold a snapshot newer than \
                     the event log"
                );
            }
            return Err(e);
        }
        Ok(())
    }

    fn recover(&mut self) -> Result<Option<Arc<A::State>>, Error> {
        debug!("Starting recovery process");

        if let Some(snapshot) = self.get_state()? {
            return self
                .recover_from_snapshot(snapshot.state, snapshot.counter);
        }

        debug!("No previous state found");

        if let Some((key, ..)) = self.events.last()? {
            return self.recover_from_initial_events(&key);
        }

        debug!("No previous state and no events found, starting fresh");
        Ok(None)
    }

    /// Replays events in `[from..=to]`, folding them over `state`.
    ///
    /// Shared by both recovery paths so replay semantics cannot diverge.
    fn apply_events(
        &mut self,
        from: u64,
        to: u64,
        state: Arc<A::State>,
    ) -> Result<Arc<A::State>, Error> {
        let events = self.events(from, to)?;
        debug!("Found {} events to replay", events.len());

        let mut state = state;
        for (i, event) in events.iter().enumerate() {
            debug!("Applying event {} of {}", i + 1, events.len());
            state = A::apply(state, event).map_err(|e| {
                store_error_with_source(
                    StoreOperation::ApplyEvent,
                    format!("{:?}", e),
                    e,
                )
            })?;
        }
        Ok(state)
    }

    fn recover_from_snapshot(
        &mut self,
        state: Arc<A::State>,
        counter: u64,
    ) -> Result<Option<Arc<A::State>>, Error> {
        self.state_counter = counter;
        debug!("Recovered state with counter: {}", counter);

        let last_event_counter = self
            .events
            .last()?
            .map(|(key, _)| {
                key.parse::<u64>()
                    .map_err(|e| store_error(StoreOperation::ParseEventKey, e))
                    .and_then(|n| {
                        n.checked_add(1).ok_or_else(|| {
                            store_error(
                                StoreOperation::ParseEventKey,
                                "event key overflow",
                            )
                        })
                    })
            })
            .transpose()?
            .unwrap_or(0);

        self.event_counter = self.state_counter.max(last_event_counter);

        debug!(
            "Recovery state: event_counter={}, state_counter={}",
            self.event_counter, self.state_counter
        );

        let mut state = state;
        if self.event_counter > self.state_counter {
            warn!(
                event_counter = self.event_counter,
                state_counter = self.state_counter,
                "State mismatch detected, replaying events"
            );
            debug!(
                "Applying events from {} to {}",
                self.state_counter,
                self.event_counter - 1
            );
            state = self.apply_events(
                self.state_counter,
                self.event_counter - 1,
                state,
            )?;

            debug!("Updating snapshot after applying events");
            if let Err(e) = self.snapshot(state.as_ref()) {
                warn!(
                    error = %e,
                    "Snapshot failed after recovery; state is \
                     reconstructed in memory"
                );
            }
            debug!(
                "Recovery completed. Final event_counter: {}",
                self.event_counter
            );
        } else {
            debug!("State is up to date, no events to apply");
        }

        Ok(Some(state))
    }

    fn recover_from_initial_events(
        &mut self,
        last_key: &str,
    ) -> Result<Option<Arc<A::State>>, Error> {
        debug!("No snapshot but events found - replaying from beginning");

        self.event_counter = last_key
            .parse::<u64>()
            .map_err(|e| store_error(StoreOperation::ParseEventKey, e))?
            .checked_add(1)
            .ok_or_else(|| {
                store_error(StoreOperation::ParseEventKey, "event key overflow")
            })?;
        self.state_counter = 0;

        debug!(
            "Using provided initial state and applying {} events",
            self.event_counter
        );

        let mut state = Arc::clone(&self.initial_state);

        debug!("Replaying events from scratch");
        state = self.apply_events(0, self.event_counter - 1, state)?;

        debug!("Creating snapshot after replaying events");
        if let Err(e) = self.snapshot(state.as_ref()) {
            warn!(
                error = %e,
                "Snapshot failed after recovery; state is reconstructed \
                 in memory"
            );
        }

        debug!(
            "Recovery completed. Final event_counter: {}",
            self.event_counter
        );

        Ok(Some(state))
    }

    fn snapshot_if_needed(&mut self) -> Result<(), Error> {
        if !matches!(A::Persistence::get_persistence(), PersistenceType::Full) {
            return Ok(());
        }

        if self.event_counter == 0 || self.event_counter <= self.state_counter {
            return Ok(());
        }

        let mut state = self
            .get_state()?
            .map(|s| s.state)
            .unwrap_or_else(|| Arc::clone(&self.initial_state));

        let events = self.events(self.state_counter, self.event_counter - 1)?;
        for event in &events {
            state = A::apply(state, event).map_err(|e| {
                store_error_with_source(
                    StoreOperation::ApplyEventOnStop,
                    format!("{:?}", e),
                    e,
                )
            })?;
        }

        #[cfg(feature = "prometheus")]
        let start = Instant::now();
        let result = self.snapshot(state.as_ref());
        #[cfg(feature = "prometheus")]
        self.record_command_metrics(
            start,
            "snapshot",
            "snapshot",
            &result.as_ref().map(|_| ()),
        );
        result
    }

    /// Deletes all events, snapshots, and metadata, then resets all counters
    /// to zero.
    ///
    /// Refuses to run when another live `Store` owns the prefix (fencing):
    /// purging somebody else's log is never the right answer.
    pub fn purge(&mut self) -> Result<(), Error> {
        self.check_fence(StoreOperation::Purge)?;
        self.events.purge()?;
        self.states.purge()?;
        self.metadata.purge()?;
        self.event_counter = 0;
        self.state_counter = 0;
        #[cfg(feature = "prometheus")]
        self.record_pending_events();
        Ok(())
    }

    fn encrypt(
        &self,
        key_box: &EncryptedKey,
        bytes: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let key = key_box.key().map_err(|_| {
            error!("Failed to decrypt encryption key");
            store_error(StoreOperation::DecryptKey, "Can't decrypt key")
        })?;

        if key.len() != 32 {
            error!(
                expected = 32,
                got = key.len(),
                "Invalid encryption key length"
            );
            return Err(Error::Store {
                operation: StoreOperation::ValidateKeyLength,
                reason: format!(
                    "Invalid key length: expected 32 bytes, got {}",
                    key.len()
                ),
                source: None,
            });
        }

        let cipher = XChaCha20Poly1305::new_from_slice(key.as_ref())
            .map_err(|e| store_error(StoreOperation::ValidateKeyLength, e))?;
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        fill_random(&mut nonce_bytes).map_err(|e| {
            error!(error = %e, "Failed to generate encryption nonce");
            store_error(StoreOperation::EncryptData, e)
        })?;
        let nonce = XNonce::from(nonce_bytes);
        let ciphertext: Vec<u8> =
            cipher.encrypt(&nonce, bytes.as_ref()).map_err(|e| {
                error!(error = %e, "Encryption failed");
                store_error(StoreOperation::EncryptData, e)
            })?;

        let mut out = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    fn decrypt(
        &self,
        key_box: &EncryptedKey,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, Error> {
        if ciphertext.len() < NONCE_SIZE + 16 {
            warn!(
                expected_min = NONCE_SIZE + 16,
                got = ciphertext.len(),
                "Invalid ciphertext length, possible corruption"
            );
            return Err(Error::Store {
                operation: StoreOperation::ValidateCiphertext,
                reason: format!(
                    "Invalid ciphertext length: expected at least {} \
                     bytes, got {}",
                    NONCE_SIZE + 16,
                    ciphertext.len()
                ),
                source: None,
            });
        }

        let key = key_box.key().map_err(|_| {
            error!("Failed to decrypt decryption key");
            store_error(StoreOperation::DecryptKey, "Can't decrypt key")
        })?;

        if key.len() != 32 {
            error!(
                expected = 32,
                got = key.len(),
                "Invalid decryption key length"
            );
            return Err(store_error(
                StoreOperation::ValidateKeyLength,
                format!(
                    "Invalid key length: expected 32 bytes, got {}",
                    key.len()
                ),
            ));
        }

        let nonce = XNonce::try_from(&ciphertext[..NONCE_SIZE])
            .map_err(|e| store_error(StoreOperation::DecryptData, e))?;
        let ciphertext_data = &ciphertext[NONCE_SIZE..];

        let cipher = XChaCha20Poly1305::new_from_slice(key.as_ref())
            .map_err(|e| store_error(StoreOperation::ValidateKeyLength, e))?;
        let plaintext =
            cipher.decrypt(&nonce, ciphertext_data).map_err(|e| {
                warn!(
                    error = %e,
                    "Decryption failed, possible tampering or corruption"
                );
                store_error(
                    StoreOperation::DecryptData,
                    format!("Decryption failed (possible tampering): {}", e),
                )
            })?;

        Ok(plaintext)
    }

    fn maybe_encrypt(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        self.key_box.as_ref().map_or_else(
            || Ok(data.to_vec()),
            |key_box| self.encrypt(key_box, data),
        )
    }

    fn maybe_decrypt(&self, data: Vec<u8>) -> Result<Vec<u8>, Error> {
        match &self.key_box {
            Some(key_box) => self.decrypt(key_box, &data),
            None => Ok(data),
        }
    }
}

// ---------------------------------------------------------------------------
// StoreCommand
// ---------------------------------------------------------------------------

/// Commands processed by the internal [`Store`] actor.
pub enum StoreCommand<A: PersistentActor>
where
    A::Event: BorshSerialize + BorshDeserialize,
{
    /// Persist an event and snapshot the supplied state if required.
    PersistFull {
        /// Event to append to the event log.
        event: Arc<A::Event>,
        /// Current actor state, used when a snapshot is triggered.
        state: Arc<A::State>,
        /// Snapshot cadence for `FullPersistence`.
        snapshot_every: Option<u64>,
    },
    /// Persist a snapshot of the supplied state (LightPersistence).
    PersistLight(Arc<A::State>),
    /// Snapshot the supplied state immediately.
    Snapshot(Arc<A::State>),
    /// Return the most recently persisted event.
    LastEvent,
    /// Return the next free event index.
    NextEventNumber,
    /// Return all events from the supplied event index to the end of the log.
    LastEventsFrom(u64),
    /// Return all events within the inclusive `[from, to]` range.
    GetEvents { from: u64, to: u64 },
    /// Recover the current actor state from snapshots and events.
    Recover,
    /// Delete all events, snapshots, and metadata for this actor.
    Purge,
}

impl<A: PersistentActor> Clone for StoreCommand<A>
where
    A::Event: BorshSerialize + BorshDeserialize,
{
    fn clone(&self) -> Self {
        match self {
            Self::PersistFull {
                event,
                state,
                snapshot_every,
            } => Self::PersistFull {
                event: Arc::clone(event),
                state: Arc::clone(state),
                snapshot_every: *snapshot_every,
            },
            Self::PersistLight(s) => Self::PersistLight(Arc::clone(s)),
            Self::Snapshot(s) => Self::Snapshot(Arc::clone(s)),
            Self::LastEvent => Self::LastEvent,
            Self::NextEventNumber => Self::NextEventNumber,
            Self::LastEventsFrom(n) => Self::LastEventsFrom(*n),
            Self::GetEvents { from, to } => Self::GetEvents {
                from: *from,
                to: *to,
            },
            Self::Recover => Self::Recover,
            Self::Purge => Self::Purge,
        }
    }
}

impl<A: PersistentActor> Message for StoreCommand<A> where
    A::Event: Event + BorshSerialize + BorshDeserialize
{
}

// ---------------------------------------------------------------------------
// StoreResponse
// ---------------------------------------------------------------------------

/// Responses returned by the [`Store`] actor.
#[derive(Debug, Clone)]
pub enum StoreResponse<A: PersistentActor>
where
    A::Event: BorshSerialize + BorshDeserialize,
    A::State: BorshSerialize + BorshDeserialize,
{
    /// Command completed without a payload.
    None,
    /// An event was persisted successfully.
    Persisted,
    /// A snapshot was stored successfully.
    Snapshotted,
    /// Recovered actor state, or `None` when no persisted state exists.
    State(Option<Arc<A::State>>),
    /// Most recently persisted event, or `None` when the log is empty.
    LastEvent(Option<A::Event>),
    /// Next free event index.
    NextEventNumber(u64),
    /// Event payloads returned by a range query.
    Events(Vec<A::Event>),
}

impl<A: PersistentActor> Response for StoreResponse<A>
where
    A::Event: BorshSerialize + BorshDeserialize,
    A::State: BorshSerialize + BorshDeserialize,
{
}

// ---------------------------------------------------------------------------
// Actor / Handler
// ---------------------------------------------------------------------------

#[async_trait]
impl<A> Actor for Store<A>
where
    A: PersistentActor,
    A::Event: BorshSerialize + BorshDeserialize,
{
    type Message = StoreCommand<A>;
    type Response = StoreResponse<A>;
    type Event = ();
    type SinkEvent = ();
    type ChildError = ActorError;
    type ChildFault = ActorError;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("Store", id = %id)
    }

    async fn pre_stop(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        self.snapshot_if_needed()
            .map_err(|e| actor_store_error(StoreOperation::Snapshot, e))
    }
}

#[async_trait]
impl<A> Handler<Self> for Store<A>
where
    A: PersistentActor,
    A::Event: BorshSerialize + BorshDeserialize,
{
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: StoreCommand<A>,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<StoreResponse<A>, ActorError> {
        match msg {
            StoreCommand::PersistFull {
                event,
                state,
                snapshot_every,
            } => {
                if snapshot_every == Some(0) {
                    return Err(actor_store_error(
                        StoreOperation::PersistFull,
                        Error::InvalidConfiguration {
                            component: "actor persistence".to_owned(),
                            reason: "snapshot_every cannot be Some(0)"
                                .to_owned(),
                        },
                    ));
                }
                #[cfg(feature = "prometheus")]
                let start = Instant::now();
                let combined = self.persist_full_state(
                    event.as_ref(),
                    state.as_ref(),
                    snapshot_every,
                );

                #[cfg(feature = "prometheus")]
                self.record_command_metrics(
                    start,
                    "persist_full",
                    "persist_full",
                    &combined.as_ref().map(|_| ()),
                );

                combined.map_err(|e| {
                    actor_store_error(StoreOperation::PersistFull, e)
                })?;
                #[cfg(feature = "prometheus")]
                self.record_pending_events();

                debug!("Persisted full event: {:?}", event);
                Ok(StoreResponse::Persisted)
            }
            StoreCommand::PersistLight(state) => {
                #[cfg(feature = "prometheus")]
                let start = Instant::now();
                let result = self.persist_light_state(state.as_ref());
                #[cfg(feature = "prometheus")]
                self.record_command_metrics(
                    start,
                    "persist_light",
                    "persist_light",
                    &result.as_ref().map(|_| ()),
                );
                result.map_err(|e| {
                    actor_store_error(StoreOperation::PersistLight, e)
                })?;
                debug!("Light persistence of state snapshot");
                Ok(StoreResponse::Persisted)
            }
            StoreCommand::Snapshot(state) => {
                #[cfg(feature = "prometheus")]
                let start = Instant::now();
                let result = self.snapshot(state.as_ref());
                #[cfg(feature = "prometheus")]
                self.record_command_metrics(
                    start,
                    "snapshot",
                    "snapshot",
                    &result.as_ref().map(|_| ()),
                );
                result.map_err(|e| {
                    actor_store_error(StoreOperation::Snapshot, e)
                })?;
                debug!("Snapshotted state");
                Ok(StoreResponse::Snapshotted)
            }
            StoreCommand::Recover => {
                #[cfg(feature = "prometheus")]
                let start = Instant::now();
                let result = self.recover();
                #[cfg(feature = "prometheus")]
                self.record_command_metrics(
                    start,
                    "recover",
                    "recover",
                    &result.as_ref().map(|_| ()),
                );
                let state = result.map_err(|e| {
                    actor_store_error(StoreOperation::Recover, e)
                })?;
                #[cfg(feature = "prometheus")]
                self.record_pending_events();
                debug!("Recovered state");
                Ok(StoreResponse::State(state))
            }
            StoreCommand::GetEvents { from, to } => {
                #[cfg(feature = "prometheus")]
                let start = Instant::now();
                let result = self.query_events(from, to);
                #[cfg(feature = "prometheus")]
                self.record_command_metrics(
                    start,
                    "get_events_range",
                    "get_events_range",
                    &result.as_ref().map(|_| ()),
                );
                let events = result.map_err(|e| {
                    actor_store_error(
                        StoreOperation::GetEventsRange,
                        format!("Unable to get events range: {}", e),
                    )
                })?;
                Ok(StoreResponse::Events(events))
            }
            StoreCommand::LastEvent => {
                #[cfg(feature = "prometheus")]
                let start = Instant::now();
                let result = self.last_event();
                #[cfg(feature = "prometheus")]
                self.record_command_metrics(
                    start,
                    "last_event",
                    "last_event",
                    &result.as_ref().map(|_| ()),
                );
                let event = result.map_err(|e| {
                    actor_store_error(StoreOperation::LastEvent, e)
                })?;
                debug!("Last event: {:?}", event);
                Ok(StoreResponse::LastEvent(event))
            }
            StoreCommand::Purge => {
                #[cfg(feature = "prometheus")]
                let start = Instant::now();
                let result = self.purge();
                #[cfg(feature = "prometheus")]
                self.record_command_metrics(
                    start,
                    "purge",
                    "purge",
                    &result.as_ref().map(|_| ()),
                );
                result
                    .map_err(|e| actor_store_error(StoreOperation::Purge, e))?;
                debug!("Purged store");
                Ok(StoreResponse::None)
            }
            StoreCommand::NextEventNumber => {
                Ok(StoreResponse::NextEventNumber(self.event_counter))
            }
            StoreCommand::LastEventsFrom(from) => {
                #[cfg(feature = "prometheus")]
                let start = Instant::now();
                let to = self.event_counter.saturating_sub(1);
                // query_events (not events) so that an empty or event-less
                // store yields an empty list instead of a gap error,
                // consistent with GetEvents.
                let result = self.query_events(from, to);
                #[cfg(feature = "prometheus")]
                self.record_command_metrics(
                    start,
                    "get_latest_events",
                    "get_latest_events",
                    &result.as_ref().map(|_| ()),
                );
                let events = result.map_err(|e| {
                    actor_store_error(
                        StoreOperation::GetLatestEvents,
                        format!("Unable to get the latest events: {}", e),
                    )
                })?;
                Ok(StoreResponse::Events(events))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::{MemoryManager, MemoryStore};
    use ave_actors_actor::{ActorSystem, Error as ActorError};
    use serde::{Deserialize, Serialize};
    use test_log::test;
    use tokio_util::sync::CancellationToken;
    use tracing::info_span;

    #[derive(
        Debug,
        Clone,
        Serialize,
        Deserialize,
        BorshSerialize,
        BorshDeserialize,
        Default,
    )]
    struct CounterState {
        value: i32,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum CounterMessage {
        Add(i32),
        Get,
    }

    impl Message for CounterMessage {}

    #[derive(
        Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
    )]
    struct CounterEvent(i32);

    impl Event for CounterEvent {}

    #[derive(Debug, Clone, PartialEq)]
    enum CounterResponse {
        Value(i32),
        None,
    }

    impl Response for CounterResponse {}

    #[derive(Debug)]
    struct CounterActor {
        state: Arc<CounterState>,
    }

    #[async_trait]
    impl Actor for CounterActor {
        type Message = CounterMessage;
        type Event = CounterEvent;
        type SinkEvent = Self::Event;
        type Response = CounterResponse;
        type ChildError = ActorError;
        type ChildFault = ActorError;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("CounterActor", id = %id)
        }

        fn detailed_metrics() -> bool {
            // Exercise the opt-in per-actor pending gauge in metrics tests.
            true
        }

        async fn pre_start(
            &mut self,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), ActorError> {
            let db: MemoryManager = ctx
                .system()
                .get_helper("db")
                .expect("db helper should be installed");
            self.start_store("store", None, ctx, db, None).await
        }
    }

    #[async_trait]
    impl PersistentActor for CounterActor {
        type Persistence = crate::store::LightPersistence;
        type InitParams = ();
        type State = CounterState;

        fn create_initial(_: ()) -> Self {
            Self {
                state: Arc::new(CounterState::default()),
            }
        }

        fn apply(
            state: Arc<CounterState>,
            event: &CounterEvent,
        ) -> Result<Arc<CounterState>, ActorError> {
            let mut state = Arc::clone(&state);
            let inner = Arc::make_mut(&mut state);
            inner.value += event.0;
            Ok(state)
        }

        fn state(&self) -> Arc<CounterState> {
            Arc::clone(&self.state)
        }

        fn set_state(&mut self, state: Arc<CounterState>) {
            self.state = state;
        }
    }

    #[async_trait]
    impl Handler<Self> for CounterActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: CounterMessage,
            ctx: &mut ActorContext<Self>,
        ) -> Result<CounterResponse, ActorError> {
            match msg {
                CounterMessage::Add(v) => {
                    self.persist(CounterEvent(v), ctx).await?;
                    Ok(CounterResponse::None)
                }
                CounterMessage::Get => {
                    Ok(CounterResponse::Value(self.state.value))
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Full-persistence actor with automatic snapshots
    // ------------------------------------------------------------------

    #[derive(Debug)]
    struct FullCounterActor {
        state: Arc<CounterState>,
    }

    #[async_trait]
    impl Actor for FullCounterActor {
        type Message = CounterMessage;
        type Event = CounterEvent;
        type SinkEvent = Self::Event;
        type Response = CounterResponse;
        type ChildError = ActorError;
        type ChildFault = ActorError;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("FullCounterActor", id = %id)
        }

        async fn pre_start(
            &mut self,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), ActorError> {
            let db: MemoryManager = ctx
                .system()
                .get_helper("db")
                .expect("db helper should be installed");
            self.start_store("store", None, ctx, db, None).await
        }
    }

    #[async_trait]
    impl PersistentActor for FullCounterActor {
        type Persistence = crate::store::FullPersistence;
        type InitParams = ();
        type State = CounterState;

        fn create_initial(_: ()) -> Self {
            Self {
                state: Arc::new(CounterState::default()),
            }
        }

        fn snapshot_every() -> Option<u64> {
            Some(2)
        }

        fn apply(
            state: Arc<CounterState>,
            event: &CounterEvent,
        ) -> Result<Arc<CounterState>, ActorError> {
            let mut state = Arc::clone(&state);
            let inner = Arc::make_mut(&mut state);
            inner.value += event.0;
            Ok(state)
        }

        fn state(&self) -> Arc<CounterState> {
            Arc::clone(&self.state)
        }

        fn set_state(&mut self, state: Arc<CounterState>) {
            self.state = state;
        }
    }

    #[async_trait]
    impl Handler<Self> for FullCounterActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: CounterMessage,
            ctx: &mut ActorContext<Self>,
        ) -> Result<CounterResponse, ActorError> {
            match msg {
                CounterMessage::Add(v) => {
                    self.persist(CounterEvent(v), ctx).await?;
                    Ok(CounterResponse::None)
                }
                CounterMessage::Get => {
                    Ok(CounterResponse::Value(self.state.value))
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Test: Light persistence with recovery
    // ------------------------------------------------------------------

    #[test(tokio::test)]
    async fn test_cow_light_persistence_recovery() {
        let (system, ..) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );

        system.add_helper("db", MemoryManager::default());

        let actor_ref = system
            .create_root_actor("counter", CounterActor::initial(()))
            .await
            .unwrap();

        actor_ref.ask(CounterMessage::Add(10)).await.unwrap();
        actor_ref.ask(CounterMessage::Add(5)).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let value = actor_ref.ask(CounterMessage::Get).await.unwrap();
        assert_eq!(value, CounterResponse::Value(15));

        actor_ref.ask_stop().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Recreate and verify recovery
        let actor_ref = system
            .create_root_actor("counter", CounterActor::initial(()))
            .await
            .unwrap();

        let value = actor_ref.ask(CounterMessage::Get).await.unwrap();
        assert_eq!(value, CounterResponse::Value(15));

        actor_ref.ask_stop().await.unwrap();
    }

    // ------------------------------------------------------------------
    // Test: Full persistence with automatic snapshots
    // ------------------------------------------------------------------

    #[test(tokio::test)]
    async fn test_cow_full_persistence_with_snapshots() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );
        tokio::spawn(async move {
            runner.run().await;
        });

        system.add_helper("db", MemoryManager::default());

        let actor_ref = system
            .create_root_actor("full", FullCounterActor::initial(()))
            .await
            .unwrap();

        actor_ref.ask(CounterMessage::Add(3)).await.unwrap();
        actor_ref.ask(CounterMessage::Add(7)).await.unwrap();
        actor_ref.ask(CounterMessage::Add(2)).await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        let value = actor_ref.ask(CounterMessage::Get).await.unwrap();
        assert_eq!(value, CounterResponse::Value(12));

        actor_ref.ask_stop().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Recreate and verify recovery (events + snapshots)
        let actor_ref = system
            .create_root_actor("full", FullCounterActor::initial(()))
            .await
            .unwrap();

        let value = actor_ref.ask(CounterMessage::Get).await.unwrap();
        assert_eq!(value, CounterResponse::Value(12));

        actor_ref.ask_stop().await.unwrap();
    }

    // ------------------------------------------------------------------
    // Test: Store direct operations
    // ------------------------------------------------------------------

    #[test]
    fn test_counter_actor_apply() {
        let s = Arc::new(CounterState { value: 5 });
        let e = CounterEvent(3);
        let new_s = CounterActor::apply(s, &e).unwrap();
        assert_eq!(new_s.value, 8);
    }

    #[test]
    fn test_cow_store_events_range() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store.persist(&CounterEvent(5)).unwrap();
        store.persist(&CounterEvent(3)).unwrap();

        let events = store.events(1, 1).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, 3);
    }

    #[test]
    fn test_cow_store_recovery_unit() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store.persist(&CounterEvent(5)).unwrap();
        store
            .snapshot(&Arc::new(CounterState { value: 5 }))
            .unwrap();
        store.persist(&CounterEvent(3)).unwrap();

        let recovered = store.recover().unwrap();
        assert_eq!(recovered.unwrap().value, 8);
    }

    #[test(tokio::test)]
    async fn test_cow_store_direct_commands() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );
        tokio::spawn(async move {
            runner.run().await;
        });

        let initial = Arc::new(CounterState { value: 0 });
        let store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        let store_ref = system.create_root_actor("store", store).await.unwrap();

        store_ref
            .tell(StoreCommand::PersistFull {
                event: Arc::new(CounterEvent(5)),
                state: Arc::new(CounterState { value: 0 }),
                snapshot_every: None,
            })
            .await
            .unwrap();
        store_ref
            .tell(StoreCommand::Snapshot(Arc::new(CounterState { value: 5 })))
            .await
            .unwrap();
        store_ref
            .tell(StoreCommand::PersistFull {
                event: Arc::new(CounterEvent(3)),
                state: Arc::new(CounterState { value: 0 }),
                snapshot_every: None,
            })
            .await
            .unwrap();

        let response = store_ref.ask(StoreCommand::Recover).await.unwrap();
        if let StoreResponse::State(Some(state)) = response {
            assert_eq!(state.value, 8);
        } else {
            panic!("Expected recovered state");
        }

        let response = store_ref.ask(StoreCommand::LastEvent).await.unwrap();
        if let StoreResponse::LastEvent(Some(event)) = response {
            assert_eq!(event.0, 3);
        } else {
            panic!("Expected last event");
        }

        let response = store_ref
            .ask(StoreCommand::GetEvents { from: 0, to: 1 })
            .await
            .unwrap();
        if let StoreResponse::Events(events) = response {
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].0, 5);
            assert_eq!(events[1].0, 3);
        } else {
            panic!("Expected events");
        }
    }

    #[test(tokio::test)]
    async fn test_persist_full_rejects_snapshot_every_zero() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );
        tokio::spawn(async move {
            runner.run().await;
        });

        let initial = Arc::new(CounterState { value: 0 });
        let store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();
        let store_ref = system.create_root_actor("store", store).await.unwrap();

        let result = store_ref
            .ask(StoreCommand::PersistFull {
                event: Arc::new(CounterEvent(1)),
                state: Arc::new(CounterState { value: 1 }),
                snapshot_every: Some(0),
            })
            .await;
        assert!(
            result.is_err(),
            "PersistFull with snapshot_every Some(0) must be rejected, got \
             {result:?}"
        );
    }

    #[test(tokio::test)]
    async fn test_persist_full_rolls_back_event_when_snapshot_fails() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );
        tokio::spawn(async move {
            runner.run().await;
        });

        // Events work but every snapshot write fails.
        let initial = Arc::new(CounterState { value: 0 });
        let store = Store::<CounterActor>::test_new(
            "store",
            "test",
            FailingStateManager,
            None,
            initial,
        )
        .unwrap();
        let store_ref = system.create_root_actor("store", store).await.unwrap();

        let result = store_ref
            .ask(StoreCommand::PersistFull {
                event: Arc::new(CounterEvent(1)),
                state: Arc::new(CounterState { value: 1 }),
                snapshot_every: Some(1),
            })
            .await;
        assert!(result.is_err(), "snapshot failure must fail PersistFull");

        // The appended event must be compensated: no event left behind,
        // counters back at zero, recovery finds nothing.
        let response =
            store_ref.ask(StoreCommand::NextEventNumber).await.unwrap();
        assert!(matches!(response, StoreResponse::NextEventNumber(0)));
        let response = store_ref
            .ask(StoreCommand::GetEvents { from: 0, to: 10 })
            .await
            .unwrap();
        assert!(
            matches!(response, StoreResponse::Events(events) if events.is_empty())
        );
        let response = store_ref.ask(StoreCommand::Recover).await.unwrap();
        assert!(matches!(response, StoreResponse::State(None)));
    }

    #[test(tokio::test)]
    async fn test_persist_full_rolls_back_event_when_metadata_fails() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );
        tokio::spawn(async move {
            runner.run().await;
        });

        // Snapshot bytes succeed but metadata writes fail: exercises both
        // the snapshot-bytes restore and the event rollback.
        let initial = Arc::new(CounterState { value: 0 });
        let store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MetadataFailManager::default(),
            None,
            initial,
        )
        .unwrap();
        let store_ref = system.create_root_actor("store", store).await.unwrap();

        let result = store_ref
            .ask(StoreCommand::PersistFull {
                event: Arc::new(CounterEvent(1)),
                state: Arc::new(CounterState { value: 1 }),
                snapshot_every: Some(1),
            })
            .await;
        assert!(result.is_err(), "metadata failure must fail PersistFull");

        let response =
            store_ref.ask(StoreCommand::NextEventNumber).await.unwrap();
        assert!(matches!(response, StoreResponse::NextEventNumber(0)));
        let response = store_ref.ask(StoreCommand::Recover).await.unwrap();
        assert!(matches!(response, StoreResponse::State(None)));
    }

    // ------------------------------------------------------------------
    // Mock backend whose atomic batch always fails, used to verify the
    // batch path leaves no residue and keeps counters untouched.
    // ------------------------------------------------------------------

    #[derive(Clone)]
    struct FailingBatchWriter;

    impl BatchWrite for FailingBatchWriter {
        fn write_batch(
            &self,
            _prefix: &str,
            _ops: &[BatchOp<'_>],
        ) -> Result<(), Error> {
            Err(Error::Store {
                operation: StoreOperation::ExecuteBatch,
                reason: "injected batch failure".to_owned(),
                source: None,
            })
        }
    }

    #[derive(Default, Clone)]
    struct FailingBatchManager {
        inner: MemoryManager,
    }

    impl DbManager<MemoryStore, MemoryStore> for FailingBatchManager {
        fn create_collection(
            &self,
            name: &str,
            prefix: &str,
        ) -> Result<MemoryStore, Error> {
            self.inner.create_collection(name, prefix)
        }

        fn create_state(
            &self,
            name: &str,
            prefix: &str,
        ) -> Result<MemoryStore, Error> {
            self.inner.create_state(name, prefix)
        }

        fn batch_writer(&self) -> Option<Box<dyn BatchWrite>> {
            Some(Box::new(FailingBatchWriter))
        }
    }

    #[test]
    fn test_second_store_fences_first() {
        let manager = MemoryManager::default();
        let initial = Arc::new(CounterState { value: 0 });
        let mut first = Store::<CounterActor>::test_new(
            "store",
            "test",
            manager.clone(),
            None,
            Arc::clone(&initial),
        )
        .unwrap();
        // Same handles, second adoption: ownership moves to `second`.
        let mut second = Store::<CounterActor>::test_new(
            "store", "test", manager, None, initial,
        )
        .unwrap();

        assert!(
            first.persist(&CounterEvent(1)).is_err(),
            "fenced store must refuse writes"
        );
        assert!(second.persist(&CounterEvent(1)).is_ok());
        assert!(first.purge().is_err(), "fenced store must refuse purge");
        // The owner still works and sees exactly its own event.
        let response = second
            .events
            .last()
            .unwrap()
            .expect("owner event must be present");
        assert_eq!(response.0, format!("{:020}", 0));
    }

    #[test(tokio::test)]
    async fn test_persist_full_atomic_batch_failure_applies_nothing() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );
        tokio::spawn(async move {
            runner.run().await;
        });

        let manager = FailingBatchManager::default();
        let initial = Arc::new(CounterState { value: 0 });
        let store = Store::<CounterActor>::test_new(
            "store",
            "test",
            manager.clone(),
            None,
            initial,
        )
        .unwrap();
        let store_ref = system.create_root_actor("store", store).await.unwrap();

        let result = store_ref
            .ask(StoreCommand::PersistFull {
                event: Arc::new(CounterEvent(1)),
                state: Arc::new(CounterState { value: 1 }),
                snapshot_every: Some(1),
            })
            .await;
        assert!(result.is_err(), "batch failure must fail PersistFull");

        // All-or-nothing: no event, no snapshot, counters at zero.
        let response =
            store_ref.ask(StoreCommand::NextEventNumber).await.unwrap();
        assert!(matches!(response, StoreResponse::NextEventNumber(0)));
        let events = manager
            .create_collection("store_events", "test")
            .unwrap()
            .iter(false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(events.is_empty());
        let response = store_ref.ask(StoreCommand::Recover).await.unwrap();
        assert!(matches!(response, StoreResponse::State(None)));
    }

    #[test]
    fn test_light_persistence_stores_only_snapshot() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store
            .persist_light_state(&CounterState { value: 5 })
            .unwrap();

        assert_eq!(
            store.events.iter(false).unwrap().next(),
            None,
            "LightPersistence must not store events"
        );

        let snapshot =
            store.get_state().unwrap().expect("snapshot should exist");
        assert_eq!(snapshot.state.value, 5);
        assert_eq!(snapshot.counter, 1);
    }

    #[test]
    fn test_light_persistence_no_events_stored() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store
            .persist_light_state(&CounterState { value: 1 })
            .unwrap();
        store
            .persist_light_state(&CounterState { value: 2 })
            .unwrap();

        assert!(
            store.events.iter(false).unwrap().next().is_none(),
            "LightPersistence must leave the event collection empty"
        );
    }

    #[test]
    fn test_light_persistence_last_event_is_none() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store
            .persist_light_state(&CounterState { value: 7 })
            .unwrap();

        assert!(store.last_event().unwrap().is_none());
    }

    #[test]
    fn test_light_persistence_event_counter_equals_state_counter() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store
            .persist_light_state(&CounterState { value: 1 })
            .unwrap();
        store
            .persist_light_state(&CounterState { value: 2 })
            .unwrap();

        assert_eq!(store.event_counter, 2);
        assert_eq!(store.state_counter, 2);
    }

    #[test]
    fn test_light_persistence_pending_events_is_zero() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store
            .persist_light_state(&CounterState { value: 1 })
            .unwrap();
        store
            .persist_light_state(&CounterState { value: 2 })
            .unwrap();

        assert_eq!(store.pending_events_since_snapshot(), 0);
    }

    #[test]
    fn test_light_persistence_recovery_loads_last_snapshot() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store
            .persist_light_state(&CounterState { value: 5 })
            .unwrap();
        store
            .persist_light_state(&CounterState { value: 10 })
            .unwrap();

        let recovered = store.recover().unwrap();
        assert_eq!(recovered.unwrap().value, 10);
    }

    #[test]
    fn test_light_persistence_recovery_without_snapshot_returns_none() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        assert!(store.recover().unwrap().is_none());
    }

    #[test]
    fn test_light_persistence_no_events_in_range() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store
            .persist_light_state(&CounterState { value: 1 })
            .unwrap();
        store
            .persist_light_state(&CounterState { value: 2 })
            .unwrap();

        // Even though the logical event counter advanced, no events are stored.
        let events = store.query_events(0, 0).unwrap();
        assert!(events.is_empty());
    }

    // ------------------------------------------------------------------
    // Mock backend that fails state writes, used to verify LightPersistence
    // rollback behaviour.
    // ------------------------------------------------------------------

    #[derive(Default, Clone)]
    struct FailingState {
        name: String,
        prefix: String,
        /// When `false`, writes succeed as no-ops. The manager disables
        /// failure for the fencing handle so startup (fence adoption) works
        /// while snapshot/state writes keep failing.
        fail_puts: bool,
    }

    impl State for FailingState {
        fn name(&self) -> &str {
            &self.name
        }

        fn get(&self) -> Result<Vec<u8>, Error> {
            Err(Error::EntryNotFound {
                key: self.prefix.clone(),
            })
        }

        fn put(&mut self, _data: &[u8]) -> Result<(), Error> {
            if !self.fail_puts {
                return Ok(());
            }
            Err(Error::Store {
                operation: StoreOperation::Snapshot,
                reason: "injected snapshot failure".to_owned(),
                source: None,
            })
        }

        fn del(&mut self) -> Result<(), Error> {
            Err(Error::EntryNotFound {
                key: self.prefix.clone(),
            })
        }

        fn purge(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[derive(Default, Clone)]
    struct FailingStateManager;

    impl DbManager<MemoryStore, FailingState> for FailingStateManager {
        fn create_collection(
            &self,
            name: &str,
            prefix: &str,
        ) -> Result<MemoryStore, Error> {
            MemoryManager::default().create_collection(name, prefix)
        }

        fn create_state(
            &self,
            name: &str,
            prefix: &str,
        ) -> Result<FailingState, Error> {
            Ok(FailingState {
                name: name.to_owned(),
                prefix: prefix.to_owned(),
                fail_puts: !name.ends_with("_fence"),
            })
        }

        fn stop(self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn test_light_persistence_snapshot_failure_rolls_back() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            FailingStateManager,
            None,
            initial,
        )
        .unwrap();

        assert!(
            store
                .persist_light_state(&CounterState { value: 5 })
                .is_err()
        );

        assert_eq!(store.event_counter, 0);
        assert_eq!(store.state_counter, 0);
        assert!(store.recover().unwrap().is_none());
    }

    // ------------------------------------------------------------------
    // Mock backend where snapshot writes succeed but metadata writes fail,
    // used to verify `snapshot()` rolls back `state_counter`.
    // ------------------------------------------------------------------

    #[derive(Clone)]
    struct MetadataFailState {
        inner: MemoryStore,
        fail_puts: bool,
    }

    impl State for MetadataFailState {
        fn name(&self) -> &str {
            State::name(&self.inner)
        }

        fn get(&self) -> Result<Vec<u8>, Error> {
            State::get(&self.inner)
        }

        fn put(&mut self, data: &[u8]) -> Result<(), Error> {
            if self.fail_puts {
                return Err(Error::Store {
                    operation: StoreOperation::Snapshot,
                    reason: "injected metadata failure".to_owned(),
                    source: None,
                });
            }
            State::put(&mut self.inner, data)
        }

        fn del(&mut self) -> Result<(), Error> {
            State::del(&mut self.inner)
        }

        fn purge(&mut self) -> Result<(), Error> {
            State::purge(&mut self.inner)
        }
    }

    #[derive(Default, Clone)]
    struct MetadataFailManager {
        inner: MemoryManager,
    }

    impl DbManager<MemoryStore, MetadataFailState> for MetadataFailManager {
        fn create_collection(
            &self,
            name: &str,
            prefix: &str,
        ) -> Result<MemoryStore, Error> {
            self.inner.create_collection(name, prefix)
        }

        fn create_state(
            &self,
            name: &str,
            prefix: &str,
        ) -> Result<MetadataFailState, Error> {
            Ok(MetadataFailState {
                inner: self.inner.create_state(name, prefix)?,
                fail_puts: name.contains("metadata"),
            })
        }

        fn stop(self) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test]
    fn test_snapshot_metadata_failure_rolls_back_state_counter() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MetadataFailManager::default(),
            None,
            initial,
        )
        .unwrap();

        assert!(store.snapshot(&CounterState { value: 5 }).is_err());
        assert_eq!(store.event_counter, 0);
        assert_eq!(store.state_counter, 0);
        assert_eq!(store.pending_events_since_snapshot(), 0);
    }

    #[test]
    fn test_default_store_prefix_uses_full_path() {
        use ave_actors_actor::ActorPath;

        let a = default_store_prefix(&ActorPath::from("/user/a/counter"));
        let b = default_store_prefix(&ActorPath::from("/user/b/counter"));
        assert_eq!(a, "user__a__counter");
        assert_eq!(b, "user__b__counter");
        assert_ne!(a, b);

        // Every derived prefix must satisfy backend validation.
        validate_store_prefix(&a).unwrap();
        validate_store_prefix(&b).unwrap();

        // Top-level actors keep the leaf name (unchanged behaviour).
        let top = default_store_prefix(&ActorPath::from("/user"));
        assert_eq!(top, "user");
    }

    #[test]
    fn test_default_store_prefix_long_path_is_bounded_and_stable() {
        use ave_actors_actor::ActorPath;

        let mut path = ActorPath::from("/user");
        for i in 0..30 {
            path = path / format!("segment-{i:02}-xxxxxxxxxx").as_str();
        }
        let first = default_store_prefix(&path);
        let second = default_store_prefix(&path);
        assert_eq!(first, second, "mapping must be stable across calls");
        assert!(
            first.len() <= MAX_DERIVED_PREFIX_LEN,
            "derived prefix must be bounded, got {} chars",
            first.len()
        );
        validate_store_prefix(&first).unwrap();

        // A different leaf under the same long parent must differ.
        let other = default_store_prefix(&ActorPath::from(
            format!("{path}/other").as_str(),
        ));
        assert_ne!(first, other);
    }

    #[test]
    fn test_full_persistence_stores_events() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<FullCounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store.persist(&CounterEvent(5)).unwrap();
        store.persist(&CounterEvent(3)).unwrap();

        let events: Vec<_> = store
            .events
            .iter(false)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_full_persistence_replays_events_on_recovery() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<FullCounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store.persist(&CounterEvent(5)).unwrap();
        store.persist(&CounterEvent(3)).unwrap();

        let recovered = store.recover().unwrap();
        assert_eq!(recovered.unwrap().value, 8);
    }

    #[test]
    fn test_full_persistence_snapshot_captures_pending_events() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<FullCounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store.persist(&CounterEvent(1)).unwrap();
        store.persist(&CounterEvent(2)).unwrap();
        assert!(store.get_state().unwrap().is_none());
        assert_eq!(store.pending_events_since_snapshot(), 2);

        store.snapshot(&CounterState { value: 3 }).unwrap();
        let snapshot =
            store.get_state().unwrap().expect("snapshot should exist");
        assert_eq!(snapshot.state.value, 3);
        assert_eq!(snapshot.counter, 2);
        assert_eq!(store.pending_events_since_snapshot(), 0);
    }

    #[test]
    fn test_full_persistence_pending_events_correct() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<FullCounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store.persist(&CounterEvent(1)).unwrap();
        assert_eq!(store.pending_events_since_snapshot(), 1);

        store.persist(&CounterEvent(2)).unwrap();
        assert_eq!(store.pending_events_since_snapshot(), 2);

        store.snapshot(&CounterState { value: 3 }).unwrap();
        assert_eq!(store.pending_events_since_snapshot(), 0);
    }

    #[test]
    fn test_full_persistence_last_event_present() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<FullCounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store.persist(&CounterEvent(5)).unwrap();
        store.persist(&CounterEvent(3)).unwrap();

        let last = store
            .last_event()
            .unwrap()
            .expect("last event should exist");
        assert_eq!(last.0, 3);
    }

    #[test]
    fn test_full_persistence_get_events_range() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<FullCounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        for i in 1..=5 {
            store.persist(&CounterEvent(i)).unwrap();
        }

        let events = store.events(1, 3).unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].0, 2);
        assert_eq!(events[1].0, 3);
        assert_eq!(events[2].0, 4);
    }

    #[test]
    fn test_full_persistence_recovery_with_snapshot_and_events() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<FullCounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        // Snapshot every 2 events. After 3 events: snapshot at 2, 1 pending.
        store.persist(&CounterEvent(10)).unwrap();
        store.persist(&CounterEvent(5)).unwrap();
        store.persist(&CounterEvent(3)).unwrap();

        let recovered = store.recover().unwrap();
        assert_eq!(recovered.unwrap().value, 18);
    }

    #[test]
    fn test_persist_increments_event_counter_both_strategies() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut light = Store::<CounterActor>::test_new(
            "light",
            "test",
            MemoryManager::default(),
            None,
            Arc::clone(&initial),
        )
        .unwrap();
        let mut full = Store::<FullCounterActor>::test_new(
            "full",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        light
            .persist_light_state(&CounterState { value: 1 })
            .unwrap();
        full.persist(&CounterEvent(1)).unwrap();

        assert_eq!(light.event_counter, 1);
        assert_eq!(full.event_counter, 1);
    }

    #[test]
    fn test_recover_with_empty_store_both_strategies() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut light = Store::<CounterActor>::test_new(
            "light",
            "test",
            MemoryManager::default(),
            None,
            Arc::clone(&initial),
        )
        .unwrap();
        let mut full = Store::<FullCounterActor>::test_new(
            "full",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        assert!(light.recover().unwrap().is_none());
        assert!(full.recover().unwrap().is_none());
    }

    #[test]
    fn test_snapshot_persists_state_counter() {
        let initial = Arc::new(CounterState { value: 0 });
        let mut store = Store::<CounterActor>::test_new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        store
            .persist_light_state(&CounterState { value: 1 })
            .unwrap();
        store
            .persist_light_state(&CounterState { value: 2 })
            .unwrap();
        store.snapshot(&CounterState { value: 2 }).unwrap();

        let snapshot =
            store.get_state().unwrap().expect("snapshot should exist");
        assert_eq!(snapshot.counter, 2);
    }

    #[cfg(all(test, feature = "prometheus"))]
    mod prometheus_tests {
        use super::*;
        use crate::memory::MemoryManager;
        use ave_actors_actor::ActorSystem;
        use prometheus_client::registry::Registry;
        use test_log::test;
        use tokio_util::sync::CancellationToken;

        fn pending_events_value(buf: &str, path: &str) -> Option<i64> {
            let prefix = format!(
                "ave_actors_store_pending_events{{path=\"{}\"}} ",
                path
            );
            buf.lines()
                .find(|line| line.starts_with(&prefix))
                .and_then(|line| line[prefix.len()..].trim().parse().ok())
        }

        #[test(tokio::test)]
        async fn test_store_metrics_emitted() {
            let mut registry = Registry::default();
            let metrics = Arc::new(crate::metrics::StoreMetrics::new());
            metrics.register_into(&mut registry);

            let path: Arc<str> = Arc::from("/user/counter");
            fn encode_registry(registry: &Registry) -> String {
                let mut buf = String::new();
                prometheus_client::encoding::text::encode(&mut buf, registry)
                    .expect("prometheus registry should encode to text");
                buf
            }

            let initial = Arc::new(CounterState::default());
            let store = Store::<CounterActor>::new(
                "store",
                "test",
                MemoryManager::default(),
                None,
                initial,
                Some(metrics.clone()),
                Arc::clone(&path),
            )
            .expect("store should be created");

            let (system, mut runner) = ActorSystem::create(
                CancellationToken::new(),
                CancellationToken::new(),
            );
            tokio::spawn(async move {
                runner.run().await;
            });

            let store_ref = system
                .create_root_actor("store", store)
                .await
                .expect("root store actor should be created");

            let response = store_ref
                .ask(StoreCommand::Recover)
                .await
                .expect("recover command should succeed");
            assert!(matches!(response, StoreResponse::State(None)));

            let buf = encode_registry(&registry);
            assert!(
                buf.contains("ave_actors_store_operation_duration_seconds")
            );
            assert!(buf.contains("operation=\"recover\""));
            assert!(buf.contains("ave_actors_store_pending_events"));
            assert_eq!(pending_events_value(&buf, &path), Some(0));

            store_ref
                .ask(StoreCommand::PersistFull {
                    event: Arc::new(CounterEvent(5)),
                    state: Arc::new(CounterState::default()),
                    snapshot_every: None,
                })
                .await
                .expect("persist command should succeed");

            let buf = encode_registry(&registry);
            assert!(buf.contains("operation=\"persist_full\""));
            assert_eq!(pending_events_value(&buf, &path), Some(1));

            store_ref
                .ask(StoreCommand::Snapshot(Arc::new(CounterState {
                    value: 10,
                })))
                .await
                .expect("snapshot command should succeed");

            let buf = encode_registry(&registry);
            assert!(buf.contains("operation=\"snapshot\""));
            assert_eq!(pending_events_value(&buf, &path), Some(0));

            store_ref
                .ask(StoreCommand::PersistLight(Arc::new(CounterState {
                    value: 5,
                })))
                .await
                .expect("persist light command should succeed");

            let buf = encode_registry(&registry);
            assert_eq!(pending_events_value(&buf, &path), Some(0));

            store_ref
                .ask(StoreCommand::PersistFull {
                    event: Arc::new(CounterEvent(3)),
                    state: Arc::new(CounterState { value: 8 }),
                    snapshot_every: Some(100),
                })
                .await
                .expect("persist full command should succeed");

            let buf = encode_registry(&registry);
            assert_eq!(pending_events_value(&buf, &path), Some(1));
        }
    }
}
