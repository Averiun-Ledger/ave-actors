//! Event-sourced persistence for actors via [`PersistentActor`].

use crate::{
    database::{Collection, DbManager, State},
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
    aead::{Aead, AeadCore, KeyInit, OsRng},
};

use serde::{Deserialize, Serialize};

use tracing::{debug, error, info_span, warn};

use std::fmt::Debug;

/// Nonce size for XChaCha20-Poly1305 encryption.
const NONCE_SIZE: usize = 24;

fn store_error(operation: StoreOperation, reason: impl ToString) -> Error {
    Error::Store {
        operation,
        reason: reason.to_string(),
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

/// Selects the persistence strategy used by a [`PersistentActor`].
///
/// `Light` trades storage space for fast recovery; `Full` trades recovery speed
/// for a smaller storage footprint and a complete audit trail.
#[derive(Debug, Clone)]
pub enum PersistenceType {
    /// Each event is stored together with a state snapshot.
    ///
    /// Recovery is fast (load snapshot, done) at the cost of storing the full
    /// state on every write.
    Light,
    /// Only events are stored; state is reconstructed by replaying them.
    ///
    /// Uses less storage and preserves a complete audit trail, but recovery time
    /// grows with the number of events since the last snapshot.
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

/// Wrapper that guarantees a [`PersistentActor`] was constructed via [`PersistentActor::initial`].
///
/// The actor system requires this wrapper for persistent actors, preventing users
/// from bypassing the initialization logic by constructing the actor struct directly.
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

/// Extends [`Actor`] with event-sourced state persistence.
///
/// Implement `apply` to define how each event mutates the in-memory state, then
/// call `persist` inside `handle_message` to durably record a change. The actor
/// system automatically recovers state on restart by replaying events and/or loading
/// the latest snapshot, depending on the chosen [`Persistence`] strategy.
///
/// Do NOT also implement `NotPersistentActor` on the same type.
#[async_trait]
pub trait PersistentActor:
    Actor + Handler<Self> + Debug + Clone + BorshSerialize + BorshDeserialize
where
    Self::Event: BorshSerialize + BorshDeserialize,
{
    /// The persistence strategy ([`LightPersistence`] or [`FullPersistence`]).
    type Persistence: Persistence;

    /// Parameters passed to [`create_initial`](PersistentActor::create_initial). Use `()` if no initialization data is needed.
    type InitParams;

    /// Creates the actor in its default initial state from the given parameters.
    ///
    /// This is called by [`initial`](PersistentActor::initial) and also during recovery
    /// when no snapshot exists and events must be replayed from scratch.
    fn create_initial(params: Self::InitParams) -> Self;

    /// Returns an [`InitializedActor`] wrapping the actor's initial state.
    ///
    /// This is the only way to create a persistent actor instance accepted by
    /// `create_root_actor` and `create_child`. Pass the result directly to those methods.
    fn initial(params: Self::InitParams) -> InitializedActor<Self>
    where
        Self: Sized,
    {
        InitializedActor::new(Self::create_initial(params))
    }

    /// Applies `event` to the actor's in-memory state.
    ///
    /// This method must be deterministic — the same event applied to the same state
    /// must always produce the same result. It should only update in-memory fields;
    /// persistence is handled by [`persist`](PersistentActor::persist).
    /// Returns an error if the event represents an invalid state transition.
    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError>;

    /// Replaces the current state with `state`, used during snapshot recovery.
    ///
    /// The default implementation is `*self = state`. Override if you need to
    /// preserve fields that should not be overwritten during recovery.
    fn update(&mut self, state: Self) {
        *self = state;
    }

    /// Snapshot cadence for `FullPersistence`.
    ///
    /// - `None`: snapshots are only manual or done during store shutdown.
    /// - `Some(n)`: after every `n` persisted events since the last snapshot,
    ///   the store snapshots the current actor state automatically.
    ///
    /// Default: `Some(100)`.
    fn snapshot_every() -> Option<u64> {
        Some(100)
    }

    /// Whether already snapshotted events should be compacted after a
    /// successful snapshot.
    ///
    /// Default: `false`, so `FullPersistence` keeps the full audit log unless
    /// the actor opts into retention explicitly.
    fn compact_on_snapshot() -> bool {
        false
    }

    /// Applies `event` to the in-memory state and durably persists it.
    ///
    /// Calls [`apply`](PersistentActor::apply) first; if that succeeds, sends the event
    /// (and optionally the current state) to the child `store` actor. On any failure the
    /// in-memory state is rolled back to its pre-call value. For `LightPersistence`, both
    /// the event and a state snapshot are written atomically; for `FullPersistence`, only
    /// the event is written, with periodic automatic snapshots controlled by [`snapshot_every`](PersistentActor::snapshot_every).
    async fn persist(
        &mut self,
        event: &Self::Event,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let store = ctx.get_child::<Store<Self>>("store").await?;

        let prev_state = self.clone();

        if let Err(e) = self.apply(event) {
            self.update(prev_state.clone());
            return Err(e);
        }

        let response = match Self::Persistence::get_persistence() {
            PersistenceType::Light => {
                match store
                    .ask(StoreCommand::PersistLight(
                        event.clone(),
                        self.clone(),
                    ))
                    .await
                {
                    Ok(response) => response,
                    Err(e) => {
                        self.update(prev_state.clone());
                        return Err(actor_store_error(
                            StoreOperation::PersistLight,
                            e,
                        ));
                    }
                }
            }
            PersistenceType::Full => {
                match store
                    .ask(StoreCommand::PersistFullEvent {
                        event: event.clone(),
                        snapshot_every: Self::snapshot_every(),
                    })
                    .await
                {
                    Ok(response) => response,
                    Err(e) => {
                        self.update(prev_state.clone());
                        return Err(actor_store_error(
                            StoreOperation::PersistFull,
                            e,
                        ));
                    }
                }
            }
        };

        match response {
            StoreResponse::Persisted => Ok(()),
            StoreResponse::SnapshotRequired => {
                self.snapshot(ctx).await?;
                Ok(())
            }
            _ => {
                self.update(prev_state);
                Err(ActorError::UnexpectedResponse {
                    path: ActorPath::from(format!(
                        "{}/store",
                        ctx.path().clone()
                    )),
                    expected:
                        "StoreResponse::Persisted | StoreResponse::SnapshotRequired"
                            .to_owned(),
                })
            }
        }
    }

    /// Sends the current state to the child `store` actor to be saved as a snapshot.
    ///
    /// Snapshots speed up recovery by reducing the number of events that need to be
    /// replayed. For most use cases, snapshots are triggered automatically; call this
    /// manually only when you need an immediate checkpoint.
    async fn snapshot(
        &self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let store = ctx.get_child::<Store<Self>>("store").await?;
        store
            .ask(StoreCommand::Snapshot(self.clone()))
            .await
            .map_err(|e| actor_store_error(StoreOperation::Snapshot, e))?;
        Ok(())
    }

    /// Creates the child `store` actor, opens the storage backend, and recovers any persisted state.
    ///
    /// Call this from [`pre_start`](ave_actors_actor::Actor::pre_start), passing the database manager
    /// and an optional encryption key. If a persisted state exists, it is loaded and applied to `self`
    /// before `pre_start` returns. `prefix` scopes the storage keys; if `None`, the actor's path key is used.
    async fn start_store<C: Collection, S: State>(
        &mut self,
        name: &str,
        prefix: Option<String>,
        ctx: &mut ActorContext<Self>,
        manager: impl DbManager<C, S>,
        key_box: Option<EncryptedKey>,
    ) -> Result<(), ActorError> {
        let prefix = prefix.unwrap_or_else(|| ctx.path().key());

        let store =
            Store::<Self>::new(name, &prefix, manager, key_box, self.clone())
                .map_err(|e| actor_store_error(StoreOperation::StoreInit, e))?;
        let store = ctx.create_child("store", store).await?;
        let response = store.ask(StoreCommand::Recover).await?;

        if let StoreResponse::State(Some(state)) = response {
            self.update(state);
        }

        Ok(())
    }
}

/// Internal child actor that manages event and snapshot persistence for a [`PersistentActor`].
///
/// Created automatically by [`start_store`](PersistentActor::start_store). It stores events in
/// a [`Collection`](crate::database::Collection) with zero-padded sequence-number keys and
/// snapshots in a [`State`](crate::database::State) store. Data can optionally be encrypted
/// with XChaCha20-Poly1305.
pub struct Store<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
    /// Next free event index.
    ///
    /// This is not the number of the last persisted event; it is the index that
    /// the next persisted event will use.
    event_counter: u64,
    /// Number of events already included in the latest snapshot.
    ///
    /// If `state_counter == 10`, the snapshot contains events `0..=9`.
    state_counter: u64,
    /// Exclusive upper bound of the event range already compacted from the log.
    compacted_until: u64,
    /// Collection for storing events with sequence numbers as keys.
    events: Box<dyn Collection>,
    /// Storage for the latest state snapshot.
    states: Box<dyn State>,
    /// Storage for log metadata used to resume after snapshots/compaction.
    metadata: Box<dyn State>,
    /// Encrypted password for data encryption (XChaCha20-Poly1305).
    /// If None, data is stored unencrypted.
    key_box: Option<EncryptedKey>,
    /// Initial state to use when recovering without a snapshot.
    /// This is the state created with `create_initial(params)`.
    initial_state: P,
}

impl<P> ave_actors_actor::NotPersistentActor for Store<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct StoreMetadata {
    next_event_index: u64,
    compacted_until: u64,
}

impl<P> Store<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
    /// Creates and initializes the store, opening the three backend stores (events, state, metadata).
    ///
    /// `name` is used to derive the collection names; `prefix` scopes the keys.
    /// On construction the event counter is read from persisted metadata (or inferred from the
    /// event log for backward compatibility). Returns an error if any backend allocation fails.
    pub fn new<C, S>(
        name: &str,
        prefix: &str,
        manager: impl DbManager<C, S>,
        key_box: Option<EncryptedKey>,
        initial_state: P,
    ) -> Result<Self, Error>
    where
        C: Collection + 'static,
        S: State + 'static,
    {
        let events =
            manager.create_collection(&format!("{}_events", name), prefix)?;
        let states =
            manager.create_state(&format!("{}_states", name), prefix)?;
        let metadata =
            manager.create_state(&format!("{}_metadata", name), prefix)?;

        let mut store = Self {
            event_counter: 0,
            state_counter: 0,
            compacted_until: 0,
            events: Box::new(events),
            states: Box::new(states),
            metadata: Box::new(metadata),
            key_box,
            initial_state,
        };

        // Initialize event_counter from persisted metadata when available.
        // Fall back to the latest snapshot boundary and event log for
        // backwards compatibility with stores created before metadata existed.
        let last_event_counter = if let Some((key, _)) = store.events.last()? {
            key.parse::<u64>()
                .map_err(|e| store_error(StoreOperation::ParseEventKey, e))?
                + 1
        } else {
            0
        };

        let snapshot_counter = if let Some((_, counter)) = store.get_state()? {
            counter
        } else {
            0
        };

        if let Some(metadata) = store.get_metadata()? {
            store.event_counter =
                last_event_counter.max(metadata.next_event_index);
            store.compacted_until = metadata.compacted_until;
        } else {
            store.event_counter = last_event_counter.max(snapshot_counter);
        }

        debug!(
            "Initializing Store with event_counter: {}, compacted_until: {}",
            store.event_counter, store.compacted_until
        );

        Ok(store)
    }

    const fn pending_events_since_snapshot(&self) -> u64 {
        self.event_counter.saturating_sub(self.state_counter)
    }

    fn get_metadata(&self) -> Result<Option<StoreMetadata>, Error> {
        let data = match self.metadata.get() {
            Ok(data) => data,
            Err(Error::EntryNotFound { .. }) => return Ok(None),
            Err(err) => return Err(err),
        };

        let bytes = if let Some(key_box) = &self.key_box {
            self.decrypt(key_box, data.as_slice())?
        } else {
            data
        };

        let metadata: StoreMetadata =
            borsh::from_slice(&bytes).map_err(|e| {
                error!("Can't decode metadata: {}", e);
                store_error(StoreOperation::DecodeState, e)
            })?;

        Ok(Some(metadata))
    }

    fn persist_metadata(&mut self) -> Result<(), Error> {
        let metadata = StoreMetadata {
            next_event_index: self.event_counter,
            compacted_until: self.compacted_until,
        };
        let data = borsh::to_vec(&metadata).map_err(|e| {
            error!("Can't encode metadata: {}", e);
            store_error(StoreOperation::EncodeActor, e)
        })?;

        let bytes = if let Some(key_box) = &self.key_box {
            self.encrypt(key_box, data.as_slice())?
        } else {
            data
        };

        self.metadata.put(&bytes)
    }

    fn compact_to_snapshot(&mut self) -> Result<(), Error> {
        for idx in self.compacted_until..self.state_counter {
            let key = format!("{:020}", idx);
            match self.events.del(&key) {
                Ok(()) | Err(Error::EntryNotFound { .. }) => {
                    self.compacted_until = idx + 1;
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// Serializes and stores `event` at the next sequence position, then increments the event counter.
    fn persist<E>(&mut self, event: &E) -> Result<(), Error>
    where
        E: Event + BorshSerialize + BorshDeserialize,
    {
        debug!("Persisting event: {:?}", event);

        let bytes = borsh::to_vec(event).map_err(|e| {
            error!("Can't encode event: {}", e);
            store_error(StoreOperation::EncodeEvent, e)
        })?;

        let bytes = if let Some(key_box) = &self.key_box {
            self.encrypt(key_box, &bytes)?
        } else {
            bytes
        };

        // Calculate next event position (0-indexed)
        // event_counter works like vector.len(): 0 means empty, 1 means one event at position 0
        let next_event_number = self.event_counter;

        debug!(
            "Persisting event {} at index {}",
            std::any::type_name::<E>(),
            next_event_number
        );

        // First persist the event, then increment counter (atomic operation)
        let result = self
            .events
            .put(&format!("{:020}", next_event_number), &bytes);

        // Only increment counter if persist was successful
        if result.is_ok() {
            self.event_counter += 1;
            debug!(
                "Successfully persisted event, event_counter now: {}",
                self.event_counter
            );
        }

        result
    }

    /// Atomically stores `event` and a state snapshot (used for `LightPersistence`). Rolls back the event if the snapshot fails.
    fn persist_state<E>(&mut self, event: &E, state: &P) -> Result<(), Error>
    where
        E: Event + BorshSerialize + BorshDeserialize,
    {
        debug!("Persisting event: {:?}", event);

        let bytes = borsh::to_vec(event).map_err(|e| {
            error!("Can't encode event: {}", e);
            store_error(StoreOperation::EncodeEvent, e)
        })?;

        let bytes = if let Some(key_box) = &self.key_box {
            self.encrypt(key_box, &bytes)?
        } else {
            bytes
        };

        // Calculate next event position (0-indexed)
        // event_counter works like vector.len(): 0 means empty, 1 means one event at position 0
        let next_event_number = self.event_counter;

        debug!(
            "Persisting event {} at index {} with LightPersistence",
            std::any::type_name::<E>(),
            next_event_number
        );

        // 1. First persist the event
        let event_key = format!("{:020}", next_event_number);
        let result = self.events.put(&event_key, &bytes);

        // 2. Only increment counter if persist was successful
        if result.is_ok() {
            self.event_counter += 1;
            debug!(
                "Successfully persisted event, event_counter now: {}",
                self.event_counter
            );
        } else {
            return result;
        }

        // 3. NOW create snapshot with the updated event_counter
        // This ensures state_counter = event_counter after the snapshot
        if let Err(snapshot_err) = self.snapshot(state) {
            self.event_counter = next_event_number;
            if let Err(rollback_err) = self.events.del(&event_key) {
                return Err(store_error(
                    StoreOperation::RollbackPersistLight,
                    format!(
                        "snapshot failed: {}; rollback delete failed: {}",
                        snapshot_err, rollback_err
                    ),
                ));
            }
            return Err(snapshot_err);
        }

        Ok(())
    }

    /// Returns the most recently persisted event, or `None` if no events have been stored.
    fn last_event(&self) -> Result<Option<P::Event>, Error> {
        if let Some((_, data)) = self.events.last()? {
            let data = if let Some(key_box) = &self.key_box {
                self.decrypt(key_box, data.as_slice())?
            } else {
                data
            };

            let event: P::Event = borsh::from_slice(&data).map_err(|e| {
                error!("Can't decode event: {}", e);
                store_error(StoreOperation::DecodeEvent, e)
            })?;

            Ok(Some(event))
        } else {
            Ok(None)
        }
    }

    fn get_state(&self) -> Result<Option<(P, u64)>, Error> {
        let data = match self.states.get() {
            Ok(data) => data,
            Err(e) => {
                if let Error::EntryNotFound { .. } = e {
                    return Ok(None);
                } else {
                    return Err(e);
                }
            }
        };

        let bytes = if let Some(key_box) = &self.key_box {
            self.decrypt(key_box, data.as_slice())?
        } else {
            data
        };

        let state: (P, u64) = borsh::from_slice(&bytes).map_err(|e| {
            error!("Can't decode state: {}", e);
            store_error(StoreOperation::DecodeState, e)
        })?;

        Ok(Some(state))
    }

    /// Retrieve events.
    fn events(&self, from: u64, to: u64) -> Result<Vec<P::Event>, Error> {
        if from > to {
            return Ok(Vec::new());
        }

        let mut events = Vec::new();

        for i in from..=to {
            let key = format!("{:020}", i);
            let data = self.events.get(&key)?;
            let data = if let Some(key_box) = &self.key_box {
                self.decrypt(key_box, data.as_slice())?
            } else {
                data
            };

            let event: P::Event = borsh::from_slice(&data).map_err(|e| {
                error!("Can't decode event: {}", e);
                store_error(StoreOperation::DecodeEvent, e)
            })?;

            events.push(event);
        }
        Ok(events)
    }

    /// Retrieve events for external queries, tolerating out-of-range bounds.
    ///
    /// This keeps recovery strict while making StoreCommand::GetEvents behave
    /// like a range query: if the requested interval does not intersect with
    /// existing events, the result is empty; if it overlaps partially, only
    /// the existing suffix is returned.
    fn query_events(&self, from: u64, to: u64) -> Result<Vec<P::Event>, Error> {
        if from > to || from >= self.event_counter {
            return Ok(Vec::new());
        }

        let upper = to.min(self.event_counter.saturating_sub(1));
        self.events(from, upper)
    }

    /// Serializes `actor`, stores it as the current snapshot, updates the state counter and metadata, and optionally compacts old events.
    fn snapshot(&mut self, actor: &P) -> Result<(), Error> {
        debug!("Snapshotting state: {:?}", actor);

        let next_state_counter = self.event_counter;

        let data =
            borsh::to_vec(&(actor, next_state_counter)).map_err(|e| {
                error!("Can't encode actor: {}", e);
                store_error(StoreOperation::EncodeActor, e)
            })?;

        let bytes = if let Some(key_box) = &self.key_box {
            self.encrypt(key_box, data.as_slice())?
        } else {
            data
        };

        self.states.put(&bytes)?;
        self.state_counter = next_state_counter;
        self.persist_metadata()?;
        if P::compact_on_snapshot() {
            if let Err(err) = self.compact_to_snapshot() {
                warn!(
                    error = %err,
                    "Snapshot persisted but event compaction failed; keeping event log"
                );
            } else if let Err(err) = self.persist_metadata() {
                warn!(
                    error = %err,
                    "Snapshot metadata persisted but compaction watermark update failed"
                );
            }
        }
        Ok(())
    }

    /// Loads the latest snapshot (if any) and replays all subsequent events to reconstruct the current state.
    fn recover(&mut self) -> Result<Option<P>, Error> {
        debug!("Starting recovery process");

        if let Some((mut state, counter)) = self.get_state()? {
            self.state_counter = counter;
            debug!("Recovered state with counter: {}", counter);

            let last_event_counter =
                if let Some((key, ..)) = self.events.last()? {
                    key.parse::<u64>().map_err(|e| {
                        store_error(StoreOperation::ParseEventKey, e)
                    })? + 1
                } else {
                    0
                };

            // When old events have been compacted away, the snapshot counter is
            // still the authoritative lower bound for the next event index.
            self.event_counter = self.state_counter.max(last_event_counter);

            debug!(
                "Recovery state: event_counter={}, state_counter={}",
                self.event_counter, self.state_counter
            );

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
                let events =
                    self.events(self.state_counter, self.event_counter - 1)?;
                debug!("Found {} events to replay", events.len());

                for (i, event) in events.iter().enumerate() {
                    debug!("Applying event {} of {}", i + 1, events.len());
                    state.apply(event).map_err(|e| {
                        store_error(StoreOperation::ApplyEvent, e)
                    })?;
                }

                debug!(
                    "Updating snapshot after applying {} events",
                    events.len()
                );
                self.snapshot(&state)?;
                debug!(
                    "Recovery completed. Final event_counter: {}",
                    self.event_counter
                );
                // Note: We don't increment event_counter here as it already has the correct value
                // from the last persisted event key or the latest snapshot boundary.
            } else {
                debug!("State is up to date, no events to apply");
            }

            Ok(Some(state))
        } else {
            debug!("No previous state found");

            // Check if there are any events in the database
            if let Some((key, ..)) = self.events.last()? {
                debug!(
                    "No snapshot but events found - replaying from beginning"
                );

                self.event_counter = key.parse::<u64>().map_err(|e| {
                    store_error(StoreOperation::ParseEventKey, e)
                })? + 1;
                self.state_counter = 0;

                debug!(
                    "Using provided initial state and applying {} events",
                    self.event_counter
                );

                // Use the initial state provided during Store creation
                // This was created with create_initial(params) by the user
                let mut state = self.initial_state.clone();

                // Apply ALL events from the beginning
                let events = self.events(0, self.event_counter - 1)?;
                debug!("Replaying {} events from scratch", events.len());

                for (i, event) in events.iter().enumerate() {
                    debug!("Applying event {} of {}", i + 1, events.len());
                    state.apply(event).map_err(|e| {
                        store_error(StoreOperation::ApplyEvent, e)
                    })?;
                }

                // Create snapshot for future recoveries
                debug!("Creating snapshot after replaying events");
                self.snapshot(&state)?;

                debug!(
                    "Recovery completed. Final event_counter: {}",
                    self.event_counter
                );

                Ok(Some(state))
            } else {
                debug!("No previous state and no events found, starting fresh");
                Ok(None)
            }
        }
    }

    /// Snapshot the current reconstructed state if there are events since the
    /// last snapshot. Only relevant for FullPersistence — LightPersistence
    /// already stores state with every event.
    ///
    /// Called automatically during shutdown via `pre_stop`.
    fn snapshot_if_needed(&mut self) -> Result<(), Error> {
        if !matches!(P::Persistence::get_persistence(), PersistenceType::Full) {
            return Ok(());
        }

        if self.event_counter == 0 || self.event_counter <= self.state_counter {
            return Ok(());
        }

        // Reconstruct state from last snapshot (or initial) + pending events.
        let mut state = if let Some((s, _)) = self.get_state()? {
            s
        } else {
            self.initial_state.clone()
        };

        let events = self.events(self.state_counter, self.event_counter - 1)?;
        for event in &events {
            state.apply(event).map_err(|e| {
                store_error(StoreOperation::ApplyEventOnStop, e)
            })?;
        }

        self.snapshot(&state)
    }

    /// Deletes all events, snapshots, and metadata, then resets all counters to zero.
    ///
    /// This permanently destroys all persisted data for the actor and cannot be undone.
    pub fn purge(&mut self) -> Result<(), Error> {
        self.events.purge()?;
        self.states.purge()?;
        self.metadata.purge()?;
        self.event_counter = 0;
        self.state_counter = 0;
        self.compacted_until = 0;
        Ok(())
    }

    /// Encrypts `bytes` with XChaCha20-Poly1305 using a fresh random nonce.
    ///
    /// The output format is `[nonce (24 bytes) || ciphertext || Poly1305 tag (16 bytes)]`.
    /// Returns an error if the key cannot be decrypted from the [`EncryptedKey`] or if
    /// the AEAD encryption fails.
    fn encrypt(
        &self,
        key_box: &EncryptedKey,
        bytes: &[u8],
    ) -> Result<Vec<u8>, Error> {
        if let Ok(key) = key_box.key() {
            // Validate key size (XChaCha20-Poly1305 requires exactly 32 bytes)
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
                });
            }

            // Create cipher from key
            let cipher = XChaCha20Poly1305::new(key.as_ref().into());

            // Generate cryptographically secure random nonce (192-bits/24-bytes)
            // XChaCha20 uses extended nonce, virtually eliminating collision risk
            let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);

            // Encrypt and authenticate the data
            // XChaCha20-Poly1305 provides both confidentiality and authenticity
            let ciphertext: Vec<u8> =
                cipher.encrypt(&nonce, bytes.as_ref()).map_err(|e| {
                    error!(error = %e, "Encryption failed");
                    store_error(StoreOperation::EncryptData, e)
                })?;

            // Prepend nonce to ciphertext for storage
            // Format: [nonce (24 bytes) || ciphertext || poly1305_tag (16 bytes)]
            Ok([nonce.to_vec(), ciphertext].concat())
        } else {
            error!("Failed to decrypt encryption key");
            Err(store_error(StoreOperation::DecryptKey, "Can't decrypt key"))
        }
    }

    /// Decrypts a XChaCha20-Poly1305 ciphertext produced by [`encrypt`](Store::encrypt).
    ///
    /// Reads the nonce from the first 24 bytes, then decrypts and verifies the authentication
    /// tag. Returns an error if the ciphertext is too short, the key is unavailable, or
    /// authentication fails (possible tampering or corruption).
    fn decrypt(
        &self,
        key_box: &EncryptedKey,
        ciphertext: &[u8],
    ) -> Result<Vec<u8>, Error> {
        // Validate ciphertext length (nonce + tag minimum = 24 + 16 = 40 bytes)
        if ciphertext.len() < NONCE_SIZE + 16 {
            warn!(
                expected_min = NONCE_SIZE + 16,
                got = ciphertext.len(),
                "Invalid ciphertext length, possible corruption"
            );
            return Err(Error::Store {
                operation: StoreOperation::ValidateCiphertext,
                reason: format!(
                    "Invalid ciphertext length: expected at least {} bytes, got {}",
                    NONCE_SIZE + 16,
                    ciphertext.len()
                ),
            });
        }

        if let Ok(key) = key_box.key() {
            // Validate key size (XChaCha20-Poly1305 requires exactly 32 bytes)
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

            // Extract nonce from the beginning of ciphertext
            let nonce = XNonce::from_slice(&ciphertext[..NONCE_SIZE]);

            // Extract actual ciphertext (includes Poly1305 authentication tag at the end)
            let ciphertext_data = &ciphertext[NONCE_SIZE..];

            // Create cipher and decrypt
            let cipher = XChaCha20Poly1305::new(key.as_ref().into());

            // Decrypt and verify authentication tag
            // This will fail if data has been tampered with or corrupted
            let plaintext =
                cipher.decrypt(nonce, ciphertext_data).map_err(|e| {
                    warn!(error = %e, "Decryption failed, possible tampering or corruption");
                    store_error(
                        StoreOperation::DecryptData,
                        format!(
                            "Decryption failed (possible tampering): {}",
                            e
                        ),
                    )
                })?;

            Ok(plaintext)
        } else {
            error!("Failed to decrypt decryption key");
            Err(store_error(StoreOperation::DecryptKey, "Can't decrypt key"))
        }
    }
}

/// Commands processed by the internal [`Store`] actor.
#[derive(Debug, Clone)]
pub enum StoreCommand<P, E> {
    /// Persist an event without forcing a snapshot.
    Persist(E),
    /// Persist an event and report whether the caller should snapshot now.
    PersistFullEvent {
        /// Event to append to the event log.
        event: E,
        /// Snapshot cadence for `FullPersistence`.
        snapshot_every: Option<u64>,
    },
    /// Persist an event and snapshot the supplied actor state if required.
    PersistFull {
        /// Event to append to the event log.
        event: E,
        /// Current actor state, used when a snapshot is triggered.
        actor: P,
        /// Snapshot cadence for `FullPersistence`.
        snapshot_every: Option<u64>,
    },
    /// Persist an event together with a snapshot of the supplied actor state.
    PersistLight(E, P),
    /// Snapshot the supplied actor state immediately.
    Snapshot(P),
    /// Remove event log entries already covered by the latest snapshot.
    Compact,
    /// Return the most recently persisted event.
    LastEvent,
    /// Return the next free event index.
    LastEventNumber,
    /// Return all events from the supplied event index to the end of the log.
    LastEventsFrom(u64),
    /// Return all events within the inclusive `[from, to]` range.
    GetEvents { from: u64, to: u64 },
    /// Recover the current actor state from snapshots and events.
    Recover,
    /// Delete all events, snapshots, and metadata for this actor.
    Purge,
}

impl<P, E> Message for StoreCommand<P, E>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
    E: Event + BorshSerialize + BorshDeserialize,
{
}

/// Responses returned by the [`Store`] actor for each [`StoreCommand`].
#[derive(Debug, Clone)]
pub enum StoreResponse<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
    /// Command completed without a payload.
    None,
    /// An event was persisted successfully.
    Persisted,
    /// A full-persistence write succeeded and a snapshot should be taken now.
    SnapshotRequired,
    /// A snapshot was stored successfully.
    Snapshotted,
    /// Event compaction completed successfully.
    Compacted,
    /// Recovered actor state, or `None` when no persisted state exists.
    State(Option<P>),
    /// Most recently persisted event, or `None` when the log is empty.
    LastEvent(Option<P::Event>),
    /// Next free event index.
    LastEventNumber(u64),
    /// Event payloads returned by a range query.
    Events(Vec<P::Event>),
}

impl<P> Response for StoreResponse<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
}

/// Events emitted by the [`Store`] actor (e.g. after a successful persist or snapshot).
#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
pub enum StoreEvent {
    /// Emitted after a successful persist operation.
    Persisted,
    /// Emitted after a successful snapshot.
    Snapshotted,
}

impl Event for StoreEvent {}

#[async_trait]
impl<P> Actor for Store<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
    type Message = StoreCommand<P, P::Event>;
    type Response = StoreResponse<P>;
    type Event = StoreEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("Store", id = %id)
    }

    /// On shutdown, snapshot the current state if there are unsnapshotted
    /// events (FullPersistence only). This replaces the old `stop_store()`
    /// pattern where the parent actor was responsible for triggering snapshots.
    async fn pre_stop(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        if let Err(e) = self.snapshot_if_needed() {
            error!(error = %e, "Failed to snapshot state during Store shutdown");
            let _ = ctx
                .emit_error(actor_store_error(
                    StoreOperation::EmitPreStopError,
                    e,
                ))
                .await;
        }
        Ok(())
    }
}

#[async_trait]
impl<P> Handler<Self> for Store<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: StoreCommand<P, P::Event>,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<StoreResponse<P>, ActorError> {
        // Match the command.
        match msg {
            // Persist an event.
            StoreCommand::Persist(event) => {
                self.persist(&event).map_err(|e| {
                    actor_store_error(StoreOperation::Persist, e)
                })?;
                debug!("Persisted event: {:?}", event);
                Ok(StoreResponse::Persisted)
            }
            StoreCommand::PersistFullEvent {
                event,
                snapshot_every,
            } => {
                self.persist(&event).map_err(|e| {
                    actor_store_error(StoreOperation::PersistFull, e)
                })?;

                if snapshot_every.is_some_and(|every| {
                    self.pending_events_since_snapshot() >= every
                }) {
                    debug!("Persisted full event and snapshot is now required");
                    Ok(StoreResponse::SnapshotRequired)
                } else {
                    debug!("Persisted full event: {:?}", event);
                    Ok(StoreResponse::Persisted)
                }
            }
            StoreCommand::PersistFull {
                event,
                actor,
                snapshot_every,
            } => {
                self.persist(&event).map_err(|e| {
                    actor_store_error(StoreOperation::PersistFull, e)
                })?;

                if snapshot_every.is_some_and(|every| {
                    self.pending_events_since_snapshot() >= every
                }) {
                    self.snapshot(&actor).map_err(|e| {
                        actor_store_error(StoreOperation::Snapshot, e)
                    })?;
                }

                debug!("Persisted full event: {:?}", event);
                Ok(StoreResponse::Persisted)
            }
            // Light persistence of an event.
            StoreCommand::PersistLight(event, actor) => {
                self.persist_state(&event, &actor).map_err(|e| {
                    actor_store_error(StoreOperation::PersistLight, e)
                })?;
                debug!("Light persistence of event: {:?}", event);
                Ok(StoreResponse::Persisted)
            }
            // Snapshot the state.
            StoreCommand::Snapshot(actor) => {
                self.snapshot(&actor).map_err(|e| {
                    actor_store_error(StoreOperation::Snapshot, e)
                })?;
                debug!("Snapshotted state: {:?}", actor);
                Ok(StoreResponse::Snapshotted)
            }
            StoreCommand::Compact => {
                self.compact_to_snapshot().map_err(|e| {
                    actor_store_error(StoreOperation::Compact, e)
                })?;
                debug!("Compacted events covered by the latest snapshot");
                Ok(StoreResponse::Compacted)
            }
            // Recover the state.
            StoreCommand::Recover => {
                let state = self.recover().map_err(|e| {
                    actor_store_error(StoreOperation::Recover, e)
                })?;
                debug!("Recovered state: {:?}", state);
                Ok(StoreResponse::State(state))
            }
            StoreCommand::GetEvents { from, to } => {
                let events = self.query_events(from, to).map_err(|e| {
                    actor_store_error(
                        StoreOperation::GetEventsRange,
                        format!("Unable to get events range: {}", e),
                    )
                })?;
                Ok(StoreResponse::Events(events))
            }
            // Get the last event.
            StoreCommand::LastEvent => {
                let event = self.last_event().map_err(|e| {
                    actor_store_error(StoreOperation::LastEvent, e)
                })?;
                debug!("Last event: {:?}", event);
                Ok(StoreResponse::LastEvent(event))
            }
            // Purge the store.
            StoreCommand::Purge => {
                self.purge()
                    .map_err(|e| actor_store_error(StoreOperation::Purge, e))?;
                debug!("Purged store");
                Ok(StoreResponse::None)
            }
            // Get the last event number.
            StoreCommand::LastEventNumber => {
                Ok(StoreResponse::LastEventNumber(self.event_counter))
            }
            // Get the last events from a number of counter.
            StoreCommand::LastEventsFrom(from) => {
                let to = self.event_counter.saturating_sub(1);
                let events = self.events(from, to).map_err(|e| {
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

#[cfg(test)]
mod tests {
    use std::vec;
    use test_log::test;
    use tokio_util::sync::CancellationToken;
    use tracing::info_span;

    use super::*;
    use crate::memory::MemoryManager;

    use ave_actors_actor::{ActorRef, ActorSystem, Error as ActorError};

    use async_trait::async_trait;

    #[derive(
        Debug,
        Clone,
        Serialize,
        Deserialize,
        BorshSerialize,
        BorshDeserialize,
        Default,
    )]
    struct TestActor {
        pub version: usize,
        pub value: i32,
    }

    #[derive(
        Debug,
        Clone,
        Serialize,
        Deserialize,
        BorshSerialize,
        BorshDeserialize,
        Default,
    )]
    struct TestActorLight {
        pub data: Vec<i32>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum TestMessageLight {
        SetData(Vec<i32>),
        GetData,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum TestMessage {
        Increment(i32),
        Recover,
        Snapshot,
        GetValue,
    }

    impl Message for TestMessage {}
    impl Message for TestMessageLight {}

    #[derive(
        Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
    )]
    struct TestEvent(i32);

    impl Event for TestEvent {}

    #[derive(
        Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
    )]
    struct TestEventLight(Vec<i32>);

    impl Event for TestEventLight {}

    #[derive(Debug, Clone, PartialEq)]
    enum TestResponse {
        Value(i32),
        None,
    }

    #[derive(Debug, Clone, PartialEq)]
    enum TestResponseLight {
        Data(Vec<i32>),
        None,
    }

    impl Response for TestResponse {}
    impl Response for TestResponseLight {}

    #[async_trait]
    impl Actor for TestActorLight {
        type Message = TestMessageLight;
        type Event = TestEventLight;
        type Response = TestResponseLight;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("TestActorLight", id = %id)
        }

        async fn pre_start(
            &mut self,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), ActorError> {
            let memory_db: MemoryManager =
                ctx.system().get_helper("db").await.unwrap();

            let encrypt_key = EncryptedKey::new(&[3u8; 32]).unwrap();

            let db = Store::<Self>::new(
                "store",
                "prefix",
                memory_db,
                Some(encrypt_key),
                Self::create_initial(()),
            )
            .unwrap();

            let store = ctx.create_child("store", db).await.unwrap();
            let response = store.ask(StoreCommand::Recover).await?;

            if let StoreResponse::State(Some(state)) = response {
                self.update(state);
            } else {
                debug!("Create first snapshot");
                store
                    .tell(StoreCommand::Snapshot(self.clone()))
                    .await
                    .unwrap();
            }

            Ok(())
        }
    }

    #[async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;
        type Event = TestEvent;
        type Response = TestResponse;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("TestActor", id = %id)
        }

        async fn pre_start(
            &mut self,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), ActorError> {
            let db = Store::<Self>::new(
                "store",
                "prefix",
                MemoryManager::default(),
                None,
                Self::create_initial(()),
            )
            .unwrap();
            let store = ctx.create_child("store", db).await.unwrap();
            let response = store.ask(StoreCommand::Recover).await.unwrap();
            debug!("Recover response: {:?}", response);
            if let StoreResponse::State(Some(state)) = response {
                debug!("Recovering state: {:?}", state);
                self.update(state);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl PersistentActor for TestActorLight {
        type Persistence = LightPersistence;
        type InitParams = ();

        fn create_initial(_: ()) -> Self {
            Self { data: Vec::new() }
        }

        fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
            self.data.clone_from(&event.0);
            Ok(())
        }
    }

    #[async_trait]
    impl PersistentActor for TestActor {
        type Persistence = FullPersistence;
        type InitParams = ();

        fn create_initial(_: ()) -> Self {
            Self {
                version: 0,
                value: 0,
            }
        }

        fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError> {
            self.version += 1;
            self.value += event.0;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<TestActorLight> for TestActorLight {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: TestMessageLight,
            ctx: &mut ActorContext<TestActorLight>,
        ) -> Result<TestResponseLight, ActorError> {
            match msg {
                TestMessageLight::SetData(data) => {
                    self.on_event(TestEventLight(data), ctx).await;
                    Ok(TestResponseLight::None)
                }
                TestMessageLight::GetData => {
                    Ok(TestResponseLight::Data(self.data.clone()))
                }
            }
        }

        async fn on_event(
            &mut self,
            event: TestEventLight,
            ctx: &mut ActorContext<TestActorLight>,
        ) -> () {
            self.persist(&event, ctx).await.unwrap();
        }
    }

    #[async_trait]
    impl Handler<TestActor> for TestActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: TestMessage,
            ctx: &mut ActorContext<TestActor>,
        ) -> Result<TestResponse, ActorError> {
            match msg {
                TestMessage::Increment(value) => {
                    let event = TestEvent(value);
                    self.on_event(event, ctx).await;
                    Ok(TestResponse::None)
                }
                TestMessage::Recover => {
                    let store: ActorRef<Store<Self>> =
                        ctx.get_child("store").await.unwrap();
                    let response =
                        store.ask(StoreCommand::Recover).await.unwrap();
                    if let StoreResponse::State(Some(state)) = response {
                        self.update(state.clone());
                        Ok(TestResponse::Value(state.value))
                    } else {
                        Ok(TestResponse::None)
                    }
                }
                TestMessage::Snapshot => {
                    let store: ActorRef<Store<Self>> =
                        ctx.get_child("store").await.unwrap();
                    store
                        .ask(StoreCommand::Snapshot(self.clone()))
                        .await
                        .unwrap();
                    Ok(TestResponse::None)
                }
                TestMessage::GetValue => Ok(TestResponse::Value(self.value)),
            }
        }

        async fn on_event(
            &mut self,
            event: TestEvent,
            ctx: &mut ActorContext<TestActor>,
        ) -> () {
            self.persist(&event, ctx).await.unwrap();
        }
    }

    #[test(tokio::test)]
    async fn test_store_actor() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );
        // Init runner.
        tokio::spawn(async move {
            runner.run().await;
        });

        let encrypt_key =
            EncryptedKey::new(b"0123456789abcdef0123456789abcdef").unwrap();
        let db = Store::<TestActor>::new(
            "store",
            "test",
            MemoryManager::default(),
            Some(encrypt_key),
            TestActor::create_initial(()),
        )
        .unwrap();
        let store = system.create_root_actor("store", db).await.unwrap();

        let mut actor = TestActor {
            version: 0,
            value: 0,
        };
        store
            .tell(StoreCommand::Snapshot(actor.clone()))
            .await
            .unwrap();
        store
            .tell(StoreCommand::Persist(TestEvent(10)))
            .await
            .unwrap();
        actor.apply(&TestEvent(10)).unwrap();
        store
            .tell(StoreCommand::Snapshot(actor.clone()))
            .await
            .unwrap();
        store
            .tell(StoreCommand::Persist(TestEvent(10)))
            .await
            .unwrap();

        actor.apply(&TestEvent(10)).unwrap();
        let response = store.ask(StoreCommand::Recover).await.unwrap();
        if let StoreResponse::State(Some(state)) = response {
            assert_eq!(state.value, actor.value);
        }
        let response = store.ask(StoreCommand::Recover).await.unwrap();
        if let StoreResponse::State(Some(state)) = response {
            assert_eq!(state.value, actor.value);
        }
        let response = store.ask(StoreCommand::LastEvent).await.unwrap();
        if let StoreResponse::LastEvent(Some(event)) = response {
            assert_eq!(event.0, 10);
        } else {
            panic!("Event not found");
        }
        let response = store.ask(StoreCommand::LastEventNumber).await.unwrap();
        if let StoreResponse::LastEventNumber(number) = response {
            assert_eq!(number, 2);
        } else {
            panic!("Event number not found");
        }
        let response =
            store.ask(StoreCommand::LastEventsFrom(1)).await.unwrap();
        if let StoreResponse::Events(events) = response {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].0, 10);
        } else {
            panic!("Events not found");
        }
        let response = store
            .ask(StoreCommand::GetEvents { from: 0, to: 1 })
            .await
            .unwrap();
        if let StoreResponse::Events(events) = response {
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].0, 10);
            assert_eq!(events[1].0, 10);
        } else {
            panic!("Events not found");
        }
    }

    #[test(tokio::test)]
    async fn test_persistent_light_actor() {
        let (system, ..) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );

        system.add_helper("db", MemoryManager::default()).await;

        let actor_ref = system
            .create_root_actor("test", TestActorLight::initial(()))
            .await
            .unwrap();

        let result = actor_ref
            .ask(TestMessageLight::SetData(vec![12, 13, 14, 15]))
            .await
            .unwrap();

        assert_eq!(result, TestResponseLight::None);

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        actor_ref.ask_stop().await.unwrap();

        let actor_ref = system
            .create_root_actor("test", TestActorLight::initial(()))
            .await
            .unwrap();

        let result = actor_ref.ask(TestMessageLight::GetData).await.unwrap();

        let TestResponseLight::Data(data) = result else {
            panic!("Invalid response")
        };

        assert_eq!(data, vec![12, 13, 14, 15]);
    }

    #[test(tokio::test)]
    async fn test_persistent_actor() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );
        // Init runner.
        tokio::spawn(async move {
            runner.run().await;
        });

        let actor_ref = system
            .create_root_actor("test", TestActor::initial(()))
            .await
            .unwrap();

        let result = actor_ref.ask(TestMessage::Increment(10)).await.unwrap();

        assert_eq!(result, TestResponse::None);

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        actor_ref.tell(TestMessage::Snapshot).await.unwrap();

        let result = actor_ref.ask(TestMessage::GetValue).await.unwrap();

        assert_eq!(result, TestResponse::Value(10));
        actor_ref.tell(TestMessage::Increment(10)).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        let value = actor_ref.ask(TestMessage::GetValue).await.unwrap();

        assert_eq!(value, TestResponse::Value(20));

        actor_ref.ask(TestMessage::Recover).await.unwrap();

        let value = actor_ref.ask(TestMessage::GetValue).await.unwrap();

        assert_eq!(value, TestResponse::Value(20));

        actor_ref.ask_stop().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    #[test(tokio::test)]
    async fn test_encrypt_decrypt() {
        let encrypt_key = EncryptedKey::new(&[0u8; 32]).unwrap();

        let store = Store::<TestActor>::new(
            "store",
            "test",
            MemoryManager::default(),
            Some(encrypt_key),
            TestActor::create_initial(()),
        )
        .unwrap();
        let data = b"Hello, world!";
        let encrypted = store
            .encrypt(&store.key_box.clone().unwrap(), data)
            .unwrap();
        let decrypted = store
            .decrypt(&store.key_box.clone().unwrap(), &encrypted)
            .unwrap();
        assert_eq!(data, decrypted.as_slice());
    }
}
