//! Event-sourced persistence via [`PersistentActor`].
//!
//! This module provides a Copy-on-Write style persistence trait where actor
//! state is managed as `Arc<State>`, eliminating deep clones on the hot path.

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

use tracing::{debug, error, info_span, warn};

use std::fmt::Debug;
use std::sync::Arc;

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
/// `Light` trades storage space for fast recovery; `Full` trades recovery speed
/// for a smaller storage footprint and a complete audit trail.
#[derive(Debug, Clone)]
pub enum PersistenceType {
    /// Each event is stored together with a state snapshot.
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
    /// Default: `Some(100)`.
    fn snapshot_every() -> Option<u64> {
        Some(100)
    }

    /// Whether already snapshotted events should be compacted after a
    /// successful snapshot.
    ///
    /// Default: `false`.
    fn compact_on_snapshot() -> bool {
        false
    }

    /// Returns the current actor state.
    fn state(&self) -> Arc<Self::State>;

    /// Replaces the current actor state.
    fn set_state(&mut self, state: Arc<Self::State>);

    /// Applies `event` to the in-memory state and durably persists it.
    ///
    /// On failure the in-memory state is rolled back to its pre-call value.
    async fn persist(
        &mut self,
        event: Self::Event,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let store = ctx.get_child::<Store<Self>>("store").await?;

        let prev_state = self.state();
        let new_state = match Self::apply(Arc::clone(&prev_state), &event) {
            Ok(s) => s,
            Err(e) => {
                self.set_state(prev_state);
                return Err(e);
            }
        };

        let response = match Self::Persistence::get_persistence() {
            PersistenceType::Light => {
                let state = Arc::clone(&new_state);
                match store
                    .ask(StoreCommand::PersistLight(Arc::new(event), state))
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        self.set_state(prev_state);
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
                        event: Arc::new(event),
                        state: Arc::clone(&new_state),
                        snapshot_every: Self::snapshot_every(),
                    })
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        self.set_state(prev_state);
                        return Err(actor_store_error(
                            StoreOperation::PersistFull,
                            e,
                        ));
                    }
                }
            }
        };

        match response {
            StoreResponse::Persisted => {
                self.set_state(new_state);
                Ok(())
            }
            _ => {
                self.set_state(prev_state);
                Err(ActorError::UnexpectedResponse {
                    path: ActorPath::from(format!(
                        "{}/store",
                        ctx.path().key()
                    )),
                    expected: "StoreResponse::Persisted".to_owned(),
                })
            }
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
    /// This helper is used internally by [`persist`](PersistentActor::persist)
    /// so that the snapshot reflects the already-applied state without requiring
    /// an in-place mutation of `self`.
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
    /// Call this from [`pre_start`](Actor::pre_start).
    async fn start_store<C: Collection, S: crate::database::State>(
        &mut self,
        name: &str,
        prefix: Option<&str>,
        ctx: &mut ActorContext<Self>,
        manager: impl DbManager<C, S>,
        key_box: Option<EncryptedKey>,
    ) -> Result<(), ActorError> {
        let prefix = prefix.unwrap_or_else(|| ctx.path().key());

        let store =
            Store::<Self>::new(name, prefix, manager, key_box, self.state())
                .map_err(|e| actor_store_error(StoreOperation::StoreInit, e))?;
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
    /// Exclusive upper bound of the event range already compacted from the log.
    compacted_until: u64,
    /// Collection for storing events with sequence numbers as keys.
    events: Box<dyn Collection>,
    /// Storage for the latest state snapshot.
    states: Box<dyn State>,
    /// Storage for log metadata used to resume after snapshots/compaction.
    metadata: Box<dyn State>,
    /// Encrypted password for data encryption (XChaCha20-Poly1305).
    key_box: Option<EncryptedKey>,
    /// Initial state to use when recovering without a snapshot.
    initial_state: Arc<A::State>,
}

impl<A> ave_actors_actor::NotPersistentActor for Store<A>
where
    A: PersistentActor,
    A::Event: BorshSerialize + BorshDeserialize,
{
}

/// Legacy metadata format (v1) without `state_counter`.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct StoreMetadataV1 {
    next_event_index: u64,
    compacted_until: u64,
}

/// Current metadata format (v2) including `state_counter`.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
struct StoreMetadata {
    next_event_index: u64,
    compacted_until: u64,
    state_counter: u64,
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

        let last_event_counter = store
            .events
            .last()?
            .map(|(key, _)| {
                key.parse::<u64>()
                    .map_err(|e| store_error(StoreOperation::ParseEventKey, e))
                    .map(|n| n + 1)
            })
            .transpose()?
            .unwrap_or(0);

        let snapshot_counter = store.get_state()?.map(|(_, c)| c).unwrap_or(0);

        if let Some(metadata) = store.get_metadata()? {
            store.event_counter =
                last_event_counter.max(metadata.next_event_index);
            store.compacted_until = metadata.compacted_until;
            store.state_counter = metadata.state_counter;
        } else {
            store.event_counter = last_event_counter.max(snapshot_counter);
            store.state_counter = snapshot_counter;
        }

        debug!(
            "Initializing Store with event_counter: {}, \
             compacted_until: {}",
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

        let bytes = self.maybe_decrypt(data)?;

        // Try current format (v2) first.
        if let Ok(metadata) = borsh::from_slice::<StoreMetadata>(&bytes) {
            return Ok(Some(metadata));
        }

        // Fall back to legacy format (v1) and migrate on-the-fly.
        if let Ok(v1) = borsh::from_slice::<StoreMetadataV1>(&bytes) {
            return Ok(Some(StoreMetadata {
                next_event_index: v1.next_event_index,
                compacted_until: v1.compacted_until,
                state_counter: 0,
            }));
        }

        error!("Can't decode metadata: incompatible format");
        Err(store_error(
            StoreOperation::DecodeState,
            "Metadata format is incompatible",
        ))
    }

    fn persist_metadata(&mut self) -> Result<(), Error> {
        let metadata = StoreMetadata {
            next_event_index: self.event_counter,
            compacted_until: self.compacted_until,
            state_counter: self.state_counter,
        };
        let data = borsh::to_vec(&metadata).map_err(|e| {
            error!("Can't encode metadata: {}", e);
            store_error(StoreOperation::EncodeActor, e)
        })?;

        let bytes = self.maybe_encrypt(&data)?;

        self.metadata.put(&bytes)
    }

    fn compact_to_snapshot(&mut self) -> Result<(), Error> {
        if self.compacted_until >= self.state_counter {
            return Ok(());
        }

        let start_key = format!("{:020}", self.compacted_until);
        let end_key = format!("{:020}", self.state_counter.saturating_sub(1));

        match self.events.del_range(&start_key, &end_key) {
            Ok(()) => {
                self.compacted_until = self.state_counter;
            }
            Err(Error::EntryNotFound { .. }) => {
                self.compacted_until = self.state_counter;
            }
            Err(err) => return Err(err),
        }

        Ok(())
    }

    fn persist<E>(&mut self, event: &E) -> Result<(), Error>
    where
        E: Event + BorshSerialize + BorshDeserialize,
    {
        debug!("Persisting event: {:?}", event);

        let bytes = borsh::to_vec(event).map_err(|e| {
            error!("Can't encode event: {}", e);
            store_error(StoreOperation::EncodeEvent, e)
        })?;

        let bytes = self.maybe_encrypt(&bytes)?;

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
            self.event_counter += 1;
            debug!(
                "Successfully persisted event, event_counter now: {}",
                self.event_counter
            );
        }

        result
    }

    fn persist_state<E>(
        &mut self,
        event: &E,
        state: &Arc<A::State>,
    ) -> Result<(), Error>
    where
        E: Event + BorshSerialize + BorshDeserialize,
    {
        debug!("Persisting event: {:?}", event);

        let bytes = borsh::to_vec(event).map_err(|e| {
            error!("Can't encode event: {}", e);
            store_error(StoreOperation::EncodeEvent, e)
        })?;

        let bytes = self.maybe_encrypt(&bytes)?;

        let next_event_number = self.event_counter;

        debug!(
            "Persisting event {} at index {} with LightPersistence",
            std::any::type_name::<E>(),
            next_event_number
        );

        let event_key = format!("{:020}", next_event_number);
        self.events.put(&event_key, &bytes).map_err(|e| {
            error!(key = %event_key, error = %e, "Failed to persist event");
            store_error(StoreOperation::PersistLight, e)
        })?;

        self.event_counter += 1;
        debug!(
            "Successfully persisted event, event_counter now: {}",
            self.event_counter
        );

        if let Err(e) = self.snapshot(state) {
            error!(error = %e, "Snapshot failed after event persistence");
            return Err(store_error(StoreOperation::Snapshot, e));
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

    fn get_state(&self) -> Result<Option<(Arc<A::State>, u64)>, Error> {
        let data = match self.states.get() {
            Ok(data) => data,
            Err(Error::EntryNotFound { .. }) => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        let bytes = self.maybe_decrypt(data)?;

        let state: (A::State, u64) =
            borsh::from_slice(&bytes).map_err(|e| {
                error!("Can't decode state: {}", e);
                store_error(StoreOperation::DecodeState, e)
            })?;

        Ok(Some((Arc::new(state.0), state.1)))
    }

    fn events(&self, from: u64, to: u64) -> Result<Vec<A::Event>, Error> {
        if from > to {
            return Ok(Vec::new());
        }

        let from_key = format!("{:020}", from);
        let to_key = format!("{:020}", to);
        let expected = (to - from + 1) as usize;
        let mut events = Vec::with_capacity(expected);

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
        if from > to || from >= self.event_counter {
            return Ok(Vec::new());
        }

        let upper = to.min(self.event_counter.saturating_sub(1));
        self.events(from, upper)
    }

    fn snapshot(&mut self, state: &Arc<A::State>) -> Result<(), Error> {
        debug!("Snapshotting state");

        let next_state_counter = self.event_counter;

        let data = borsh::to_vec(&(state.as_ref(), next_state_counter))
            .map_err(|e| {
                error!("Can't encode state: {}", e);
                store_error(StoreOperation::EncodeActor, e)
            })?;

        let bytes = self.maybe_encrypt(&data)?;

        self.states.put(&bytes)?;
        self.state_counter = next_state_counter;
        self.persist_metadata()?;
        if A::compact_on_snapshot() {
            if let Err(err) = self.compact_to_snapshot() {
                warn!(
                    error = %err,
                    "Snapshot persisted but event compaction failed; \
                     keeping event log"
                );
            } else if let Err(err) = self.persist_metadata() {
                warn!(
                    error = %err,
                    "Snapshot metadata persisted but compaction \
                     watermark update failed"
                );
            }
        }
        Ok(())
    }

    fn recover(&mut self) -> Result<Option<Arc<A::State>>, Error> {
        debug!("Starting recovery process");

        if let Some((state, counter)) = self.get_state()? {
            return self.recover_from_snapshot(state, counter);
        }

        debug!("No previous state found");

        if let Some((key, ..)) = self.events.last()? {
            return self.recover_from_initial_events(&key);
        }

        debug!("No previous state and no events found, starting fresh");
        Ok(None)
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
                    .map(|n| n + 1)
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
            let events =
                self.events(self.state_counter, self.event_counter - 1)?;
            debug!("Found {} events to replay", events.len());

            for (i, event) in events.iter().enumerate() {
                debug!("Applying event {} of {}", i + 1, events.len());
                state = A::apply(Arc::clone(&state), event).map_err(|e| {
                    store_error_with_source(
                        StoreOperation::ApplyEvent,
                        format!("{:?}", e),
                        e,
                    )
                })?;
            }

            debug!("Updating snapshot after applying {} events", events.len());
            if let Err(e) = self.snapshot(&state) {
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
            + 1;
        self.state_counter = 0;

        debug!(
            "Using provided initial state and applying {} events",
            self.event_counter
        );

        let mut state = Arc::clone(&self.initial_state);

        let events = self.events(0, self.event_counter - 1)?;
        debug!("Replaying {} events from scratch", events.len());

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

        debug!("Creating snapshot after replaying events");
        if let Err(e) = self.snapshot(&state) {
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
            .map(|(s, _)| s)
            .unwrap_or_else(|| Arc::clone(&self.initial_state));

        let events = self.events(self.state_counter, self.event_counter - 1)?;
        for event in &events {
            state = A::apply(state, event).map_err(|e| {
                store_error(
                    StoreOperation::ApplyEventOnStop,
                    format!("{:?}", e),
                )
            })?;
        }

        self.snapshot(&state)
    }

    /// Deletes all events, snapshots, and metadata, then resets all counters
    /// to zero.
    pub fn purge(&mut self) -> Result<(), Error> {
        self.events.purge()?;
        self.states.purge()?;
        self.metadata.purge()?;
        self.event_counter = 0;
        self.state_counter = 0;
        self.compacted_until = 0;
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

        let cipher = XChaCha20Poly1305::new(key.as_ref().into());
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
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

        let nonce = XNonce::from_slice(&ciphertext[..NONCE_SIZE]);
        let ciphertext_data = &ciphertext[NONCE_SIZE..];

        let cipher = XChaCha20Poly1305::new(key.as_ref().into());
        let plaintext =
            cipher.decrypt(nonce, ciphertext_data).map_err(|e| {
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
    /// Persist an event without forcing a snapshot.
    Persist(Arc<A::Event>),
    /// Persist an event and snapshot the supplied state if required.
    PersistFullEvent {
        /// Event to append to the event log.
        event: Arc<A::Event>,
        /// Current actor state, used when a snapshot is triggered.
        state: Arc<A::State>,
        /// Snapshot cadence for `FullPersistence`.
        snapshot_every: Option<u64>,
    },
    /// Persist an event and snapshot the supplied state if required.
    PersistFull {
        /// Event to append to the event log.
        event: Arc<A::Event>,
        /// Current actor state, used when a snapshot is triggered.
        state: Arc<A::State>,
        /// Snapshot cadence for `FullPersistence`.
        snapshot_every: Option<u64>,
    },
    /// Persist an event together with a snapshot of the supplied state.
    PersistLight(Arc<A::Event>, Arc<A::State>),
    /// Persist a batch of events atomically.
    PersistBatch(Vec<Arc<A::Event>>),
    /// Snapshot the supplied state immediately.
    Snapshot(Arc<A::State>),
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

impl<A: PersistentActor> Clone for StoreCommand<A>
where
    A::Event: BorshSerialize + BorshDeserialize,
{
    fn clone(&self) -> Self {
        match self {
            Self::Persist(e) => Self::Persist(Arc::clone(e)),
            Self::PersistFullEvent {
                event,
                state,
                snapshot_every,
            } => Self::PersistFullEvent {
                event: Arc::clone(event),
                state: Arc::clone(state),
                snapshot_every: *snapshot_every,
            },
            Self::PersistFull {
                event,
                state,
                snapshot_every,
            } => Self::PersistFull {
                event: Arc::clone(event),
                state: Arc::clone(state),
                snapshot_every: *snapshot_every,
            },
            Self::PersistLight(e, s) => {
                Self::PersistLight(Arc::clone(e), Arc::clone(s))
            }
            Self::PersistBatch(events) => {
                Self::PersistBatch(events.iter().map(Arc::clone).collect())
            }
            Self::Snapshot(s) => Self::Snapshot(Arc::clone(s)),
            Self::Compact => Self::Compact,
            Self::LastEvent => Self::LastEvent,
            Self::LastEventNumber => Self::LastEventNumber,
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
    /// Event compaction completed successfully.
    Compacted,
    /// Recovered actor state, or `None` when no persisted state exists.
    State(Option<Arc<A::State>>),
    /// Most recently persisted event, or `None` when the log is empty.
    LastEvent(Option<A::Event>),
    /// Next free event index.
    LastEventNumber(u64),
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

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("Store", id = %id)
    }

    async fn pre_stop(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        if let Err(e) = self.snapshot_if_needed() {
            error!(
                error = %e,
                "Failed to snapshot state during Store shutdown"
            );
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
            StoreCommand::Persist(event) => {
                self.persist(event.as_ref()).map_err(|e| {
                    actor_store_error(StoreOperation::Persist, e)
                })?;
                debug!("Persisted event: {:?}", event);
                Ok(StoreResponse::Persisted)
            }
            StoreCommand::PersistFullEvent {
                event,
                state,
                snapshot_every,
            } => {
                self.persist(event.as_ref()).map_err(|e| {
                    actor_store_error(StoreOperation::PersistFull, e)
                })?;

                if snapshot_every.is_some_and(|every| {
                    self.pending_events_since_snapshot() >= every
                }) {
                    self.snapshot(&state).map_err(|e| {
                        actor_store_error(StoreOperation::Snapshot, e)
                    })?;
                }

                debug!("Persisted full event: {:?}", event);
                Ok(StoreResponse::Persisted)
            }
            StoreCommand::PersistFull {
                event,
                state,
                snapshot_every,
            } => {
                self.persist(event.as_ref()).map_err(|e| {
                    actor_store_error(StoreOperation::PersistFull, e)
                })?;

                if snapshot_every.is_some_and(|every| {
                    self.pending_events_since_snapshot() >= every
                }) {
                    self.snapshot(&state).map_err(|e| {
                        actor_store_error(StoreOperation::Snapshot, e)
                    })?;
                }

                debug!("Persisted full event: {:?}", event);
                Ok(StoreResponse::Persisted)
            }
            StoreCommand::PersistLight(event, state) => {
                self.persist_state(event.as_ref(), &state).map_err(|e| {
                    actor_store_error(StoreOperation::PersistLight, e)
                })?;
                debug!("Light persistence of event: {:?}", event);
                Ok(StoreResponse::Persisted)
            }
            StoreCommand::PersistBatch(events) => {
                for event in &events {
                    self.persist(event.as_ref()).map_err(|e| {
                        actor_store_error(StoreOperation::PersistBatch, e)
                    })?;
                }
                debug!("Persisted batch of {} events", events.len());
                Ok(StoreResponse::Persisted)
            }
            StoreCommand::Snapshot(state) => {
                self.snapshot(&state).map_err(|e| {
                    actor_store_error(StoreOperation::Snapshot, e)
                })?;
                debug!("Snapshotted state");
                Ok(StoreResponse::Snapshotted)
            }
            StoreCommand::Compact => {
                self.compact_to_snapshot().map_err(|e| {
                    actor_store_error(StoreOperation::Compact, e)
                })?;
                debug!("Compacted events covered by the latest snapshot");
                Ok(StoreResponse::Compacted)
            }
            StoreCommand::Recover => {
                let state = self.recover().map_err(|e| {
                    actor_store_error(StoreOperation::Recover, e)
                })?;
                debug!("Recovered state");
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
            StoreCommand::LastEvent => {
                let event = self.last_event().map_err(|e| {
                    actor_store_error(StoreOperation::LastEvent, e)
                })?;
                debug!("Last event: {:?}", event);
                Ok(StoreResponse::LastEvent(event))
            }
            StoreCommand::Purge => {
                self.purge()
                    .map_err(|e| actor_store_error(StoreOperation::Purge, e))?;
                debug!("Purged store");
                Ok(StoreResponse::None)
            }
            StoreCommand::LastEventNumber => {
                Ok(StoreResponse::LastEventNumber(self.event_counter))
            }
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryManager;
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
        type Response = CounterResponse;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("CounterActor", id = %id)
        }

        async fn pre_start(
            &mut self,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), ActorError> {
            let db: MemoryManager =
                ctx.system().get_helper("db").await.unwrap();
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
    impl Handler<CounterActor> for CounterActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: CounterMessage,
            ctx: &mut ActorContext<CounterActor>,
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
        type Response = CounterResponse;

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
            let db: MemoryManager =
                ctx.system().get_helper("db").await.unwrap();
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
    impl Handler<FullCounterActor> for FullCounterActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: CounterMessage,
            ctx: &mut ActorContext<FullCounterActor>,
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

        system.add_helper("db", MemoryManager::default()).await;

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

        system.add_helper("db", MemoryManager::default()).await;

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
        let mut store = Store::<CounterActor>::new(
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
        let mut store = Store::<CounterActor>::new(
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
        let store = Store::<CounterActor>::new(
            "store",
            "test",
            MemoryManager::default(),
            None,
            initial,
        )
        .unwrap();

        let store_ref = system.create_root_actor("store", store).await.unwrap();

        store_ref
            .tell(StoreCommand::Persist(Arc::new(CounterEvent(5))))
            .await
            .unwrap();
        store_ref
            .tell(StoreCommand::Snapshot(Arc::new(CounterState { value: 5 })))
            .await
            .unwrap();
        store_ref
            .tell(StoreCommand::Persist(Arc::new(CounterEvent(3))))
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
}
