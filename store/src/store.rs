//! # Store module.
//!
//! This module contains the store implementation.
//!
//! The `Store` actor is an actor that offers the ability to persist actors from events that modify
//! their state (applying the event sourcing pattern). It also allows you to store snapshots
//! of an actor. The `PersistentActor` trait is an extension of the `Actor` trait that must be
//! implemented by actors who need to persist.
//!

use crate::{
    database::{Collection, DbManager, State},
    error::Error,
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

use std::{fmt::Debug};

/// Nonce size for XChaCha20-Poly1305 encryption.
const NONCE_SIZE: usize = 24;

/// Defines the persistence strategy for an actor.
/// This determines how events and state are stored.
#[derive(Debug, Clone)]
pub enum PersistenceType {
    /// Light persistence: Stores each event along with the current state snapshot.
    /// Faster recovery but uses more storage space.
    Light,
    /// Full persistence: Stores only events, state is reconstructed by replaying them.
    /// Uses less storage but slower recovery due to event replay.
    Full,
}

/// Marker type for light persistence strategy.
/// Use this when you want fast recovery and don't mind larger storage footprint.
pub struct LightPersistence;

/// Marker type for full event sourcing strategy.
/// Use this for complete audit trail and smaller storage footprint.
pub struct FullPersistence;

/// Trait for defining persistence strategy at the type level.
/// Implement this on your persistence marker types.
pub trait Persistence {
    /// Returns the persistence type for this strategy.
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

/// Wrapper type that guarantees a persistent actor was created via `PersistentActor::initial()`.
///
/// This type cannot be constructed directly by users - it can only be obtained
/// by calling `PersistentActor::initial()`. This ensures that all persistent actors
/// are initialized correctly through their defined initialization logic.
///
/// The actor system requires `InitializedActor<A>` for persistent actors,
/// preventing users from manually constructing instances that bypass the
/// initialization process.
#[derive(Debug)]
pub struct InitializedActor<A>(A);

impl<A> InitializedActor<A> {
    /// Creates a new InitializedActor wrapper.
    /// This is `pub(crate)` so only PersistentActor::initial() can call it.
    pub(crate) fn new(actor: A) -> Self {
        Self(actor)
    }
}

/// Implement `IntoActor` for `InitializedActor` to allow it to be used
/// with the actor system's create methods.
impl<A> IntoActor<A> for InitializedActor<A>
where
    A: PersistentActor,
    A::Event: BorshSerialize + BorshDeserialize,
{
    fn into_actor(self) -> A {
        self.0
    }
}

/// Trait for actors that persist their state using event sourcing.
/// PersistentActor extends the Actor trait with methods for persisting events,
/// snapshotting state, and recovering from storage.
///
/// # Event Sourcing Pattern
///
/// This trait implements the event sourcing pattern where:
/// 1. Events represent state changes and are persisted immutably.
/// 2. Actor state is derived by applying events in sequence.
/// 3. Snapshots can be taken to speed up recovery.
///
/// # Type Requirements
///
/// Implementing types must be:
/// - Cloneable (for state snapshots)
/// - Serializable (for persistence)
/// - Debuggable (for logging)
///
/// # Usage
///
/// Implementing actors should define how events modify state in the apply()
/// method, and choose either Light or Full persistence strategy based on
/// recovery speed vs storage requirements tradeoffs.
///
/// # Important
///
/// Do NOT implement both `PersistentActor` and `NotPersistentActor` on the same type.
/// This is enforced by convention but not by the type system.
///
#[async_trait]
pub trait PersistentActor:
    Actor + Handler<Self> + Debug + Clone + BorshSerialize + BorshDeserialize
where
    Self::Event: BorshSerialize + BorshDeserialize,
{
    /// The persistence strategy type (Light or Full).
    type Persistence: Persistence;

    /// Parameters needed to initialize this actor.
    /// Use `()` if no parameters are needed.
    type InitParams;

    /// Creates the initial state for this actor with the given parameters.
    ///
    /// # Arguments
    ///
    /// * `params` - Initialization parameters (can be `()` if none needed)
    ///
    /// # Returns
    ///
    /// A new instance of the actor in its initial state.
    fn create_initial(params: Self::InitParams) -> Self;

    /// Returns an `InitializedActor` wrapper containing the initial state.
    /// This is the ONLY way to create a persistent actor instance that the
    /// actor system will accept.
    ///
    /// # Arguments
    ///
    /// * `params` - Initialization parameters
    ///
    /// # Returns
    ///
    /// An `InitializedActor<Self>` that can be passed to the actor system.
    fn initial(params: Self::InitParams) -> InitializedActor<Self>
    where
        Self: Sized,
    {
        InitializedActor::new(Self::create_initial(params))
    }

    /// Applies an event to the actor's state.
    /// This method should be deterministic - applying the same event
    /// to the same state should always produce the same result.
    ///
    /// # Arguments
    ///
    /// * `event` - The event to apply to the actor's state.
    ///
    /// # Returns
    ///
    /// Ok(()) if the event was applied successfully.
    ///
    /// # Errors
    ///
    /// Returns an error if the event cannot be applied (e.g., invalid state transition).
    ///
    /// # Important
    ///
    /// This method should NOT persist the event - that's handled by the `persist()` method.
    /// This only updates the in-memory state.
    ///
    fn apply(&mut self, event: &Self::Event) -> Result<(), ActorError>;

    /// Updates the actor's state by replacing it with a recovered state.
    /// This is called during recovery to restore the actor from a snapshot.
    ///
    /// # Arguments
    ///
    /// * `state` - The recovered state to restore.
    ///
    /// # Default Behavior
    ///
    /// The default implementation simply replaces the current state.
    /// Override this if you need custom recovery logic.
    ///
    fn update(&mut self, state: Self) {
        *self = state;
    }

    /// Persists an event by applying it to state and storing it in the database.
    /// This is the main method for persisting state changes in event sourcing.
    ///
    /// # Process
    ///
    /// 1. Retrieves the Store child actor
    /// 2. Creates a backup of the current state
    /// 3. Applies the event to the state
    /// 4. Persists the event (and possibly state) based on persistence type
    /// 5. Rolls back state if persistence fails
    ///
    /// # Arguments
    ///
    /// * `event` - The event to persist.
    /// * `ctx` - The actor context (used to access the store child actor).
    ///
    /// # Returns
    ///
    /// Ok(()) if the event was applied and persisted successfully.
    ///
    /// # Errors
    ///
    /// Returns ActorError::Store if:
    /// - The store actor cannot be accessed
    /// - The event application fails
    /// - The persistence operation fails
    ///
    /// # Persistence Behavior
    ///
    /// - **Light**: Persists both the event and current state snapshot
    /// - **Full**: Persists only the event
    ///
    async fn persist(
        &mut self,
        event: &Self::Event,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let store = ctx.get_child::<Store<Self>>("store").await?;

        let prev_state = self.clone();

        if let Err(e) = self.apply(event) {
            self.update(prev_state);
            return Err(e);
        }

        let response = match Self::Persistence::get_persistence() {
            PersistenceType::Light => store
                .ask(StoreCommand::PersistLight(event.clone(), self.clone()))
                .await
                .map_err(|e| ActorError::StoreOperation {
                    operation: "persist ligth".to_owned(),
                    reason: e.to_string(),
                })?,
            PersistenceType::Full => store
                .ask(StoreCommand::Persist(event.clone()))
                .await
                .map_err(|e| ActorError::StoreOperation {
                    operation: "persist".to_owned(),
                    reason: e.to_string(),
                })?,
        };

        match response {
            StoreResponse::Persisted => Ok(()),
            StoreResponse::Error(error) => Err(ActorError::StoreOperation {
                operation: "response".to_owned(),
                reason: error.to_string(),
            }),
            _ => Err(ActorError::UnexpectedResponse {
                path: ActorPath::from(format!("{}/store", ctx.path().clone())),
                expected: "StoreResponse::Persisted".to_owned(),
            }),
        }
    }

    /// Snapshot the state.
    ///
    /// # Arguments
    ///
    /// - store: The store actor.
    ///
    /// # Returns
    ///
    /// Void.
    ///
    /// # Errors
    ///
    /// An error if the operation failed.
    ///
    async fn snapshot(
        &self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        let store = ctx.get_child::<Store<Self>>("store").await?;
        store
            .ask(StoreCommand::Snapshot(self.clone()))
            .await
            .map_err(|e| ActorError::StoreOperation {
                operation: "snapshot".to_owned(),
                reason: e.to_string(),
            })?;
        Ok(())
    }

    /// Start the child store and recover the state (if any).
    ///
    /// If a persisted state exists, it will be recovered and applied to the actor.
    /// If no state exists, the actor continues with its current state (which should
    /// be initialized using `ActorState::initial()`).
    ///
    /// No initial snapshot is created for new actors without events.
    /// Snapshots are only created when:
    /// - Events are persisted (with Light persistence, automatically; with Full, on shutdown if events exist)
    /// - Manually calling `snapshot()`
    /// - During recovery if events need to be replayed
    ///
    /// # Arguments
    ///
    /// - ctx: The actor context.
    /// - name: Actor type.
    /// - manager: The database manager.
    /// - password: Optional password.
    ///
    /// # Returns
    ///
    /// Void.
    ///
    /// # Errors
    ///
    /// An error if the operation failed.
    ///
    async fn start_store<C: Collection, S: State>(
        &mut self,
        name: &str,
        prefix: Option<String>,
        ctx: &mut ActorContext<Self>,
        manager: impl DbManager<C, S>,
        key_box: Option<EncryptedKey>,
    ) -> Result<(), ActorError> {
        let prefix = match prefix {
            Some(prefix) => prefix,
            None => ctx.path().key(),
        };

        let store =
            Store::<Self>::new(name, &prefix, manager, key_box, self.clone())
                .map_err(|e| ActorError::StoreOperation {
                operation: "store_operation".to_owned(),
                reason: e.to_string(),
            })?;
        let store = ctx.create_child("store", store).await?;
        let response = store.ask(StoreCommand::Recover).await?;

        if let StoreResponse::State(Some(state)) = response {
            self.update(state);
        }
        Ok(())
    }

    /// Stop the child store and snapshot the state.
    ///
    /// For Full persistence mode, a snapshot is only created if there are
    /// persisted events. Actors without events won't create a snapshot.
    ///
    /// # Arguments
    ///
    /// - ctx: The actor context.
    ///
    /// # Returns
    ///
    /// Void.
    ///
    /// # Errors
    ///
    /// An error if the operation failed.
    ///
    async fn stop_store(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), ActorError> {
        if let Ok(store) = ctx.get_child::<Store<Self>>("store").await {
            if let PersistenceType::Full = Self::Persistence::get_persistence()
            {
                // Only snapshot if there are events
                let response = store.ask(StoreCommand::LastEventNumber).await?;
                if let StoreResponse::LastEventNumber(count) = response
                    && count > 0
                {
                    let _ =
                        store.ask(StoreCommand::Snapshot(self.clone())).await?;
                }
            }

            store.ask_stop().await
        } else {
            Err(ActorError::StoreOperation {
                operation: "get_store".to_owned(),
                reason: "Can't get store".to_string(),
            })
        }
    }
}

/// Store actor that manages persistent storage for a PersistentActor.
/// The Store handles event persistence, state snapshots, and recovery.
/// It operates as a child actor of the persistent actor it serves.
///
/// # Storage Model
///
/// - **Events**: Stored in a Collection with sequence numbers as keys
/// - **Snapshots**: Stored in State storage (single value, updated on each snapshot)
/// - **Encryption**: Optional ChaCha20-Poly1305 encryption for at-rest data
///
/// # Type Parameters
///
/// * `P` - The PersistentActor type this store manages
///
pub struct Store<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
    /// Current event sequence number (auto-incrementing).
    event_counter: u64,
    /// Current state version number (for snapshots).
    state_counter: u64,
    /// Collection for storing events with sequence numbers as keys.
    events: Box<dyn Collection>,
    /// Storage for the latest state snapshot.
    states: Box<dyn State>,
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

impl<P> Store<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
    /// Creates a new store actor.
    ///
    /// # Arguments
    ///
    /// - name: The name of the actor.
    /// - prefix: The prefix for the database keys.
    /// - manager: The database manager.
    /// - key_box: Optional encryption key.
    /// - initial_state: The initial state to use when recovering without a snapshot.
    ///
    /// # Returns
    ///
    /// The persistent actor.
    ///
    /// # Errors
    ///
    /// An error if it fails to create the collections.
    ///
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

        // Initialize event_counter from the last event in the database
        // event_counter works like vector.len(): last position + 1
        let initial_event_counter = if let Some((key, _)) = events.last() {
            key.parse::<u64>().unwrap_or(0) + 1
        } else {
            0
        };

        debug!(
            "Initializing Store with event_counter: {}",
            initial_event_counter
        );

        Ok(Self {
            event_counter: initial_event_counter,
            state_counter: 0,
            events: Box::new(events),
            states: Box::new(states),
            key_box,
            initial_state,
        })
    }

    /// Persist an event.
    ///
    /// # Arguments
    ///
    /// - event: The event to persist.
    ///
    /// # Returns
    ///
    /// An error if the operation failed.
    ///
    fn persist<E>(&mut self, event: &E) -> Result<(), Error>
    where
        E: Event + BorshSerialize + BorshDeserialize,
    {
        debug!("Persisting event: {:?}", event);

        let bytes = borsh::to_vec(event).map_err(|e| {
            error!("Can't encode event: {}", e);
            Error::Store {
                operation: "encode_event".to_owned(),
                reason: format!("{}", e),
            }
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

    /// Persist an event and the state.
    /// This method is used to persist an event and the state of the actor in a single operation.
    /// This applies in scenarios where we want to keep only the last event and state.
    ///
    /// # Arguments
    ///
    /// - event: The event to persist.
    /// - state: The state of the actor (without applying the event).
    ///
    /// # Returns
    ///
    /// An error if the operation failed.
    ///
    fn persist_state<E>(&mut self, event: &E, state: &P) -> Result<(), Error>
    where
        E: Event + BorshSerialize + BorshDeserialize,
    {
        debug!("Persisting event: {:?}", event);

        let bytes = borsh::to_vec(event).map_err(|e| {
            error!("Can't encode event: {}", e);
            Error::Store {
                operation: "encode_event".to_owned(),
                reason: format!("{}", e),
            }
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
        let result = self
            .events
            .put(&format!("{:020}", next_event_number), &bytes);

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
        self.snapshot(state)?;

        Ok(())
    }

    /// Returns the last event.
    ///
    /// # Returns
    ///
    /// The last event.
    ///
    /// An error if the operation failed.
    ///
    fn last_event(&self) -> Result<Option<P::Event>, Error> {
        if let Some((_, data)) = self.events.last() {
            let data = if let Some(key_box) = &self.key_box {
                self.decrypt(key_box, data.as_slice())?
            } else {
                data
            };

            let event: P::Event = borsh::from_slice(&data).map_err(|e| {
                error!("Can't decode event: {}", e);
                Error::Store {
                    operation: "decode_event".to_owned(),
                    reason: format!("{}", e),
                }
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
            Error::Store {
                operation: "decode_state".to_owned(),
                reason: format!("{}", e),
            }
        })?;

        Ok(Some(state))
    }

    /// Retrieve events.
    fn events(&mut self, from: u64, to: u64) -> Result<Vec<P::Event>, Error> {
        let mut events = Vec::new();

        for i in from..=to {
            if let Ok(data) = self.events.get(&format!("{:020}", i)) {
                let data = if let Some(key_box) = &self.key_box {
                    self.decrypt(key_box, data.as_slice())?
                } else {
                    data
                };

                let event: P::Event =
                    borsh::from_slice(&data).map_err(|e| {
                        error!("Can't decode event: {}", e);
                        Error::Store {
                            operation: "decode_event".to_owned(),
                            reason: format!("{}", e),
                        }
                    })?;

                events.push(event);
            } else {
                break;
            }
        }
        Ok(events)
    }

    /// Snapshot the state.
    ///
    /// # Arguments
    ///
    /// - actor: The actor to snapshot.
    ///
    /// # Returns
    ///
    /// An error if the operation failed.
    ///
    fn snapshot(&mut self, actor: &P) -> Result<(), Error> {
        debug!("Snapshotting state: {:?}", actor);

        self.state_counter = self.event_counter;

        let data =
            borsh::to_vec(&(actor, self.state_counter)).map_err(|e| {
                error!("Can't encode actor: {}", e);
                Error::Store {
                    operation: "encode_actor".to_owned(),
                    reason: format!("{}", e),
                }
            })?;

        let bytes = if let Some(key_box) = &self.key_box {
            self.encrypt(key_box, data.as_slice())?
        } else {
            data
        };

        self.states.put(&bytes)
    }

    /// Recover the state.
    ///
    /// # Returns
    ///
    /// The recovered state.
    ///
    /// An error if the operation failed.
    ///
    fn recover(&mut self) -> Result<Option<P>, Error> {
        debug!("Starting recovery process");

        if let Some((mut state, counter)) = self.get_state()? {
            self.state_counter = counter;
            debug!("Recovered state with counter: {}", counter);

            if let Some((key, ..)) = self.events.last() {
                self.event_counter =
                    key.parse::<u64>().map_err(|e| Error::Store {
                        operation: "parse_event_key".to_owned(),
                        reason: format!("{}", e),
                    })? + 1;

                debug!(
                    "Recovery state: event_counter={}, state_counter={}",
                    self.event_counter, self.state_counter
                );

                if self.event_counter != self.state_counter {
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
                    let events = self
                        .events(self.state_counter, self.event_counter - 1)?;
                    debug!("Found {} events to replay", events.len());

                    for (i, event) in events.iter().enumerate() {
                        debug!("Applying event {} of {}", i + 1, events.len());
                        state.apply(event).map_err(|e| Error::Store {
                            operation: "apply_event".to_owned(),
                            reason: e.to_string(),
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
                    // from the last persisted event key
                } else {
                    debug!("State is up to date, no events to apply");
                }

                Ok(Some(state))
            } else {
                debug!(
                    "No events found in database, using recovered state as-is"
                );
                Ok(Some(state))
            }
        } else {
            debug!("No previous state found");

            // Check if there are any events in the database
            if let Some((key, ..)) = self.events.last() {
                debug!(
                    "No snapshot but events found - replaying from beginning"
                );

                self.event_counter =
                    key.parse::<u64>().map_err(|e| Error::Store {
                        operation: "parse_event_key".to_owned(),
                        reason: format!("{}", e),
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
                    state.apply(event).map_err(|e| Error::Store {
                        operation: "apply_event".to_owned(),
                        reason: e.to_string(),
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

    /// Purge the store.
    ///
    /// # Returns
    ///
    /// An error if the operation failed.
    ///
    pub fn purge(&mut self) -> Result<(), Error> {
        self.events.purge()?;
        self.states.purge()?;
        self.event_counter = 0;
        self.state_counter = 0;
        Ok(())
    }

    /// Encrypt bytes using XChaCha20-Poly1305 AEAD.
    ///
    /// # Security considerations
    ///
    /// - Uses XChaCha20-Poly1305 with 192-bit random nonce (superior to ChaCha20)
    /// - Cryptographically secure random nonce via OsRng (no collision risk)
    /// - Key must be exactly 32 bytes (256 bits)
    /// - Each encryption produces a unique ciphertext due to random nonce
    /// - The nonce is prepended to the ciphertext for later decryption
    /// - Provides both confidentiality (XChaCha20) and authenticity (Poly1305 MAC)
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte encryption key wrapped in Zeroizing for secure memory handling
    /// * `bytes` - Plaintext data to encrypt
    ///
    /// # Returns
    ///
    /// Encrypted data with format: [nonce (24 bytes) || ciphertext || auth_tag (16 bytes)]
    ///
    /// # Errors
    ///
    /// Returns Error::Store if:
    /// - Key length is not exactly 32 bytes
    /// - Encryption operation fails
    ///
    fn encrypt(
        &self,
        key_box: &EncryptedKey,
        bytes: &[u8],
    ) -> Result<Vec<u8>, Error> {
        if let Ok(key) = key_box.key() {
            // Validate key size (XChaCha20-Poly1305 requires exactly 32 bytes)
            if key.len() != 32 {
                error!(expected = 32, got = key.len(), "Invalid encryption key length");
                return Err(Error::Store {
                    operation: "validate_key_length".to_owned(),
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
            let ciphertext: Vec<u8> = cipher
                .encrypt(&nonce, bytes.as_ref())
                .map_err(|e| {
                    error!(error = %e, "Encryption failed");
                    Error::Store {
                        operation: "encrypt_data".to_owned(),
                        reason: format!("{}", e),
                    }
                })?;

            // Prepend nonce to ciphertext for storage
            // Format: [nonce (24 bytes) || ciphertext || poly1305_tag (16 bytes)]
            Ok([nonce.to_vec(), ciphertext].concat())
        } else {
            error!("Failed to decrypt encryption key");
            Err(Error::Store {
                operation: "decrypt_key".to_owned(),
                reason: "Can't decrypt key".to_owned(),
            })
        }
    }

    /// Decrypt bytes using XChaCha20-Poly1305 AEAD.
    ///
    /// # Security considerations
    ///
    /// - Validates ciphertext length to prevent panics
    /// - Verifies authentication tag (built into XChaCha20-Poly1305)
    /// - Returns error if authentication fails (data tampered or corrupted)
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte decryption key (must match encryption key)
    /// * `ciphertext` - Encrypted data with format: [nonce || ciphertext || auth_tag]
    ///
    /// # Returns
    ///
    /// Decrypted plaintext data
    ///
    /// # Errors
    ///
    /// Returns Error::Store if:
    /// - Key length is not exactly 32 bytes
    /// - Ciphertext is too short (minimum: nonce + tag = 40 bytes)
    /// - Authentication tag verification fails (tampering detected)
    /// - Decryption operation fails
    ///
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
                operation: "validate_ciphertext".to_owned(),
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
                error!(expected = 32, got = key.len(), "Invalid decryption key length");
                return Err(Error::Store {
                    operation: "validate_key_length".to_owned(),
                    reason: format!(
                        "Invalid key length: expected 32 bytes, got {}",
                        key.len()
                    ),
                });
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
                    Error::Store {
                        operation: "decrypt_data".to_owned(),
                        reason: format!(
                            "Decryption failed (possible tampering): {}",
                            e
                        ),
                    }
                })?;

            Ok(plaintext)
        } else {
            error!("Failed to decrypt decryption key");
            Err(Error::Store {
                operation: "decrypt_key".to_owned(),
                reason: "Can't decrypt key".to_owned(),
            })
        }
    }
}

/// Store command.
#[derive(Debug, Clone)]
pub enum StoreCommand<P, E> {
    Persist(E),
    PersistLight(E, P),
    Snapshot(P),
    LastEvent,
    LastEventNumber,
    LastEventsFrom(u64),
    GetEvents { from: u64, to: u64 },
    Recover,
    Purge,
}

/// Implements `Message` for store command.
impl<P, E> Message for StoreCommand<P, E>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
    E: Event + BorshSerialize + BorshDeserialize,
{
}

/// Store response.
#[derive(Debug, Clone)]
pub enum StoreResponse<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
    None,
    Persisted,
    Snapshotted,
    State(Option<P>),
    LastEvent(Option<P::Event>),
    LastEventNumber(u64),
    Events(Vec<P::Event>),
    Error(Error),
}

/// Implements `Response` for store response.
impl<P> Response for StoreResponse<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
}

/// Store event.
#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
pub enum StoreEvent {
    Persisted,
    Snapshotted,
}

/// Implements `Event` for store event.
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

    fn get_span(id: &str, _parent_span: Option<tracing::Span>) -> tracing::Span {
        info_span!("Store", id = %id)
    }
}

#[async_trait]
impl<P> Handler<Store<P>> for Store<P>
where
    P: PersistentActor,
    P::Event: BorshSerialize + BorshDeserialize,
{
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: StoreCommand<P, P::Event>,
        _ctx: &mut ActorContext<Store<P>>,
    ) -> Result<StoreResponse<P>, ActorError> {
        // Match the command.
        match msg {
            // Persist an event.
            StoreCommand::Persist(event) => match self.persist(&event) {
                Ok(_) => {
                    debug!("Persisted event: {:?}", event);
                    Ok(StoreResponse::Persisted)
                }
                Err(e) => Ok(StoreResponse::Error(e)),
            },
            // Light persistence of an event.
            StoreCommand::PersistLight(event, actor) => {
                match self.persist_state(&event, &actor) {
                    Ok(_) => {
                        debug!("Light persistence of event: {:?}", event);
                        Ok(StoreResponse::Persisted)
                    }
                    Err(e) => Ok(StoreResponse::Error(e)),
                }
            }
            // Snapshot the state.
            StoreCommand::Snapshot(actor) => match self.snapshot(&actor) {
                Ok(_) => {
                    debug!("Snapshotted state: {:?}", actor);
                    Ok(StoreResponse::Snapshotted)
                }
                Err(e) => Ok(StoreResponse::Error(e)),
            },
            // Recover the state.
            StoreCommand::Recover => match self.recover() {
                Ok(state) => {
                    debug!("Recovered state: {:?}", state);
                    Ok(StoreResponse::State(state))
                }
                Err(e) => Ok(StoreResponse::Error(e)),
            },
            StoreCommand::GetEvents { from, to } => {
                let events = self.events(from, to).map_err(|e| {
                    ActorError::StoreOperation {
                        operation: "get_events_range".to_owned(),
                        reason: format!("Unable to get events range: {}", e),
                    }
                })?;
                Ok(StoreResponse::Events(events))
            }
            // Get the last event.
            StoreCommand::LastEvent => match self.last_event() {
                Ok(event) => {
                    debug!("Last event: {:?}", event);
                    Ok(StoreResponse::LastEvent(event))
                }
                Err(e) => Ok(StoreResponse::Error(e)),
            },
            // Purge the store.
            StoreCommand::Purge => match self.purge() {
                Ok(_) => {
                    debug!("Purged store");
                    Ok(StoreResponse::None)
                }
                Err(e) => Ok(StoreResponse::Error(e)),
            },
            // Get the last event number.
            StoreCommand::LastEventNumber => {
                Ok(StoreResponse::LastEventNumber(self.event_counter))
            }
            // Get the last events from a number of counter.
            StoreCommand::LastEventsFrom(from) => {
                let events =
                    self.events(from, self.event_counter).map_err(|e| {
                        ActorError::StoreOperation {
                            operation: "get_latest_events".to_owned(),
                            reason: format!(
                                "Unable to get the latest events: {}",
                                e
                            ),
                        }
                    })?;
                Ok(StoreResponse::Events(events))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::vec;
    use tokio_util::sync::CancellationToken;
    use tracing::info_span;

    use super::*;
    use crate::memory::MemoryManager;

    use ave_actors_actor::{ActorRef, ActorSystem, Error as ActorError, build_tracing_subscriber};

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

        fn get_span(id: &str, _parent_span: Option<tracing::Span>) -> tracing::Span {
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

        async fn pre_stop(
            &mut self,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), ActorError> {
            let store: ActorRef<Store<Self>> =
                ctx.get_child("store").await.unwrap();
            let response = store
                .ask(StoreCommand::Snapshot(self.clone()))
                .await
                .unwrap();
            if let StoreResponse::Snapshotted = response {
                store.ask_stop().await
            } else {
                Err(ActorError::StoreOperation {
                    operation: "snapshot_state".to_owned(),
                    reason: "Can't snapshot state".to_string(),
                })
            }
        }
    }

    #[async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;
        type Event = TestEvent;
        type Response = TestResponse;

        fn get_span(id: &str, _parent_span: Option<tracing::Span>) -> tracing::Span {
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

        async fn pre_stop(
            &mut self,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), ActorError> {
            let store: ActorRef<Store<Self>> =
                ctx.get_child("store").await.unwrap();
            let response = store
                .ask(StoreCommand::Snapshot(self.clone()))
                .await
                .unwrap();
            if let StoreResponse::Snapshotted = response {
                store.ask_stop().await
            } else {
                Err(ActorError::StoreOperation {
                    operation: "snapshot_state".to_owned(),
                    reason: "Can't snapshot state".to_string(),
                })
            }
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

    #[tokio::test]
    async fn test_store_actor() {
        build_tracing_subscriber();
        let (system, mut runner) =
            ActorSystem::create(CancellationToken::new());
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
            .ask(StoreCommand::GetEvents { from: 0, to: 2 })
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

    #[tokio::test]
    async fn test_persistent_light_actor() {
        build_tracing_subscriber();
        let (system, ..) = ActorSystem::create(CancellationToken::new());

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

    #[tokio::test]
    async fn test_persistent_actor() {
        build_tracing_subscriber();
        let (system, mut runner) =
            ActorSystem::create(CancellationToken::new());
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

    #[tokio::test]
    async fn test_encrypt_decrypt() {
        build_tracing_subscriber();
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
