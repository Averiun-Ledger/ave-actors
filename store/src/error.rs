//! Error types for the store system.

use ave_actors_actor::Error as ActorError;
use std::fmt;
use thiserror::Error;

/// Canonical store operations used for diagnostics and error reporting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreOperation {
    StoreInit,
    Persist,
    PersistLight,
    PersistFull,
    Snapshot,
    Recover,
    ApplyEvent,
    ApplyEventOnStop,
    GetEventsRange,
    GetLatestEvents,
    ParseEventKey,
    EncodeEvent,
    DecodeEvent,
    DecodeState,
    EncodeActor,
    ValidateKeyLength,
    EncryptData,
    DecryptKey,
    ValidateCiphertext,
    DecryptData,
    LastEvent,
    Purge,
    Delete,
    LockManagerData,
    LockData,
    LockConnection,
    WalCheckpoint,
    FlushWal,
    OpenConnection,
    Insert,
    ExecuteBatch,
    ListCf,
    ColumnAccess,
    RocksdbOperation,
    /// Failure creating a collection (event log) handle in a backend.
    CreateCollection,
    /// Failure creating a state (snapshot) handle in a backend.
    CreateState,
    /// Diagnostic discriminator used only by tests.
    Test,
}

impl fmt::Display for StoreOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::StoreInit => "store_init",
            Self::Persist => "persist",
            Self::PersistLight => "persist_light",
            Self::PersistFull => "persist_full",
            Self::Snapshot => "snapshot",
            Self::Recover => "recover",
            Self::ApplyEvent => "apply_event",
            Self::ApplyEventOnStop => "apply_event_on_stop",
            Self::GetEventsRange => "get_events_range",
            Self::GetLatestEvents => "get_latest_events",
            Self::ParseEventKey => "parse_event_key",
            Self::EncodeEvent => "encode_event",
            Self::DecodeEvent => "decode_event",
            Self::DecodeState => "decode_state",
            Self::EncodeActor => "encode_actor",
            Self::ValidateKeyLength => "validate_key_length",
            Self::EncryptData => "encrypt_data",
            Self::DecryptKey => "decrypt_key",
            Self::ValidateCiphertext => "validate_ciphertext",
            Self::DecryptData => "decrypt_data",
            Self::LastEvent => "last_event",
            Self::Purge => "purge",
            Self::Delete => "delete",
            Self::LockManagerData => "lock_manager_data",
            Self::LockData => "lock_data",
            Self::LockConnection => "lock_connection",
            Self::WalCheckpoint => "wal_checkpoint",
            Self::FlushWal => "flush_wal",
            Self::OpenConnection => "open_connection",
            Self::Insert => "insert",
            Self::ExecuteBatch => "execute_batch",
            Self::ListCf => "list_cf",
            Self::ColumnAccess => "column_access",
            Self::RocksdbOperation => "rocksdb_operation",
            Self::CreateCollection => "create_collection",
            Self::CreateState => "create_state",
            Self::Test => "test",
        };
        f.write_str(value)
    }
}

/// Error type for the store system.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum Error {
    /// The store could not be created or initialized.
    ///
    /// Returned when the underlying database cannot be opened or allocated.
    #[error("failed to create store: {reason}")]
    CreateStore { reason: String },

    /// Failed to retrieve data for `key` from the store.
    ///
    /// Indicates an I/O or backend error while reading; the key may exist but
    /// is not readable. Use [`EntryNotFound`](Error::EntryNotFound) when a key is simply absent.
    #[error("failed to get data for key '{key}': {reason}")]
    Get { key: String, reason: String },

    /// The requested `key` does not exist in the store.
    #[error("entry not found: {key}")]
    EntryNotFound { key: String },

    /// A store operation failed.
    ///
    /// `operation` identifies which operation failed (e.g. `persist`, `snapshot`);
    /// `reason` contains the underlying error message.
    /// `source` optionally holds the original [`ActorError`] when the failure
    /// originated in actor logic (e.g. during recovery replay).
    #[error("store operation failed: {operation} - {reason}")]
    Store {
        operation: StoreOperation,
        reason: String,
        source: Option<ActorError>,
    },

    /// Invalid store or database configuration supplied by the user.
    #[error("invalid configuration for {component}: {reason}")]
    InvalidConfiguration {
        /// Component affected (e.g. "MachineSpec").
        component: String,
        /// Human-readable reason.
        reason: String,
    },
}
