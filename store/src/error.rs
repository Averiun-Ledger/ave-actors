//! # Errors module
//!
//! This module defines error types for the store system using `thiserror`.

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
    EmitPreStopError,
    EncodeEvent,
    RollbackPersistLight,
    DecodeEvent,
    DecodeState,
    EncodeActor,
    ValidateKeyLength,
    EncryptData,
    DecryptKey,
    ValidateCiphertext,
    DecryptData,
    Compact,
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
    CreateCollection,
    CreateState,
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
            Self::EmitPreStopError => "emit_prestop_error",
            Self::EncodeEvent => "encode_event",
            Self::RollbackPersistLight => "rollback_persist_light",
            Self::DecodeEvent => "decode_event",
            Self::DecodeState => "decode_state",
            Self::EncodeActor => "encode_actor",
            Self::ValidateKeyLength => "validate_key_length",
            Self::EncryptData => "encrypt_data",
            Self::DecryptKey => "decrypt_key",
            Self::ValidateCiphertext => "validate_ciphertext",
            Self::DecryptData => "decrypt_data",
            Self::Compact => "compact",
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
    /// Failed to create or initialize the store.
    ///
    /// This error occurs when the underlying database cannot be created,
    /// opened, or initialized properly.
    #[error("failed to create store: {reason}")]
    CreateStore {
        /// The reason why store creation failed.
        reason: String,
    },

    /// Failed to retrieve data from the store.
    ///
    /// This error indicates a problem occurred while attempting to read
    /// data from the store, but the data might exist.
    #[error("failed to get data for key '{key}': {reason}")]
    Get {
        /// The key that was being accessed.
        key: String,
        /// The reason why the get operation failed.
        reason: String,
    },

    /// Requested entry was not found in the store.
    ///
    /// This error indicates that the requested key does not exist in the store.
    #[error("entry not found: {key}")]
    EntryNotFound {
        /// The key that was not found.
        key: String,
    },

    /// A generic store operation failed.
    ///
    /// This error covers various store operations that don't fit into
    /// more specific error categories.
    #[error("store operation failed: {operation} - {reason}")]
    Store {
        /// The operation that was being performed.
        operation: StoreOperation,
        /// The reason why the operation failed.
        reason: String,
    },
}
