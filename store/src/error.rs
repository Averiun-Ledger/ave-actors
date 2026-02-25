

//! # Errors module
//!
//! This module defines error types for the store system using `thiserror`.

use thiserror::Error;

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
        operation: String,
        /// The reason why the operation failed.
        reason: String,
    },
}
