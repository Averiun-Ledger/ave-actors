

//! # Errors module
//!
//! This module defines error types for the actor system using `thiserror`.

use crate::ActorPath;
use thiserror::Error;

/// Error type for the actor system.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum Error {
    // ===== Actor Lifecycle Errors =====
    /// Actor already exists at the specified path.
    ///
    /// This error indicates an attempt to create an actor at a path
    /// that is already occupied.
    #[error("actor '{path}' already exists")]
    Exists {
        /// The path where the actor already exists.
        path: ActorPath,
    },

    /// Actor not found at the specified path.
    ///
    /// This error indicates that no actor exists at the requested path.
    #[error("actor '{path}' not found")]
    NotFound {
        /// The path where the actor was expected but not found.
        path: ActorPath,
    },

    // ===== Message Passing Errors =====
    /// Failed to send a message to an actor.
    ///
    /// This error occurs when a message cannot be delivered to an actor's mailbox.
    #[error("failed to send message: {reason}")]
    Send {
        /// The reason why message sending failed.
        reason: String,
    },

    /// Actor returned an unexpected response type.
    ///
    /// This error occurs when an actor's response doesn't match the expected type.
    #[error("actor '{path}' returned unexpected response, expected: {expected}")]
    UnexpectedResponse {
        /// The path of the actor that sent the unexpected response.
        path: ActorPath,
        /// Description of the expected response type.
        expected: String,
    },

    /// Failed to send an event to the event bus.
    ///
    /// This error occurs when an event cannot be published to the event bus.
    #[error("failed to send event to event bus: {reason}")]
    SendEvent {
        /// The reason why event sending failed.
        reason: String,
    },

    /// A store operation failed.
    ///
    /// This error covers various store operations that don't fit into
    /// more specific error categories.
    #[error("store operation '{operation}' failed: {reason}")]
    StoreOperation {
        /// The operation that was being performed.
        operation: String,
        /// The reason why the operation failed.
        reason: String,
    },

    // ===== Helper Errors =====
    /// Failed to access a helper.
    ///
    /// This error occurs when a required helper cannot be accessed.
    #[error("failed to access helper '{name}': {reason}")]
    Helper {
        /// The name of the helper that could not be accessed.
        name: String,
        /// The reason why helper access failed.
        reason: String,
    },

    /// Maximum number of retry attempts reached.
    ///
    /// This error occurs when an operation has been retried the maximum
    /// allowed number of times without success.
    #[error("maximum retry attempts reached")]
    Retry,

    // ===== Functional Errors =====
    /// A recoverable functional error.
    ///
    /// This error indicates a problem that doesn't compromise the system's
    /// overall operation and may be recoverable.
    #[error("functional error (recoverable): {description}")]
    Functional {
        /// Description of the functional error.
        description: String,
    },

    /// A critical functional error.
    ///
    /// This error indicates a problem that compromises the system's operation
    /// and requires intervention.
    #[error("critical functional error: {description}")]
    FunctionalCritical {
        /// Description of the critical error.
        description: String,
    },
}
