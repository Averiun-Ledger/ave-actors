//! Message metadata and middleware interceptors.
//!
//! Every envelope carries a [`MessageMetadata`]: a unique message id,
//! a correlation id linking causally related messages, and the send
//! timestamp. Actors read the current message's metadata from their
//! [`ActorContext`](crate::ActorContext); senders propagate causality
//! explicitly with `tell_with` / `ask_with`.
//!
//! [`Interceptor`]s are system-wide middleware: they observe every
//! handled message before and after (path, kind, metadata, outcome,
//! elapsed) for logging, metrics, or validation. Register them with
//! [`SystemRef::add_interceptor`](crate::SystemRef::add_interceptor)
//! before spawning actors; each actor snapshots the registry at
//! spawn, so middleware costs nothing when none is registered.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::{ActorPath, Error};

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

/// Envelope metadata attached to every message.
///
/// `id` is unique per message in the system. `correlation_id` links
/// causally related messages: fresh sends start a new chain
/// (`correlation_id == id`), while `tell_with` / `ask_with` continue
/// an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageMetadata {
    /// Unique message identifier.
    pub id: u64,
    /// Correlation chain identifier.
    pub correlation_id: u64,
    /// When the envelope entered the mailbox.
    pub sent_at: Instant,
}

impl MessageMetadata {
    fn next_id() -> u64 {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    }

    /// Starts a new correlation chain.
    pub fn root() -> Self {
        let id = Self::next_id();
        Self {
            id,
            correlation_id: id,
            sent_at: Instant::now(),
        }
    }

    /// Continues the `correlation_id` chain with a fresh message id.
    pub fn correlated(correlation_id: u64) -> Self {
        Self {
            id: Self::next_id(),
            correlation_id,
            sent_at: Instant::now(),
        }
    }
}

// ---------------------------------------------------------------------------
// Interceptors
// ---------------------------------------------------------------------------

/// Read-only view passed to [`Interceptor`] hooks.
#[derive(Debug, Clone, Copy)]
pub struct Intercept<'a> {
    /// Path of the actor handling the message.
    pub path: &'a ActorPath,
    /// `"tell"` or `"ask"`.
    pub kind: &'static str,
    /// Envelope metadata of the message being handled.
    pub metadata: MessageMetadata,
}

/// System-wide message middleware.
///
/// Both hooks have empty default bodies: implement only what the
/// middleware needs. Hooks run on the actor's task around every
/// handled message (never during shutdown drain), so slow hooks
/// delay that actor — keep them fast.
pub trait Interceptor: Send + Sync + 'static {
    /// Runs before the actor handles the message.
    fn before_handle(&self, _ctx: Intercept<'_>) {}

    /// Runs after the actor handled the message, with the handler
    /// outcome and the time it took.
    fn after_handle(
        &self,
        _ctx: Intercept<'_>,
        _result: &Result<(), Error>,
        _elapsed: Duration,
    ) {
    }
}
