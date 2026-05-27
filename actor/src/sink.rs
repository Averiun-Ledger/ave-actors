//! Named sinks with filtered subscribers and optional retry.
//!
//! A [`Sink`] is a lightweight router.  Code holding an [`ActorRef`] or
//! [`ActorContext`](crate::ActorContext) registers named sinks; the actor
//! then explicitly sends events to a sink by name and the sink distributes
//! the event to every subscriber whose filter accepts it.

use crate::{Error, Event};

use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tracing::{error, warn};

/// Retry policy applied when a subscriber returns an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPolicy {
    /// If the subscriber fails it is ignored immediately.
    None,
    /// Retry up to `max` times waiting `backoff` between attempts.
    AtMost {
        /// Maximum number of retry attempts.
        max: u32,
        /// Fixed backoff duration between retries.
        backoff: Duration,
    },
}

/// A subscriber that receives events from a [`Sink`].
#[async_trait]
pub trait Subscriber<E: Event>: Send + Sync + 'static {
    /// Called for each event that passes the subscriber's filter.
    ///
    /// If this returns an error the sink applies the configured
    /// [`RetryPolicy`].
    async fn notify(&self, event: Arc<E>) -> Result<(), Error>;
}

/// Entry inside a [`Sink`] describing a single subscriber, its filter
/// and retry policy.
pub struct SinkEntry<E: Event> {
    /// Identifier for the subscriber.
    pub id: String,
    subscriber: Arc<dyn Subscriber<E>>,
    filter: Arc<dyn Fn(&E) -> bool + Send + Sync>,
    /// Retry policy for this subscriber.
    pub retry: RetryPolicy,
}

impl<E: Event> Clone for SinkEntry<E> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            subscriber: self.subscriber.clone(),
            filter: self.filter.clone(),
            retry: self.retry,
        }
    }
}

impl<E: Event> SinkEntry<E> {
    /// Create a new entry for `subscriber` identified by `id`.
    pub fn new(id: impl Into<String>, subscriber: impl Subscriber<E>) -> Self {
        Self {
            id: id.into(),
            subscriber: Arc::new(subscriber),
            filter: Arc::new(|_| true),
            retry: RetryPolicy::None,
        }
    }

    /// Set a filter so the subscriber only receives events for which
    /// `f` returns `true`.
    pub fn filter(
        mut self,
        f: impl Fn(&E) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.filter = Arc::new(f);
        self
    }

    /// Set the retry policy for this subscriber.
    pub const fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }
}

/// Named sink that routes events to filtered subscribers.
#[derive(Clone)]
pub struct Sink<E: Event> {
    name: String,
    entries: Vec<SinkEntry<E>>,
}

impl<E: Event> Sink<E> {
    /// Create a new sink with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            entries: Vec::new(),
        }
    }

    /// Return the sink's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Add a subscriber entry to this sink.
    pub fn add(
        &mut self,
        id: impl Into<String>,
        subscriber: impl Subscriber<E>,
    ) {
        self.entries.push(SinkEntry::new(id, subscriber));
    }

    /// Add a pre-built [`SinkEntry`] to this sink.
    pub fn add_entry(&mut self, entry: SinkEntry<E>) {
        self.entries.push(entry);
    }

    /// Remove the subscriber entry with `id` and return it, if present.
    pub fn remove_entry(&mut self, id: &str) -> Option<SinkEntry<E>> {
        let pos = self.entries.iter().position(|e| e.id == id)?;
        Some(self.entries.remove(pos))
    }

    /// Remove all subscriber entries from this sink.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Send `event` to every subscriber whose filter accepts it.
    ///
    /// Subscribers are processed concurrently in spawned tasks so the
    /// caller never blocks waiting for slow or retrying subscribers.
    /// If a subscriber returns an error and has a retry policy, the sink
    /// retries with the configured backoff.  After exhausting retries (or if
    /// no retry is configured) the error is logged.
    pub fn send(&self, event: Arc<E>) {
        let sink_name = self.name.clone();
        for entry in &self.entries {
            if !(entry.filter)(&event) {
                continue;
            }
            let subscriber = Arc::clone(&entry.subscriber);
            let id = entry.id.clone();
            let retry = entry.retry;
            let event = Arc::clone(&event);
            let sink_name = sink_name.clone();
            tokio::spawn(async move {
                match retry {
                    RetryPolicy::None => {
                        if let Err(err) = subscriber.notify(event).await {
                            error!(
                                subscriber = %id,
                                sink = %sink_name,
                                error = %err,
                                "Subscriber failed"
                            );
                        }
                    }
                    RetryPolicy::AtMost { max, backoff } => {
                        let mut ok = false;
                        for attempt in 0..=max {
                            match subscriber.notify(Arc::clone(&event)).await {
                                Ok(()) => {
                                    ok = true;
                                    break;
                                }
                                Err(err) => {
                                    if attempt == max {
                                        error!(
                                            subscriber = %id,
                                            sink = %sink_name,
                                            error = %err,
                                            attempts = max,
                                            "Subscriber exhausted retries"
                                        );
                                    } else {
                                        warn!(
                                            subscriber = %id,
                                            sink = %sink_name,
                                            attempt,
                                            "Subscriber failed, retrying"
                                        );
                                        tokio::time::sleep(backoff).await;
                                    }
                                }
                            }
                        }
                        if !ok {
                            error!(
                                subscriber = %id,
                                sink = %sink_name,
                                "Subscriber permanently failed"
                            );
                        }
                    }
                }
            });
        }
    }
}
