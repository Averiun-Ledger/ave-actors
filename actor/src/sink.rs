//! Named sinks with filtered subscribers and optional retry.
//!
//! A [`Sink`] is a lightweight router.  Code holding an [`ActorRef`] or
//! [`ActorContext`](crate::ActorContext) registers named sinks; the actor
//! then explicitly sends events to a sink by name and the sink distributes
//! the event to every subscriber whose filter accepts it.

use crate::{ActorPath, Error, Event};

#[cfg(feature = "prometheus")]
use crate::metrics::{SinkDropLabels, SinkLabels};
#[cfg(feature = "prometheus")]
use prometheus_client::metrics::counter::Counter;

use async_trait::async_trait;
use std::{
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{error, warn};

/// Default number of subscribers notified concurrently by a [`Sink`].
const DEFAULT_SINK_CONCURRENCY: usize = 10;

/// Maximum number of subscribers notified concurrently by a [`Sink`].
const MAX_SINK_CONCURRENCY: usize = 1_000_000;

/// Default event buffer capacity for a [`Sink`].
const DEFAULT_SINK_BUFFER_CAPACITY: usize = 1024;

/// Maximum event buffer capacity allowed for a [`Sink`].
const MAX_SINK_BUFFER_CAPACITY: usize = 1_000_000;

/// Maximum number of retries allowed in a [`RetryPolicy::AtMost`].
const MAX_RETRY_ATTEMPTS: u32 = 100;

/// Maximum backoff duration allowed in a [`RetryPolicy::AtMost`].
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(3600);

/// Retry policy applied when a subscriber returns an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RetryPolicy {
    /// If the subscriber fails it is ignored immediately.
    #[default]
    None,
    /// Retry up to `max` **additional** times waiting `backoff` between
    /// attempts.  The total number of delivery attempts is `max + 1`
    /// (one initial attempt plus `max` retries).
    AtMost {
        /// Maximum number of retry attempts *after* the first try.
        max: u32,
        /// Fixed backoff duration between retries.
        backoff: Duration,
    },
}

impl RetryPolicy {
    /// Validates the policy parameters.
    ///
    /// - `AtMost { max, backoff }` requires `max <= 100`,
    ///   `backoff > 0` and `backoff <= 1 hour`.
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::None => Ok(()),
            Self::AtMost { max, backoff } => {
                if *max > MAX_RETRY_ATTEMPTS {
                    return Err(Error::InvalidConfiguration {
                        component: "RetryPolicy::AtMost".to_owned(),
                        reason: format!(
                            "max retries cannot exceed {}",
                            MAX_RETRY_ATTEMPTS
                        ),
                    });
                }
                if backoff.is_zero() {
                    return Err(Error::InvalidConfiguration {
                        component: "RetryPolicy::AtMost".to_owned(),
                        reason: "backoff must be greater than zero".to_owned(),
                    });
                }
                if *backoff > MAX_RETRY_BACKOFF {
                    return Err(Error::InvalidConfiguration {
                        component: "RetryPolicy::AtMost".to_owned(),
                        reason: format!(
                            "backoff cannot exceed {:?}",
                            MAX_RETRY_BACKOFF
                        ),
                    });
                }
                Ok(())
            }
        }
    }
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

impl<E: Event> std::fmt::Debug for SinkEntry<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkEntry")
            .field("id", &self.id)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
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
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfiguration`] if `policy` contains invalid
    /// parameters (see [`RetryPolicy::validate`]).
    pub fn retry(mut self, policy: RetryPolicy) -> Result<Self, Error> {
        policy.validate()?;
        self.retry = policy;
        Ok(self)
    }
}

struct SinkInner<E: Event> {
    name: String,
    #[cfg(feature = "prometheus")]
    dropped_full_counter: Option<Counter>,
    #[cfg(feature = "prometheus")]
    dropped_closed_counter: Option<Counter>,
    #[cfg(feature = "prometheus")]
    delivery_failure_counter: Option<Counter>,
    entries: RwLock<Vec<SinkEntry<E>>>,
    max_concurrent: AtomicUsize,
    sender: Mutex<Option<tokio::sync::mpsc::Sender<Arc<E>>>>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl<E: Event> SinkInner<E> {
    #[cfg(feature = "prometheus")]
    fn inc_dropped_full(&self) {
        if let Some(c) = &self.dropped_full_counter {
            c.inc();
        }
    }

    #[cfg(feature = "prometheus")]
    fn inc_dropped_closed(&self) {
        if let Some(c) = &self.dropped_closed_counter {
            c.inc();
        }
    }

    #[cfg(feature = "prometheus")]
    fn inc_delivery_failure(&self) {
        if let Some(c) = &self.delivery_failure_counter {
            c.inc();
        }
    }

    #[cfg(not(feature = "prometheus"))]
    fn inc_dropped_full(&self) {}

    #[cfg(not(feature = "prometheus"))]
    fn inc_dropped_closed(&self) {}

    #[cfg(not(feature = "prometheus"))]
    fn inc_delivery_failure(&self) {}
}

/// Named sink that routes events to filtered subscribers.
pub struct Sink<E: Event> {
    inner: Arc<SinkInner<E>>,
}

impl<E: Event> std::fmt::Debug for Sink<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sink")
            .field("name", &self.inner.name)
            .finish_non_exhaustive()
    }
}

impl<E: Event> Clone for Sink<E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<E: Event> Sink<E> {
    /// Create a new sink with the given name.
    ///
    /// `max_concurrent` controls how many subscribers are notified
    /// concurrently for a single event.  If `None`, a default of 10 is
    /// used. The internal event buffer uses [`DEFAULT_SINK_BUFFER_CAPACITY`]
    /// slots.
    ///
    /// Sinks created directly through this constructor do not report
    /// Prometheus metrics. Actors should use
    /// [`ActorContext::register_sink`](crate::ActorContext::register_sink)
    /// or [`crate::ActorRef::register_sink`] when metrics collection is required.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfiguration`] if `max_concurrent` is
    /// `Some(0)` or exceeds [`MAX_SINK_CONCURRENCY`].
    pub fn new(
        name: impl Into<String>,
        max_concurrent: Option<usize>,
    ) -> Result<Self, Error> {
        Self::with_buffer_inner(
            name,
            max_concurrent,
            DEFAULT_SINK_BUFFER_CAPACITY,
            None,
            #[cfg(feature = "prometheus")]
            None,
        )
    }

    /// Create a new sink with the given name and event buffer capacity.
    ///
    /// `max_concurrent` controls how many subscribers are notified
    /// concurrently for a single event. If `None`, a default of 10 is used.
    /// `buffer_capacity` sets the size of the internal bounded channel; it
    /// must be between 1 and [`MAX_SINK_BUFFER_CAPACITY`].
    ///
    /// Sinks created directly through this constructor do not report
    /// Prometheus metrics. Actors should use
    /// [`ActorContext::register_sink_with_buffer`](crate::ActorContext::register_sink_with_buffer)
    /// or [`crate::ActorRef::register_sink_with_buffer`] when metrics collection
    /// is required.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfiguration`] if `max_concurrent` or
    /// `buffer_capacity` are out of range.
    pub fn with_buffer(
        name: impl Into<String>,
        max_concurrent: Option<usize>,
        buffer_capacity: usize,
    ) -> Result<Self, Error> {
        Self::with_buffer_inner(
            name,
            max_concurrent,
            buffer_capacity,
            None,
            #[cfg(feature = "prometheus")]
            None,
        )
    }

    /// Create a new sink with metrics collection.
    ///
    /// This is the internal constructor used by the actor runtime when the
    /// `prometheus` feature is enabled. External callers should use
    /// [`Sink::new`] and register the sink through an actor handle or context
    /// to obtain metrics.
    #[cfg(feature = "prometheus")]
    pub(crate) fn new_with_metrics(
        name: impl Into<String>,
        max_concurrent: Option<usize>,
        path: ActorPath,
        metrics: Option<Arc<crate::metrics::ActorMetrics>>,
    ) -> Result<Self, Error> {
        Self::with_buffer_inner(
            name,
            max_concurrent,
            DEFAULT_SINK_BUFFER_CAPACITY,
            Some(path),
            metrics,
        )
    }

    /// Create a new sink with a custom buffer capacity and metrics collection.
    ///
    /// This is the internal constructor used by the actor runtime when the
    /// `prometheus` feature is enabled.
    #[cfg(feature = "prometheus")]
    pub(crate) fn with_buffer_and_metrics(
        name: impl Into<String>,
        max_concurrent: Option<usize>,
        buffer_capacity: usize,
        path: ActorPath,
        metrics: Option<Arc<crate::metrics::ActorMetrics>>,
    ) -> Result<Self, Error> {
        Self::with_buffer_inner(
            name,
            max_concurrent,
            buffer_capacity,
            Some(path),
            metrics,
        )
    }

    fn with_buffer_inner(
        name: impl Into<String>,
        max_concurrent: Option<usize>,
        buffer_capacity: usize,
        _path: Option<ActorPath>,
        #[cfg(feature = "prometheus")] metrics: Option<
            Arc<crate::metrics::ActorMetrics>,
        >,
    ) -> Result<Self, Error> {
        let name = name.into();
        let max_concurrent = max_concurrent.unwrap_or(DEFAULT_SINK_CONCURRENCY);
        if max_concurrent == 0 {
            return Err(Error::InvalidConfiguration {
                component: "Sink".to_owned(),
                reason: "max_concurrent must be >= 1".to_owned(),
            });
        }
        if max_concurrent > MAX_SINK_CONCURRENCY {
            return Err(Error::InvalidConfiguration {
                component: "Sink".to_owned(),
                reason: format!(
                    "max_concurrent cannot exceed {}",
                    MAX_SINK_CONCURRENCY
                ),
            });
        }
        if buffer_capacity == 0 {
            return Err(Error::InvalidConfiguration {
                component: "Sink".to_owned(),
                reason: "buffer_capacity must be >= 1".to_owned(),
            });
        }
        if buffer_capacity > MAX_SINK_BUFFER_CAPACITY {
            return Err(Error::InvalidConfiguration {
                component: "Sink".to_owned(),
                reason: format!(
                    "buffer_capacity cannot exceed {}",
                    MAX_SINK_BUFFER_CAPACITY
                ),
            });
        }
        #[cfg(feature = "prometheus")]
        let scope = _path.as_ref().map(|p| Arc::from(p.scope_key()));

        #[cfg(feature = "prometheus")]
        let (
            dropped_full_counter,
            dropped_closed_counter,
            delivery_failure_counter,
        ) = if let (Some(m), Some(scope)) = (metrics.as_ref(), scope) {
            let sink_name = name.clone();
            let dropped_full = m
                .sink_events_dropped_total
                .get_or_create(&SinkDropLabels {
                    scope: Arc::clone(&scope),
                    sink_name: sink_name.clone(),
                    reason: "buffer_full",
                })
                .clone();
            let dropped_closed = m
                .sink_events_dropped_total
                .get_or_create(&SinkDropLabels {
                    scope: Arc::clone(&scope),
                    sink_name: sink_name.clone(),
                    reason: "closed",
                })
                .clone();
            let delivery_failure = m
                .sink_delivery_failures_total
                .get_or_create(&SinkLabels {
                    scope: Arc::clone(&scope),
                    sink_name,
                })
                .clone();
            (
                Some(dropped_full),
                Some(dropped_closed),
                Some(delivery_failure),
            )
        } else {
            (None, None, None)
        };

        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<Arc<E>>(buffer_capacity);

        let inner = Arc::new_cyclic(|weak: &std::sync::Weak<SinkInner<E>>| {
            let weak = weak.clone();
            let handle = tokio::spawn(async move {
                while let Some(event) = receiver.recv().await {
                    let Some(inner) = weak.upgrade() else {
                        break;
                    };
                    let limit =
                        inner.max_concurrent.load(Ordering::Relaxed).max(1);
                    let to_notify: Vec<SinkEntry<E>> = {
                        let entries = inner
                            .entries
                            .read()
                            .unwrap_or_else(|e| e.into_inner());
                        entries
                            .iter()
                            .filter(|e| (e.filter)(&event))
                            .cloned()
                            .collect()
                    };

                    let semaphore =
                        Arc::new(tokio::sync::Semaphore::new(limit));
                    let mut set = tokio::task::JoinSet::new();

                    for entry in to_notify {
                        let Ok(permit) =
                            semaphore.clone().acquire_owned().await
                        else {
                            continue;
                        };
                        let subscriber = entry.subscriber;
                        let id = entry.id;
                        let retry = entry.retry;
                        let event = Arc::clone(&event);
                        let inner = Arc::clone(&inner);

                        set.spawn(async move {
                            let _permit = permit;
                            match retry {
                                RetryPolicy::None => {
                                    if let Err(err) =
                                        subscriber.notify(event).await
                                    {
                                        error!(
                                            subscriber = %id,
                                            sink = %inner.name,
                                            error = %err,
                                            "Subscriber failed"
                                        );
                                        inner.inc_delivery_failure();
                                    }
                                }
                                RetryPolicy::AtMost { max, backoff } => {
                                    for attempt in 0..=max {
                                        match subscriber
                                            .notify(Arc::clone(&event))
                                            .await
                                        {
                                            Ok(()) => break,
                                            Err(err) => {
                                                if attempt == max {
                                                    error!(
                                                        subscriber = %id,
                                                        sink = %inner.name,
                                                        error = %err,
                                                        attempts = max + 1,
                                                        "Subscriber exhausted retries"
                                                    );
                                                    inner.inc_delivery_failure();
                                                } else {
                                                    warn!(
                                                        subscriber = %id,
                                                        sink = %inner.name,
                                                        attempt = attempt + 1,
                                                        "Subscriber failed, retrying"
                                                    );
                                                    tokio::time::sleep(
                                                        backoff,
                                                    )
                                                    .await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        });
                    }

                    while set.join_next().await.is_some() {}
                }
            });

            SinkInner {
                name,
                #[cfg(feature = "prometheus")]
                dropped_full_counter,
                #[cfg(feature = "prometheus")]
                dropped_closed_counter,
                #[cfg(feature = "prometheus")]
                delivery_failure_counter,
                entries: RwLock::new(Vec::new()),
                max_concurrent: AtomicUsize::new(max_concurrent),
                sender: Mutex::new(Some(sender)),
                worker: Mutex::new(Some(handle)),
            }
        });

        Ok(Self { inner })
    }

    /// Return the sink's name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Update the maximum number of concurrent subscriber notifications.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfiguration`] if `limit` is `0` or exceeds
    /// [`MAX_SINK_CONCURRENCY`].
    pub fn set_max_concurrent(&self, limit: usize) -> Result<(), Error> {
        if limit == 0 {
            return Err(Error::InvalidConfiguration {
                component: "Sink".to_owned(),
                reason: "max_concurrent must be >= 1".to_owned(),
            });
        }
        if limit > MAX_SINK_CONCURRENCY {
            return Err(Error::InvalidConfiguration {
                component: "Sink".to_owned(),
                reason: format!(
                    "max_concurrent cannot exceed {}",
                    MAX_SINK_CONCURRENCY
                ),
            });
        }
        self.inner.max_concurrent.store(limit, Ordering::Relaxed);
        Ok(())
    }

    /// Add a subscriber entry to this sink.
    pub fn add(
        &mut self,
        id: impl Into<String>,
        subscriber: impl Subscriber<E>,
    ) {
        self.inner
            .entries
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .push(SinkEntry::new(id, subscriber));
    }

    /// Add a pre-built [`SinkEntry`] to this sink.
    pub fn add_entry(&mut self, entry: SinkEntry<E>) {
        self.inner
            .entries
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .push(entry);
    }

    /// Remove the subscriber entry with `id` and return it, if present.
    pub fn remove_entry(&mut self, id: &str) -> Option<SinkEntry<E>> {
        let mut entries = self
            .inner
            .entries
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let pos = entries.iter().position(|e| e.id == id)?;
        Some(entries.remove(pos))
    }

    /// Returns the number of subscriber entries in this sink.
    pub fn len(&self) -> usize {
        self.inner
            .entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Returns `true` if this sink has no subscribers.
    pub fn is_empty(&self) -> bool {
        self.inner
            .entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    /// Remove all subscriber entries from this sink.
    pub fn clear(&mut self) {
        self.inner
            .entries
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// Send `event` to every subscriber whose filter accepts it.
    ///
    /// The event is placed on a bounded channel and processed by a
    /// persistent worker task so the caller never blocks. If the channel is
    /// full the event is dropped and a warning is logged.
    pub fn send(&self, event: Arc<E>) {
        if let Some(sender) = self
            .inner
            .sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            match sender.try_send(event) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    self.inner.inc_dropped_full();
                    warn!(
                        sink = %self.inner.name,
                        "Sink buffer full, event dropped"
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    self.inner.inc_dropped_closed();
                    warn!(
                        sink = %self.inner.name,
                        "Sink is closed, event dropped"
                    );
                }
            }
        } else {
            self.inner.inc_dropped_closed();
            warn!(
                sink = %self.inner.name,
                "Sink is closed, event dropped"
            );
        }
    }

    /// Gracefully shut down the sink.
    ///
    /// Closes the channel so no new events are accepted, then waits up to
    /// `deadline` for the worker to finish processing pending events.
    /// Returns `true` if the worker finished cleanly, `false` if it was
    /// aborted.
    pub async fn shutdown(&self, deadline: Instant) -> bool {
        // Close the channel: drop the sender so the worker sees None.
        drop(
            self.inner
                .sender
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take(),
        );

        let worker = self
            .inner
            .worker
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let mut handle = match worker {
            Some(h) => h,
            None => return true,
        };

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            handle.abort();
            return false;
        }

        match tokio::time::timeout(remaining, &mut handle).await {
            Ok(_) => true,
            Err(_) => {
                handle.abort();
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    struct SleepSubscriber {
        millis: u64,
        done: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Subscriber<()> for SleepSubscriber {
        async fn notify(&self, _event: Arc<()>) -> Result<(), Error> {
            tokio::time::sleep(Duration::from_millis(self.millis)).await;
            self.done.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct CountingSubscriber {
        count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Subscriber<()> for CountingSubscriber {
        async fn notify(&self, _event: Arc<()>) -> Result<(), Error> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_sink_concurrency_limit() {
        let mut sink = Sink::new("test", Some(2)).expect("valid concurrency");
        let done = Arc::new(AtomicUsize::new(0));
        for i in 0..5 {
            sink.add(
                format!("sub-{}", i),
                SleepSubscriber {
                    millis: 100,
                    done: Arc::clone(&done),
                },
            );
        }

        let start = Instant::now();
        sink.send(Arc::new(()));
        while done.load(Ordering::SeqCst) < 5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(300),
            "expected >= 300ms with concurrency=2, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn test_sink_hot_reload_max_concurrent() {
        let mut sink = Sink::new("test", Some(1)).expect("valid concurrency");
        let done = Arc::new(AtomicUsize::new(0));
        for i in 0..5 {
            sink.add(
                format!("sub-{}", i),
                SleepSubscriber {
                    millis: 100,
                    done: Arc::clone(&done),
                },
            );
        }

        let start = Instant::now();
        sink.send(Arc::new(()));
        while done.load(Ordering::SeqCst) < 5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let elapsed1 = start.elapsed();

        done.store(0, Ordering::SeqCst);
        sink.set_max_concurrent(5).expect("valid concurrency");

        let start = Instant::now();
        sink.send(Arc::new(()));
        while done.load(Ordering::SeqCst) < 5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let elapsed2 = start.elapsed();

        assert!(
            elapsed2 < elapsed1,
            "expected second event to be faster: elapsed1={:?} elapsed2={:?}",
            elapsed1,
            elapsed2
        );
        assert!(
            elapsed1 >= Duration::from_millis(400),
            "expected first event >= 400ms with limit=1, got {:?}",
            elapsed1
        );
        assert!(
            elapsed2 < Duration::from_millis(250),
            "expected second event < 250ms with limit=5, got {:?}",
            elapsed2
        );
    }

    #[tokio::test]
    async fn test_sink_no_event_loss() {
        let mut sink = Sink::new("test", None).expect("valid concurrency");
        let count = Arc::new(AtomicUsize::new(0));
        sink.add(
            "counter",
            CountingSubscriber {
                count: Arc::clone(&count),
            },
        );

        for _ in 0..1000 {
            sink.send(Arc::new(()));
        }

        while count.load(Ordering::SeqCst) < 1000 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert_eq!(count.load(Ordering::SeqCst), 1000);
    }

    #[test]
    fn test_sink_new_rejects_zero_concurrency() {
        let err = Sink::<()>::new("test", Some(0))
            .expect_err("zero concurrency should be rejected");
        assert!(
            matches!(err, Error::InvalidConfiguration { component, .. } if component == "Sink")
        );
    }

    #[test]
    fn test_sink_new_rejects_excessive_concurrency() {
        let err = Sink::<()>::new("test", Some(MAX_SINK_CONCURRENCY + 1))
            .expect_err("excessive concurrency should be rejected");
        assert!(
            matches!(err, Error::InvalidConfiguration { component, .. } if component == "Sink")
        );
    }

    #[tokio::test]
    async fn test_sink_set_max_concurrent_rejects_zero() {
        let sink = Sink::<()>::new("test", Some(1)).expect("valid concurrency");
        let err = sink
            .set_max_concurrent(0)
            .expect_err("zero concurrency should be rejected");
        assert!(
            matches!(err, Error::InvalidConfiguration { component, .. } if component == "Sink")
        );
    }

    #[tokio::test]
    async fn test_sink_with_buffer_accepts_valid_capacity() {
        let result = Sink::<()>::with_buffer("test", None, 1);
        let Ok(sink) = result else {
            panic!("buffer capacity of 1 should be valid");
        };
        assert_eq!(sink.name(), "test");
    }

    #[test]
    fn test_sink_with_buffer_rejects_zero_capacity() {
        let result = Sink::<()>::with_buffer("test", None, 0);
        let Err(err) = result else {
            panic!("zero buffer capacity should be rejected");
        };
        assert!(
            matches!(err, Error::InvalidConfiguration { component, .. } if component == "Sink")
        );
    }

    #[test]
    fn test_sink_with_buffer_rejects_excessive_capacity() {
        let result =
            Sink::<()>::with_buffer("test", None, MAX_SINK_BUFFER_CAPACITY + 1);
        let Err(err) = result else {
            panic!("excessive buffer capacity should be rejected");
        };
        assert!(
            matches!(err, Error::InvalidConfiguration { component, .. } if component == "Sink")
        );
    }

    #[test]
    fn test_retry_policy_at_most_rejects_zero_backoff() {
        let policy = RetryPolicy::AtMost {
            max: 1,
            backoff: Duration::ZERO,
        };
        let err = policy
            .validate()
            .expect_err("zero backoff should be rejected");
        assert!(
            matches!(err, Error::InvalidConfiguration { component, .. } if component == "RetryPolicy::AtMost")
        );
    }

    #[test]
    fn test_retry_policy_at_most_rejects_excessive_backoff() {
        let policy = RetryPolicy::AtMost {
            max: 1,
            backoff: Duration::from_secs(3601),
        };
        let err = policy
            .validate()
            .expect_err("excessive backoff should be rejected");
        assert!(
            matches!(err, Error::InvalidConfiguration { component, .. } if component == "RetryPolicy::AtMost")
        );
    }

    #[test]
    fn test_retry_policy_at_most_rejects_too_many_retries() {
        let policy = RetryPolicy::AtMost {
            max: MAX_RETRY_ATTEMPTS + 1,
            backoff: Duration::from_millis(100),
        };
        let err = policy
            .validate()
            .expect_err("too many retries should be rejected");
        assert!(
            matches!(err, Error::InvalidConfiguration { component, .. } if component == "RetryPolicy::AtMost")
        );
    }

    #[test]
    fn test_retry_policy_valid() {
        let policy = RetryPolicy::AtMost {
            max: 5,
            backoff: Duration::from_millis(100),
        };
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_sink_entry_retry_rejects_invalid_policy() {
        let entry = SinkEntry::<()>::new(
            "sub",
            CountingSubscriber {
                count: Arc::new(AtomicUsize::new(0)),
            },
        );
        let invalid = RetryPolicy::AtMost {
            max: 1,
            backoff: Duration::ZERO,
        };
        let err = entry
            .retry(invalid)
            .expect_err("invalid retry policy should be rejected");
        assert!(
            matches!(err, Error::InvalidConfiguration { component, .. } if component == "RetryPolicy::AtMost")
        );
    }
}

#[cfg(all(test, feature = "prometheus"))]
mod prometheus_tests {
    use super::*;
    use crate::ActorPath;
    use crate::metrics::{ActorMetrics, SinkDropLabels, SinkLabels};
    use async_trait::async_trait;
    use std::sync::Arc;

    struct FailingSubscriber;

    #[async_trait]
    impl Subscriber<()> for FailingSubscriber {
        async fn notify(&self, _event: Arc<()>) -> Result<(), Error> {
            Err(Error::Functional {
                description: "intentional failure".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn test_sink_events_dropped_metric() {
        let metrics = Arc::new(ActorMetrics::new());
        let sink = Sink::with_buffer_and_metrics(
            "full",
            None,
            2,
            ActorPath::from("/user/test"),
            Some(Arc::clone(&metrics)),
        )
        .expect("valid sink");

        // Flood the channel before the worker task is scheduled.
        for _ in 0..10 {
            sink.send(Arc::new(()));
        }

        let dropped = metrics
            .sink_events_dropped_total
            .get_or_create(&SinkDropLabels {
                scope: Arc::from("user"),
                sink_name: "full".to_owned(),
                reason: "buffer_full",
            })
            .get();
        assert!(dropped > 0, "expected some events to be dropped");
    }

    #[tokio::test]
    async fn test_sink_delivery_failures_metric() {
        let metrics = Arc::new(ActorMetrics::new());
        let mut sink = Sink::new_with_metrics(
            "fail",
            None,
            ActorPath::from("/user/test"),
            Some(Arc::clone(&metrics)),
        )
        .expect("valid sink");

        sink.add("failing", FailingSubscriber);
        sink.send(Arc::new(()));

        // Give the worker time to process the failed delivery.
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            metrics
                .sink_delivery_failures_total
                .get_or_create(&SinkLabels {
                    scope: Arc::from("user"),
                    sink_name: "fail".to_owned(),
                })
                .get(),
            1
        );
    }
}
