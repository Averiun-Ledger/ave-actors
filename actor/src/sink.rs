//! Named sinks with filtered subscribers and optional retry.
//!
//! A [`Sink`] is a lightweight router.  Code holding an [`ActorRef`] or
//! [`ActorContext`](crate::ActorContext) registers named sinks; the actor
//! then explicitly sends events to a sink by name and the sink distributes
//! the event to every subscriber whose filter accepts it.

use crate::{Error, Event};

use async_trait::async_trait;
use std::{
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{error, warn};

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

struct SinkInner<E: Event> {
    name: String,
    entries: RwLock<Vec<SinkEntry<E>>>,
    max_concurrent: AtomicUsize,
    sender: Mutex<Option<tokio::sync::mpsc::Sender<Arc<E>>>>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Named sink that routes events to filtered subscribers.
pub struct Sink<E: Event> {
    inner: Arc<SinkInner<E>>,
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
    /// used.
    pub fn new(name: impl Into<String>, max_concurrent: Option<usize>) -> Self {
        let name = name.into();
        let max_concurrent = max_concurrent.unwrap_or(10);
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<Arc<E>>(1024);

        let inner = Arc::new_cyclic(|weak: &std::sync::Weak<SinkInner<E>>| {
            let worker_name = name.clone();
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
                    drop(inner);

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
                        let sink_name = worker_name.clone();

                        set.spawn(async move {
                            let _permit = permit;
                            match retry {
                                RetryPolicy::None => {
                                    if let Err(err) =
                                        subscriber.notify(event).await
                                    {
                                        error!(
                                            subscriber = %id,
                                            sink = %sink_name,
                                            error = %err,
                                            "Subscriber failed"
                                        );
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
                                                        sink = %sink_name,
                                                        error = %err,
                                                        attempts = max + 1,
                                                        "Subscriber exhausted retries"
                                                    );
                                                } else {
                                                    warn!(
                                                        subscriber = %id,
                                                        sink = %sink_name,
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
                entries: RwLock::new(Vec::new()),
                max_concurrent: AtomicUsize::new(max_concurrent),
                sender: Mutex::new(Some(sender)),
                worker: Mutex::new(Some(handle)),
            }
        });

        Self { inner }
    }

    /// Return the sink's name.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Update the maximum number of concurrent subscriber notifications.
    pub fn set_max_concurrent(&self, limit: usize) {
        self.inner.max_concurrent.store(limit, Ordering::Relaxed);
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
    /// The event is placed on an unbounded channel and processed by a
    /// persistent worker task so the caller never blocks.
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
                    warn!(
                        sink = %self.inner.name,
                        "Sink buffer full, event dropped"
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    warn!(
                        sink = %self.inner.name,
                        "Sink is closed, event dropped"
                    );
                }
            }
        } else {
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
        let mut sink = Sink::new("test", Some(2));
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
        let mut sink = Sink::new("test", Some(1));
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
        sink.set_max_concurrent(5);

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
        let mut sink = Sink::new("test", None);
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
}
