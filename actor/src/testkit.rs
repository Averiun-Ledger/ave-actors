//! Minimal test kit: [`TestProbe`] and [`TestSystem`].
//!
//! A probe is a spy actor that records every message it receives so
//! tests can assert on them without hand-rolled collector actors and
//! fixed sleeps. Waiting is deadline-based with [`Notify`] wakeups:
//! fast when the message arrives early, patient when the system is
//! slow.
//!
//! ```rust,no_run
//! use ave_actors_actor::{TestProbe, TestSystem};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), ave_actors_actor::Error> {
//! let system = TestSystem::start();
//! let probe = TestProbe::<()>::new();
//! let probe_ref = probe.spawn(system.system(), "probe").await?;
//! // ... make the actor under test tell `probe_ref` something ...
//! let msg = probe.expect_msg(Duration::from_secs(2)).await?;
//! system.shutdown().await;
//! # Ok(())
//! # }
//! ```

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::info_span;

use crate::actor::validate_timeout;
use crate::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystemConfig, Error, Event,
    Handler, Message, NotPersistentActor, ShutdownReason, Subscriber,
    SystemRef,
};

// ---------------------------------------------------------------------------
// TestSystem
// ---------------------------------------------------------------------------

/// A throwaway actor system for tests: starts the runner on creation
/// and stops it on [`shutdown`](TestSystem::shutdown) (or best-effort
/// on drop).
pub struct TestSystem {
    system: SystemRef,
    runner: Option<JoinHandle<ShutdownReason>>,
}

impl TestSystem {
    /// Starts a system with fresh cancellation tokens.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime, like
    /// [`ActorSystem::create`](crate::ActorSystem::create).
    pub fn start() -> Self {
        use tokio_util::sync::CancellationToken;

        use crate::ActorSystem;
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let handle = tokio::spawn(async move { runner.run().await });
        Self {
            system,
            runner: Some(handle),
        }
    }

    /// Borrow the system handle to create actors under test.
    pub const fn system(&self) -> &SystemRef {
        &self.system
    }

    /// Starts a system with an explicit configuration (e.g. tiny
    /// mailbox caps or watcher limits for rejection tests).
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the configuration is invalid.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    pub fn start_with_config(config: ActorSystemConfig) -> Result<Self, Error> {
        use tokio_util::sync::CancellationToken;

        use crate::ActorSystem;
        let (system, mut runner) = ActorSystem::create_with_config(
            CancellationToken::new(),
            CancellationToken::new(),
            config,
        )?;
        let handle = tokio::spawn(async move { runner.run().await });
        Ok(Self {
            system,
            runner: Some(handle),
        })
    }
    /// Stops the system and waits for the runner to finish.
    pub async fn shutdown(mut self) -> ShutdownReason {
        self.system.stop_system();
        match self.runner.take() {
            Some(handle) => handle.await.unwrap_or(ShutdownReason::Crash),
            None => ShutdownReason::Graceful,
        }
    }
}

impl Drop for TestSystem {
    fn drop(&mut self) {
        self.system.stop_system();
        if let Some(handle) = self.runner.take() {
            handle.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// TestProbe
// ---------------------------------------------------------------------------

/// Shared queue behind a spawned [`ProbeActor`].
#[derive(Debug)]
struct ProbeQueue<M> {
    messages: Mutex<VecDeque<M>>,
    notify: Notify,
}

/// Spy actor: records every received message for later assertions.
///
/// Cloneable: clones share the same queue. Create with
/// [`TestProbe::new`], spawn with [`TestProbe::spawn`], then assert
/// with `expect_*`.
#[derive(Debug, Clone)]
pub struct TestProbe<M> {
    queue: Arc<ProbeQueue<M>>,
}

impl<M> TestProbe<M> {
    /// Creates an unspawned probe (no actor exists yet).
    pub fn new() -> Self {
        Self {
            queue: Arc::new(ProbeQueue {
                messages: Mutex::new(VecDeque::new()),
                notify: Notify::new(),
            }),
        }
    }

    /// Number of messages currently buffered.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Returns `true` when no message is buffered.
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Takes all buffered messages, leaving the probe empty.
    pub fn drain(&self) -> Vec<M> {
        self.lock().drain(..).collect()
    }

    /// Pops one buffered message without waiting, if any.
    pub fn try_msg(&self) -> Option<M> {
        self.lock().pop_front()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, VecDeque<M>> {
        self.queue
            .messages
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}

impl<M: Message> TestProbe<M> {
    /// Spawns the probe actor under `name` on `system`.
    ///
    /// # Errors
    ///
    /// Returns [`Error`] when the name is taken or invalid.
    pub async fn spawn(
        &self,
        system: &SystemRef,
        name: &str,
    ) -> Result<ActorRef<ProbeActor<M>>, Error> {
        system
            .create_root_actor(
                name,
                ProbeActor {
                    probe: self.clone(),
                },
            )
            .await
    }

    /// Waits up to `timeout` for the next message.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Timeout`] when nothing arrives in time, or
    /// [`Error::InvalidConfiguration`] for an out-of-range timeout.
    pub async fn expect_msg(&self, timeout: Duration) -> Result<M, Error> {
        validate_timeout("expect_msg", timeout)?;
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(msg) = self.try_msg() {
                return Ok(msg);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout { duration: timeout });
            }
            // Register interest before re-checking so a message that
            // lands between the check above and the wait is not lost.
            let notified = self.queue.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(msg) = self.try_msg() {
                return Ok(msg);
            }
            tokio::time::timeout(remaining, notified).await.ok();
        }
    }

    /// Waits up to `timeout` for exactly `n` messages and returns them
    /// in arrival order.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Timeout`] when fewer than `n` messages arrive
    /// in time.
    pub async fn expect_count(
        &self,
        n: usize,
        timeout: Duration,
    ) -> Result<Vec<M>, Error> {
        validate_timeout("expect_count", timeout)?;
        let deadline = Instant::now() + timeout;
        loop {
            {
                let buffered = self.lock();
                if buffered.len() >= n {
                    break;
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout { duration: timeout });
            }
            let notified = self.queue.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                if self.lock().len() >= n {
                    break;
                }
            }
            tokio::time::timeout(remaining, notified).await.ok();
        }
        Ok(self.lock().drain(..n).collect())
    }

    /// Asserts that no message arrives within `duration`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Functional`] describing the first intruder when
    /// a message arrives, [`Error::Timeout`] is never returned: silence
    /// is success.
    pub async fn expect_no_msg(&self, duration: Duration) -> Result<(), Error> {
        validate_timeout("expect_no_msg", duration)?;
        let deadline = Instant::now() + duration;
        loop {
            if self.try_msg().is_some() {
                return Err(Error::Functional {
                    description: "expected no message, but one arrived"
                        .to_owned(),
                });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(());
            }
            let notified = self.queue.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.try_msg().is_some() {
                return Err(Error::Functional {
                    description: "expected no message, but one arrived"
                        .to_owned(),
                });
            }
            tokio::time::timeout(remaining, notified).await.ok();
        }
    }
}

impl<M> Default for TestProbe<M> {
    fn default() -> Self {
        Self::new()
    }
}

/// The actor behind [`TestProbe`]. Users never name this type: spawn
/// it through [`TestProbe::spawn`] and talk to the returned
/// [`ActorRef`].
#[derive(Debug, Clone)]
pub struct ProbeActor<M> {
    probe: TestProbe<M>,
}

impl<M: Message> NotPersistentActor for ProbeActor<M> {}

#[async_trait]
impl<M: Message> Actor for ProbeActor<M> {
    type Message = M;
    type Response = ();
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TestProbe", id = %id)
    }
}

#[async_trait]
impl<M: Message> Handler<Self> for ProbeActor<M> {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: M,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        self.probe.push(msg);
        Ok(())
    }
}

impl<M> TestProbe<M> {
    fn push(&self, msg: M) {
        self.lock().push_back(msg);
        self.queue.notify.notify_one();
    }
}

/// Lets a probe act as a sink subscriber: subscribed events land in
/// the same queue as told messages, so sink tests use one assertion
/// API for both.
#[async_trait]
impl<M: Event + Clone> Subscriber<M> for TestProbe<M> {
    async fn notify(&self, event: Arc<M>) -> Result<(), Error> {
        self.push((*event).clone());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Self-tests (the TDD red phase for this module)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use test_log::test;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Ping(u64);

    impl Message for Ping {}
    impl Event for Ping {}

    use crate::Subscriber;

    #[test(tokio::test)]
    async fn probe_receives_tell() {
        let system = TestSystem::start();
        let probe = TestProbe::new();
        let probe_ref = probe.spawn(system.system(), "probe").await.unwrap();

        probe_ref.tell(Ping(1)).await.unwrap();
        let got = probe.expect_msg(Duration::from_secs(2)).await.unwrap();
        assert_eq!(got, Ping(1));
        assert!(probe.is_empty());
        system.shutdown().await;
    }

    #[test(tokio::test)]
    async fn probe_times_out_when_silent() {
        let system = TestSystem::start();
        let probe = TestProbe::<Ping>::new();
        let _probe_ref = probe.spawn(system.system(), "probe").await.unwrap();

        let err = probe
            .expect_msg(Duration::from_millis(100))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Timeout { .. }));
        system.shutdown().await;
    }

    #[test(tokio::test)]
    async fn probe_collects_sequence_in_order() {
        let system = TestSystem::start();
        let probe = TestProbe::new();
        let probe_ref = probe.spawn(system.system(), "probe").await.unwrap();

        for i in 0..5 {
            probe_ref.tell(Ping(i)).await.unwrap();
        }
        let got = probe.expect_count(5, Duration::from_secs(2)).await.unwrap();
        assert_eq!(got, vec![Ping(0), Ping(1), Ping(2), Ping(3), Ping(4)]);
        system.shutdown().await;
    }

    #[test(tokio::test)]
    async fn probe_rejects_unexpected_message() {
        let system = TestSystem::start();
        let probe = TestProbe::new();
        let probe_ref = probe.spawn(system.system(), "probe").await.unwrap();

        probe_ref.tell(Ping(9)).await.unwrap();
        let err = probe
            .expect_no_msg(Duration::from_millis(200))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Functional { .. }));
        system.shutdown().await;
    }

    #[test(tokio::test)]
    async fn probe_accepts_silence() {
        let system = TestSystem::start();
        let probe = TestProbe::<Ping>::new();
        let _probe_ref = probe.spawn(system.system(), "probe").await.unwrap();

        probe
            .expect_no_msg(Duration::from_millis(100))
            .await
            .unwrap();
        system.shutdown().await;
    }

    #[test(tokio::test)]
    async fn probe_rejects_out_of_range_timeout() {
        let probe = TestProbe::<Ping>::new();
        let err = probe
            .expect_msg(Duration::from_secs(3600))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidConfiguration { .. }));
    }

    #[test(tokio::test)]
    async fn probe_collects_subscribed_events() {
        let probe = TestProbe::new();
        probe.notify(Arc::new(Ping(7))).await.unwrap();
        let got = probe.expect_msg(Duration::from_secs(2)).await.unwrap();
        assert_eq!(got, Ping(7));
    }
}
