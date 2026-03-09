//! # Actor system
//!
//! The `system` module provides the `ActorSystem` type. The `ActorSystem` type is the responsible for
//! creating and managing actors.
//!

use crate::{
    Actor, ActorPath, ActorRef, Error, Event, Handler,
    actor::ChildErrorSender,
    runner::{ActorRunner, StopHandle, StopSender},
    sink::Sink,
};

use tokio::sync::{RwLock, broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use tracing::{Instrument, Span, debug, error, warn};

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// The reason why the actor system stopped.
///
/// Returned by [`SystemRunner::run()`] so the caller can decide the appropriate
/// exit code or recovery action.
///
/// # Example
///
/// ```ignore
/// let reason = runner.run().await;
/// std::process::exit(reason.exit_code());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShutdownReason {
    /// System was stopped gracefully (e.g., SIGTERM or explicit operator request).
    /// Exit with code 0 — no automatic restart needed.
    Graceful,
    /// System crashed due to an internal actor failure.
    /// Exit with a non-zero code so the supervisor (e.g., Docker) restarts the process.
    Crash,
}

impl ShutdownReason {
    /// Returns the appropriate process exit code for this shutdown reason.
    ///
    /// - `0` for graceful shutdown.
    /// - `1` for crash (supervisor should restart).
    pub const fn exit_code(&self) -> i32 {
        match self {
            Self::Graceful => 0,
            Self::Crash => 1,
        }
    }
}

/// Actor system factory.
///
/// This is the main entry point for creating an actor system instance.
/// The ActorSystem provides factory methods for initializing the system
/// infrastructure including the SystemRef and SystemRunner.
///
pub struct ActorSystem {}

/// Default implementation for `ActorSystem`.
impl ActorSystem {
    /// Creates a new actor system with its reference and runner.
    ///
    /// # Arguments
    ///
    /// * `graceful_token` - Cancelled by an external signal (SIGTERM, operator).
    ///   Results in [`ShutdownReason::Graceful`] and exit code 0.
    /// * `crash_token` - Cancelled by an actor on unrecoverable failure.
    ///   Results in [`ShutdownReason::Crash`] and exit code 1, triggering a
    ///   supervisor restart.
    ///
    /// # Returns
    ///
    /// Returns a tuple of:
    /// - `SystemRef` - Cloneable reference for creating and managing actors.
    /// - `SystemRunner` - Event loop that must be driven to process system events.
    ///
    pub fn create(
        graceful_token: CancellationToken,
        crash_token: CancellationToken,
    ) -> (SystemRef, SystemRunner) {
        let (event_sender, event_receiver) = mpsc::channel(4);
        let system = SystemRef::new(event_sender, graceful_token, crash_token);
        let runner = SystemRunner::new(event_receiver);
        (system, runner)
    }
}

/// System-level events for coordinating actor system lifecycle.
/// `StopSystem` is used internally by the runner. Other variants are also
/// broadcast so callers can observe system-level activity.
///
#[derive(Debug, Clone)]
pub enum SystemEvent {
    /// Non-fatal error emitted by a root actor that has no parent to receive it.
    ActorError {
        /// Path of the actor that emitted the error.
        path: ActorPath,
        /// Error emitted by that actor.
        error: Error,
    },
    /// Signals that the actor system should stop.
    /// Carries the reason so the runner can report it to the caller.
    StopSystem(ShutdownReason),
}

/// Cloneable reference to the actor system.
///
/// The SystemRef provides methods for creating, retrieving, and managing actors.
/// Multiple SystemRef instances can be cloned and used concurrently across
/// different parts of the application.
///
/// # Thread Safety
///
/// SystemRef is thread-safe and can be cloned and shared across tasks.
/// All operations use internal locks for safe concurrent access.
///
#[derive(Clone)]
pub struct SystemRef {
    /// Registry of all actors in the system, indexed by their paths.
    /// Uses type erasure (Any) to store heterogeneous actor types.
    actors:
        Arc<RwLock<HashMap<ActorPath, Box<dyn Any + Send + Sync + 'static>>>>,
    /// Direct-children index to avoid scanning the full actor registry on lookups.
    child_index: Arc<RwLock<HashMap<ActorPath, HashSet<ActorPath>>>>,

    /// Registry of helper objects that can be shared across actors.
    /// Helpers can be any type (database connections, configurations, etc.).
    helpers: Arc<RwLock<HashMap<String, Box<dyn Any + Send + Sync + 'static>>>>,

    /// Stop senders for root-level actors to enable coordinated shutdown.
    root_senders: Arc<RwLock<HashMap<ActorPath, StopHandle>>>,
    /// Broadcast bus for observable system-level events such as root actor errors.
    system_event_sender: broadcast::Sender<SystemEvent>,

    /// Cancelled by an external signal (SIGTERM, operator). Exit code 0.
    graceful_token: CancellationToken,
    /// Cancelled by an actor on unrecoverable failure. Exit code 1.
    crash_token: CancellationToken,
    /// Set as soon as shutdown begins; blocks creation of new actors.
    shutting_down: Arc<AtomicBool>,
}

impl SystemRef {
    /// Creates a new system reference with shutdown coordination.
    /// This method sets up the actor registry and spawns a task that
    /// listens for cancellation signals to coordinate system shutdown.
    ///
    /// # Arguments
    ///
    /// * `event_sender` - Channel for sending system events to the runner.
    /// * `token` - Cancellation token that triggers system shutdown when cancelled.
    ///
    /// # Returns
    ///
    /// Returns a new SystemRef instance.
    ///
    /// # Behavior
    ///
    /// Spawns a background task that:
    /// - Waits for the cancellation token to be cancelled.
    /// - Stops all root actors in order, waiting for each to confirm.
    /// - Sends a StopSystem event to the runner to complete shutdown.
    ///
    pub(crate) fn new(
        event_sender: mpsc::Sender<SystemEvent>,
        graceful_token: CancellationToken,
        crash_token: CancellationToken,
    ) -> Self {
        let root_senders =
            Arc::new(RwLock::new(HashMap::<ActorPath, StopHandle>::new()));
        let child_index = Arc::new(RwLock::new(HashMap::new()));
        let (system_event_sender, _) = broadcast::channel::<SystemEvent>(256);
        let shutting_down = Arc::new(AtomicBool::new(false));
        let root_sender_clone = root_senders.clone();
        let system_event_sender_clone = system_event_sender.clone();
        let shutting_down_clone = shutting_down.clone();
        let graceful_clone = graceful_token.clone();
        let crash_clone = crash_token.clone();

        tokio::spawn(async move {
            let reason = tokio::select! {
                _ = graceful_clone.cancelled() => ShutdownReason::Graceful,
                _ = crash_clone.cancelled()   => ShutdownReason::Crash,
            };
            shutting_down_clone.store(true, Ordering::SeqCst);
            debug!(reason = ?reason, "Stopping actor system");
            let root_senders = {
                let mut root_senders = root_sender_clone.write().await;
                // Move the senders out while holding the lock, then release it
                // before awaiting on stop notifications.
                std::mem::take(&mut *root_senders)
            };

            // Send all stop signals first so all root actors begin shutdown concurrently.
            let mut receivers = Vec::with_capacity(root_senders.len());
            for (path, handle) in root_senders {
                let (stop_sender, stop_receiver) = oneshot::channel();
                if handle.sender().send(Some(stop_sender)).await.is_ok() {
                    receivers.push((path, handle.timeout(), stop_receiver));
                } else {
                    warn!(path = %path, "Failed to send stop signal to root actor");
                }
            }

            // Wait for all confirmations in parallel.
            for (path, timeout, receiver) in receivers {
                if let Some(timeout) = timeout {
                    if tokio::time::timeout(timeout, receiver).await.is_err() {
                        warn!(
                            path = %path,
                            timeout_ms = timeout.as_millis(),
                            "Timed out waiting for root actor shutdown acknowledgement"
                        );
                    }
                } else {
                    let _ = receiver.await;
                }
            }

            if let Err(e) = event_sender
                .send(SystemEvent::StopSystem(reason.clone()))
                .await
            {
                error!(error = %e, "Failed to send StopSystem event");
            }
            let _ =
                system_event_sender_clone.send(SystemEvent::StopSystem(reason));
        });

        Self {
            actors: Arc::new(RwLock::new(HashMap::new())),
            child_index,
            helpers: Arc::new(RwLock::new(HashMap::new())),
            graceful_token,
            crash_token,
            root_senders,
            system_event_sender,
            shutting_down,
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
            || self.graceful_token.is_cancelled()
            || self.crash_token.is_cancelled()
    }

    /// Subscribes to system-level observable events.
    ///
    /// This is useful for monitoring root actor errors and shutdown events.
    pub fn subscribe_system_events(&self) -> broadcast::Receiver<SystemEvent> {
        self.system_event_sender.subscribe()
    }

    pub(crate) fn publish_system_event(&self, event: SystemEvent) {
        let _ = self.system_event_sender.send(event);
    }

    async fn index_actor(&self, path: &ActorPath) {
        let parent = path.parent();
        self.child_index
            .write()
            .await
            .entry(parent)
            .or_default()
            .insert(path.clone());
    }

    async fn deindex_actor(&self, path: &ActorPath) {
        let parent = path.parent();
        let mut child_index = self.child_index.write().await;
        if let Some(children) = child_index.get_mut(&parent) {
            children.remove(path);
            if children.is_empty() {
                child_index.remove(&parent);
            }
        }
    }

    /// Retrieves an actor running in this actor system. If actor does not exist, a None
    /// is returned instead.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the actor to retrieve.
    ///
    /// # Returns
    ///
    /// Returns the actor reference.
    ///
    pub async fn get_actor<A>(
        &self,
        path: &ActorPath,
    ) -> Result<ActorRef<A>, Error>
    where
        A: Actor + Handler<A>,
    {
        let actors = self.actors.read().await;
        actors
            .get(path)
            .and_then(|any| any.downcast_ref::<ActorRef<A>>().cloned())
            .ok_or_else(|| Error::NotFound { path: path.clone() })
    }

    /// Creates an actor in this actor system with the given path and actor type.
    /// If the actor already exists, an error is returned.
    pub(crate) async fn create_actor_path<A>(
        &self,
        path: ActorPath,
        actor: A,
        parent_error_sender: Option<ChildErrorSender>,
        span: Span,
    ) -> Result<(ActorRef<A>, StopSender), Error>
    where
        A: Actor + Handler<A>,
    {
        if self.is_shutting_down() {
            debug!(path = %path, "Rejecting actor creation during shutdown");
            return Err(Error::SystemStopped);
        }

        // Create the actor runner and init it.
        let system = self.clone();
        let is_root = parent_error_sender.is_none();
        let (mut runner, actor_ref, stop_sender) =
            ActorRunner::create(path.clone(), actor, parent_error_sender);

        // Atomically check+insert under the same write lock to avoid
        // concurrent duplicate creations for the same path.
        {
            let mut actors = self.actors.write().await;
            if actors.contains_key(&path) {
                debug!(path = %path, "Actor already exists");
                return Err(Error::Exists { path });
            }
            actors.insert(path.clone(), Box::new(actor_ref.clone()));
        }
        self.index_actor(&path).await;

        if is_root {
            let mut root_senders = self.root_senders.write().await;
            if self.is_shutting_down() {
                drop(root_senders);
                self.remove_actor(&path).await;
                debug!(path = %path, "Rejecting root actor creation after shutdown started");
                return Err(Error::SystemStopped);
            }
            root_senders.insert(
                path.clone(),
                StopHandle::new(stop_sender.clone(), A::stop_timeout()),
            );
        }

        let (sender, receiver) = oneshot::channel::<bool>();

        let stop_sender_clone = stop_sender.clone();
        let span_clone = span.clone();
        let init_handle = tokio::spawn(
            async move {
                runner
                    .init(system, stop_sender_clone, Some(sender), span_clone)
                    .await;
            }
            .instrument(span),
        );

        let startup_result = match A::startup_timeout() {
            Some(timeout) => tokio::time::timeout(timeout, receiver)
                .await
                .map_err(|_| timeout),
            None => Ok(receiver.await),
        };

        match startup_result {
            Ok(Ok(true)) => {
                debug!(path = %path, "Actor initialized successfully");
                Ok((actor_ref, stop_sender))
            }
            Ok(Ok(false)) => {
                error!(path = %path, "Actor runner failed to initialize");
                self.remove_actor(&path).await;
                if is_root {
                    self.root_senders.write().await.remove(&path);
                }
                Err(Error::FunctionalCritical {
                    description: format!("Runner can not init {}", path),
                })
            }
            Ok(Err(e)) => {
                error!(path = %path, error = %e, "Failed to receive initialization signal");
                self.remove_actor(&path).await;
                if is_root {
                    self.root_senders.write().await.remove(&path);
                }
                Err(Error::FunctionalCritical {
                    description: e.to_string(),
                })
            }
            Err(timeout) => {
                init_handle.abort();
                self.remove_actor(&path).await;
                if is_root {
                    self.root_senders.write().await.remove(&path);
                }
                Err(Error::Timeout {
                    ms: timeout.as_millis(),
                })
            }
        }
    }

    /// Launches a new top level actor on th is actor system at the '/user'
    /// actor path. If another actor with the same name already exists,
    /// an `Err(Error::Exists(ActorPath))` is returned instead.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the actor to create.
    /// * `actor` - The type with `Actor` trait to create.
    /// * `error_helper` - The error helper actor (`None` it it is root actor).
    ///
    /// # Returns
    ///
    /// Returns the actor reference.
    ///
    /// # Error
    ///
    /// Returns an error if the actor already exists.
    ///
    pub async fn create_root_actor<A, I>(
        &self,
        name: &str,
        actor_init: I,
    ) -> Result<ActorRef<A>, Error>
    where
        A: Actor + Handler<A>,
        I: crate::IntoActor<A>,
    {
        let actor = actor_init.into_actor();
        let path = ActorPath::from("/user") / name;
        let id = &path.key();

        let (actor_ref, ..) = self
            .create_actor_path::<A>(
                path.clone(),
                actor,
                None,
                A::get_span(id, None),
            )
            .await?;

        // When this root actor fully terminates on its own, remove its stop
        // sender entry so shutdown only sees live roots.
        let root_senders = self.root_senders.clone();
        let watch = actor_ref.clone();
        let watch_path = path.clone();
        tokio::spawn(async move {
            watch.closed().await;
            root_senders.write().await.remove(&watch_path);
        });

        Ok(actor_ref)
    }

    /// Remove an actor from this actor system.
    /// If the actor does not exist, nothing happens.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the actor to remove.
    ///
    pub(crate) async fn remove_actor(&self, path: &ActorPath) {
        let mut actors = self.actors.write().await;
        let removed = actors.remove(path).is_some();
        drop(actors);
        if removed {
            self.deindex_actor(path).await;
        }
    }

    /// Initiates graceful shutdown for the whole system.
    ///
    /// This cancels the graceful token, stops root actors and eventually makes
    /// [`SystemRunner::run()`] return [`ShutdownReason::Graceful`].
    pub fn stop_system(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.graceful_token.cancel();
    }

    /// Initiates a crash shutdown. Actors call this on unrecoverable failure.
    /// [`SystemRunner::run()`] will return [`ShutdownReason::Crash`] (exit code 1).
    pub fn crash_system(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        self.crash_token.cancel();
    }

    /// Retrieves all direct children of the specified actor.
    /// This scans the actor registry for actors whose parent matches
    /// the given path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the parent actor.
    ///
    /// # Returns
    ///
    /// Returns a vector of ActorPath for all direct children.
    ///
    pub async fn children(&self, path: &ActorPath) -> Vec<ActorPath> {
        self.child_index
            .read()
            .await
            .get(path)
            .into_iter()
            .flat_map(|children| children.iter())
            .cloned()
            .collect()
    }

    /// Adds a helper object to the actor system.
    /// Helpers are shared objects (like database pools, configurations, etc.)
    /// that actors can retrieve by name. This enables dependency injection
    /// for actors without tight coupling.
    ///
    /// # Arguments
    ///
    /// * `name` - Unique identifier for this helper.
    /// * `helper` - The helper object to store (must be Clone + Send + Sync).
    ///
    pub async fn add_helper<H>(&self, name: &str, helper: H)
    where
        H: Any + Send + Sync + Clone + 'static,
    {
        let mut helpers = self.helpers.write().await;
        helpers.insert(name.to_owned(), Box::new(helper));
    }

    /// Retrieves a helper object from the actor system.
    /// Actors can use this to access shared resources like database
    /// connections, configuration, or other services.
    ///
    /// # Arguments
    ///
    /// * `name` - The identifier of the helper to retrieve.
    ///
    /// # Returns
    ///
    /// Returns Some(helper) if found and type matches, None otherwise.
    ///
    pub async fn get_helper<H>(&self, name: &str) -> Option<H>
    where
        H: Any + Send + Sync + Clone + 'static,
    {
        let helpers = self.helpers.read().await;
        helpers
            .get(name)
            .and_then(|any| any.downcast_ref::<H>())
            .cloned()
    }

    /// Spawns a sink to run in a separate task.
    /// Sinks process events emitted by actors and run independently
    /// in their own tasks for concurrent event processing.
    ///
    /// # Arguments
    ///
    /// * `sink` - The sink to run (contains subscriber and event receiver).
    ///
    pub async fn run_sink<E>(&self, mut sink: Sink<E>)
    where
        E: Event,
    {
        tokio::spawn(async move {
            sink.run().await;
        });
    }
}

/// System runner that processes system-wide events.
/// The SystemRunner must be spawned in a task and run to process
/// system lifecycle events like shutdown notifications.
///
pub struct SystemRunner {
    /// Receiver for system-wide events.
    event_receiver: mpsc::Receiver<SystemEvent>,
}

impl SystemRunner {
    /// Creates a new system runner with the given event receiver.
    ///
    /// # Arguments
    ///
    /// * `event_receiver` - Channel receiver for SystemEvent messages.
    ///
    /// # Returns
    ///
    /// Returns a new SystemRunner instance.
    ///
    pub(crate) const fn new(
        event_receiver: mpsc::Receiver<SystemEvent>,
    ) -> Self {
        Self { event_receiver }
    }

    /// Runs the system event loop until the system stops.
    ///
    /// Returns the [`ShutdownReason`] so the caller can choose the exit code:
    ///
    /// ```ignore
    /// let reason = runner.run().await;
    /// std::process::exit(reason.exit_code());
    /// ```
    pub async fn run(&mut self) -> ShutdownReason {
        debug!("Running actor system");
        loop {
            match self.event_receiver.recv().await {
                Some(SystemEvent::StopSystem(reason)) => {
                    debug!(reason = ?reason, "Actor system stopped");
                    return reason;
                }
                Some(SystemEvent::ActorError { path, error }) => {
                    warn!(path = %path, error = %error, "Ignoring observable ActorError on control channel");
                }
                None => {
                    warn!("System event channel closed unexpectedly");
                    return ShutdownReason::Graceful;
                }
            }
        }
    }
}

#[cfg(feature = "test")]
use tracing_subscriber::{
    EnvFilter,
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

#[cfg(feature = "test")]
use std::sync::Once;

#[cfg(feature = "test")]
static INIT: Once = Once::new();

#[cfg(feature = "test")]
#[allow(dead_code)]
pub fn build_tracing_subscriber() {
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"));

        let layer = fmt::layer()
            .with_test_writer()
            .with_span_events(FmtSpan::NONE);

        tracing_subscriber::registry()
            .with(filter)
            .with(layer)
            .try_init()
            .ok();
    });
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_helpers() {
        build_tracing_subscriber();
        let (system, _) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let helper = TestHelper { value: 42 };
        system.add_helper("test", helper).await;
        let helper: Option<TestHelper> = system.get_helper("test").await;
        assert_eq!(helper, Some(TestHelper { value: 42 }));
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct TestHelper {
        pub value: i32,
    }
}
