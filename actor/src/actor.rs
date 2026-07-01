//! Core actor traits, types, and lifecycle hooks.

use crate::{
    ActorPath, Error, ParentRef, TimerKey,
    handler::HandleHelper,
    parent_ref::boxed_notifier,
    runner::{StopHandle, StopSender},
    sink::Sink,
    supervision::SupervisionStrategy,
    system::SystemRef,
    timer::TimerScheduler,
};

use tokio::sync::{mpsc, oneshot};
use tokio::task::{AbortHandle, JoinHandle};

use async_trait::async_trait;

use serde::{Serialize, de::DeserializeOwned};
use tracing::Span;

use dashmap::DashMap;
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Execution context passed to actors during message handling and lifecycle hooks.
///
/// Provides access to the actor's path, child management, event emission,
/// and error reporting. The context is created by the actor system and passed
/// as `&mut ActorContext<A>` to all handler and lifecycle methods.
pub struct ActorContext<A: Actor + Handler<A>> {
    stop: StopSender,
    /// The path of the actor.
    path: ActorPath,
    /// The actor system.
    system: SystemRef,
    /// Startup error from pre_start/pre_restart; used for retry and passed to pre_restart.
    startup_error: Option<Error>,
    /// The error sender to send errors/faults to this actor's children.
    error_sender: ChildErrorSender<A::ChildError, A::ChildFault>,
    /// Parent information passed by this actor's parent; used by `get_parent`.
    parent_info: Option<crate::parent_ref::ParentInfo>,
    /// Scheduler for timers created via `schedule_once` and `schedule`.
    pub(crate) timer_scheduler: TimerScheduler<A>,
    /// Child action senders.
    child_senders: HashMap<ActorPath, StopHandle>,
    /// Named sinks registered for this actor.
    sinks: Arc<DashMap<String, Sink<A::SinkEvent>>>,
    /// Handles of tasks spawned via `ActorContext::spawn`. They are aborted
    /// when the actor stops so spawned work does not outlive the actor.
    spawned_tasks: Arc<Mutex<Vec<AbortHandle>>>,

    span: tracing::Span,
}

/// Parameters needed to build an `ActorContext`. Grouped into a struct to keep
/// the constructor signature readable.
pub struct ActorContextParams<A: Actor + Handler<A>> {
    pub stop: StopSender,
    pub path: ActorPath,
    pub system: SystemRef,
    pub error_sender: ChildErrorSender<A::ChildError, A::ChildFault>,
    pub parent_info: Option<crate::parent_ref::ParentInfo>,
    pub timer_scheduler: TimerScheduler<A>,
    pub sinks: Arc<DashMap<String, Sink<A::SinkEvent>>>,
    pub spawned_tasks: Arc<Mutex<Vec<AbortHandle>>>,
    pub span: Span,
}

impl<A> ActorContext<A>
where
    A: Actor + Handler<A>,
{
    pub(crate) fn new(params: ActorContextParams<A>) -> Self {
        Self {
            span: params.span,
            stop: params.stop,
            path: params.path,
            system: params.system,
            startup_error: None,
            error_sender: params.error_sender,
            parent_info: params.parent_info,
            timer_scheduler: params.timer_scheduler,
            child_senders: HashMap::new(),
            sinks: params.sinks,
            spawned_tasks: params.spawned_tasks,
        }
    }

    pub(crate) async fn restart(&mut self, actor: &mut A) -> Result<(), Error>
    where
        A: Actor,
    {
        tracing::warn!("Actor restarting");
        let result = actor.pre_restart(self).await;
        if let Err(ref e) = result {
            tracing::error!(error = %e, "Actor restart failed");
        }
        result
    }
    /// Returns an `ActorRef` to this actor, or an error if it has already been removed from the system.
    pub async fn reference(&self) -> Result<ActorRef<A>, Error> {
        self.system.get_actor(&self.path).await
    }

    /// Schedules a single message to be sent to this actor after `delay`.
    /// Returns a `TimerKey` that can be used to cancel the timer.
    pub fn schedule_once(&self, delay: Duration, msg: A::Message) -> TimerKey {
        self.timer_scheduler.schedule_once(delay, msg)
    }

    /// Schedules a message to be sent to this actor every `period`.
    /// Returns a `TimerKey` that can be used to cancel the timer.
    pub fn schedule(&self, period: Duration, msg: A::Message) -> TimerKey
    where
        A::Message: Clone,
    {
        self.timer_scheduler.schedule(period, msg)
    }

    /// Cancels a previously scheduled timer.
    pub fn cancel_timer(&self, key: TimerKey) {
        self.timer_scheduler.cancel(key);
    }

    /// Watches `target` and delivers a termination message to this actor when
    /// `target` stops.
    ///
    /// `msg_factory` is called with the terminated actor's path to build the
    /// message that will be sent to this actor. It allows the watcher to model
    /// the notification with its own message type.
    ///
    /// Returns `Error::ActorStopped` if `target` has already stopped.
    pub async fn watch<B, F>(
        &self,
        target: &ActorRef<B>,
        msg_factory: F,
    ) -> Result<(), Error>
    where
        B: Actor + Handler<B>,
        F: Fn(ActorPath) -> A::Message + Send + Sync + 'static,
    {
        if target.is_closed() {
            return Err(Error::ActorStopped);
        }

        let watcher_ref = self.reference().await?;
        let watcher_path = self.path.clone();
        let factory = Arc::new(msg_factory);

        let notify = Arc::new(move |terminated: ActorPath| {
            let actor_ref = watcher_ref.clone();
            let factory = Arc::clone(&factory);
            let watcher = watcher_path.clone();
            tokio::spawn(async move {
                let msg = factory(terminated);
                if let Err(err) = actor_ref.tell(msg).await {
                    tracing::debug!(
                        error = %err,
                        watcher = %watcher,
                        "Failed to deliver termination notification to watcher"
                    );
                }
            });
        });

        self.system
            .watch(target.path(), self.path.clone(), notify)
            .await;
        Ok(())
    }

    /// Stops watching `target` from this actor.
    ///
    /// If this actor was not watching `target`, this is a no-op.
    pub async fn unwatch<B>(&self, target: &ActorRef<B>)
    where
        B: Actor + Handler<B>,
    {
        self.system.unwatch(target.path(), self.path.clone()).await;
    }

    /// Spawns an asynchronous task whose lifetime is bound to this actor.
    ///
    /// The task runs on the Tokio runtime, but it is automatically aborted when
    /// the actor stops or restarts. This is useful for work that must not
    /// outlive the actor, such as sending a delayed message to another actor
    /// or calling an external API.
    ///
    /// The returned `JoinHandle` can be awaited or ignored; the actor will
    /// abort the task on shutdown regardless.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(future);
        let abort_handle = handle.abort_handle();
        let mut tasks = self
            .spawned_tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        tasks.push(abort_handle);
        tasks.retain(|t| !t.is_finished());
        handle
    }

    /// Aborts every task spawned through `ActorContext::spawn`.
    pub(crate) fn abort_spawned_tasks(&self) {
        let mut tasks = self
            .spawned_tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for handle in tasks.drain(..) {
            handle.abort();
        }
    }

    /// Returns the hierarchical path that uniquely identifies this actor in the system.
    pub const fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Returns a reference to the actor system this actor belongs to.
    pub const fn system(&self) -> &SystemRef {
        &self.system
    }

    /// Returns a typed handle to the parent actor, or an error if this is a root
    /// actor or the parent's type does not match `P`.
    pub async fn get_parent<P: Actor + Handler<P>>(
        &self,
    ) -> Result<ParentRef<P>, Error> {
        let parent_info =
            self.parent_info.as_ref().ok_or_else(|| Error::NotFound {
                path: self.path.parent(),
            })?;
        let actor_ref = parent_info
            .actor_ref
            .downcast_ref::<ActorRef<P>>()
            .cloned()
            .ok_or_else(|| Error::NotFound {
                path: self.path.parent(),
            })?;
        let notifier = Arc::clone(&parent_info.notifier);
        Ok(ParentRef::new(actor_ref, notifier, self.stop.clone()))
    }

    pub(crate) async fn stop_children(&mut self) {
        let child_count = self.child_senders.len();
        if child_count > 0 {
            tracing::debug!(child_count, "Stopping child actors");
        }

        // Send all stop signals first so all children begin shutdown concurrently.
        let mut receivers = Vec::with_capacity(child_count);
        for (path, handle) in std::mem::take(&mut self.child_senders) {
            let (stop_sender, stop_receiver) = oneshot::channel();
            if handle
                .sender()
                .send(crate::runner::StopSignal::Stop(Some(stop_sender)))
                .await
                .is_ok()
            {
                receivers.push((path, handle.timeout(), stop_receiver));
            }
        }

        // Wait for all confirmations in parallel.
        let mut set = tokio::task::JoinSet::new();
        for (path, timeout, receiver) in receivers {
            set.spawn(async move {
                if let Some(timeout) = timeout {
                    if tokio::time::timeout(timeout, receiver).await.is_err() {
                        tracing::warn!(
                            child = %path,
                            timeout_ms = timeout.as_millis(),
                            "Timed out waiting for child actor shutdown acknowledgement"
                        );
                    }
                } else {
                    let _ = receiver.await;
                }
            });
        }
        while set.join_next().await.is_some() {}
    }

    pub(crate) async fn remove_actor(&self) {
        self.system.remove_actor(&self.path).await;
    }

    /// Sends a stop signal to this actor. Pass `Some(sender)` to receive a confirmation when shutdown completes.
    pub async fn stop(&self, sender: Option<oneshot::Sender<()>>) {
        let _ = self
            .stop
            .send(crate::runner::StopSignal::Stop(sender))
            .await;
    }

    /// Register a named sink for this actor.
    ///
    /// If a sink with the same name already exists it is replaced and the
    /// previous sink is returned.
    pub fn register_sink(
        &self,
        sink: Sink<A::SinkEvent>,
    ) -> Option<Sink<A::SinkEvent>> {
        self.sinks.insert(sink.name().to_string(), sink)
    }

    /// Remove the sink named `name` and return it, if present.
    pub fn remove_sink(&self, name: &str) -> Option<Sink<A::SinkEvent>> {
        self.sinks.remove(name).map(|(_, v)| v)
    }

    /// Send `event` to the sink named `sink_name`.
    ///
    /// If the sink does not exist a `debug!` log is emitted and the event
    /// is silently dropped (no-op).
    pub fn publish_to(&self, sink_name: impl AsRef<str>, event: A::SinkEvent) {
        let name = sink_name.as_ref();
        if let Some(entry) = self.sinks.get(name) {
            entry.value().send(Arc::new(event));
        } else {
            tracing::debug!(sink = %name, "Sink not found, event dropped");
        }
    }

    /// Send `event` to every registered sink (fire-and-forget).
    pub fn publish_all(&self, event: A::SinkEvent) {
        let event = Arc::new(event);
        for entry in self.sinks.iter() {
            entry.value().send(Arc::clone(&event));
        }
    }

    /// Send `event` to sinks whose name satisfies `predicate`
    /// (fire-and-forget).
    pub fn publish_filtered(
        &self,
        predicate: impl Fn(&str) -> bool,
        event: A::SinkEvent,
    ) {
        let event = Arc::new(event);
        for entry in self.sinks.iter() {
            if predicate(entry.key().as_str()) {
                entry.value().send(Arc::clone(&event));
            }
        }
    }

    /// Spawns a child actor and registers it under this actor's path.
    ///
    /// `name` becomes the last segment of the child's path. Returns an [`ActorRef`]
    /// to the new child on success, or an error if the actor system is shutting down
    /// or a child with the same name already exists.
    pub async fn create_child<C, I>(
        &mut self,
        name: &str,
        actor_init: I,
    ) -> Result<ActorRef<C>, Error>
    where
        C: Actor + Handler<C>,
        I: crate::IntoActor<C>,
    {
        tracing::debug!(child_name = %name, "Creating child actor");
        let actor = actor_init.into_actor();
        let path = self.path.clone() / name;
        let parent_info = crate::parent_ref::ParentInfo {
            actor_ref: Arc::new(self.reference().await?),
            notifier: boxed_notifier(self.error_sender.clone()),
        };
        let result = self
            .system
            .create_actor_path(
                path.clone(),
                actor,
                Some(parent_info),
                C::get_span(name, Some(self.span.clone())),
            )
            .await;

        match result {
            Ok((actor_ref, stop_sender)) => {
                let child_path = path.clone();
                self.child_senders.insert(
                    path,
                    StopHandle::new(stop_sender.clone(), C::stop_timeout()),
                );
                let error_sender = self.error_sender.clone();
                tokio::spawn(async move {
                    stop_sender.closed().await;
                    let _ = error_sender
                        .send(ChildError::ChildStopped(child_path))
                        .await;
                });
                tracing::debug!(child_name = %name, "Child actor created");
                Ok(actor_ref)
            }
            Err(e) => {
                tracing::debug!(child_name = %name, error = %e, "Failed to create child actor");
                Err(e)
            }
        }
    }

    pub(crate) fn remove_closed_child(&mut self, child_path: &ActorPath) {
        let should_remove = self
            .child_senders
            .get(child_path)
            .is_some_and(StopHandle::is_closed);
        if should_remove {
            self.child_senders.remove(child_path);
        }
    }

    /// Looks up a running child actor by its id and returns a typed handle.
    ///
    /// Returns an error if no child with `id` exists or if the child's message
    /// type does not match the requested actor type `C`.
    pub async fn get_child<C>(&self, name: &str) -> Result<ActorRef<C>, Error>
    where
        C: Actor + Handler<C>,
    {
        let path = self.path.clone() / name;
        self.system.get_actor(&path).await
    }

    pub(crate) fn startup_error(&self) -> Option<Error> {
        self.startup_error.clone()
    }

    pub(crate) fn set_startup_error(&mut self, error: Error) {
        self.startup_error = Some(error);
    }

    pub(crate) fn clean_startup_error(&mut self) {
        self.startup_error = None;
    }
}

/// The current lifecycle state of an actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActorLifecycle {
    /// The actor is created.
    Created,
    /// The actor is started.
    Started,
    /// The actor is restarted.
    Restarted,
    /// The actor is failed.
    Failed,
    /// The actor is stopped.
    Stopped,
    /// The actor is terminated.
    Terminated,
}

/// The action that a child actor will take when an error occurs.
#[derive(Debug, Clone)]
pub enum ChildAction {
    /// The child actor will stop.
    Stop,
    /// The child actor will restart.
    Restart,
    /// Delegate the action to the child supervision strategy.
    Delegate,
}

/// Child error receiver.
pub type ChildErrorReceiver<E, F> = mpsc::Receiver<ChildError<E, F>>;

/// Child error sender.
pub type ChildErrorSender<E, F> = mpsc::Sender<ChildError<E, F>>;

/// Message sent from a child to its parent on error, fault, or stop.
pub enum ChildError<E, F> {
    /// Error in child.
    Error {
        /// The error that caused the failure.
        error: E,
    },
    /// Fault in child.
    Fault {
        /// The fault that caused the failure.
        error: F,
        /// The sender will communicate the action to be carried out to the child.
        sender: oneshot::Sender<ChildAction>,
    },
    /// Child actor has stopped.
    ChildStopped(ActorPath),
}

/// Strategy applied when an actor's mailbox reaches its capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowStrategy {
    /// Block the sender until there is room in the mailbox.
    Backpressure,
    /// Silently discard the message being sent.
    ///
    /// This only applies to `tell`; `ask` uses backpressure because it must
    /// return a response to the caller.
    DropNewest,
    /// Return `Error::MailboxFull` to the sender immediately.
    Fail,
}

/// Defines the identity and associated types of an actor.
///
/// Implement this trait together with [`Handler`] on your actor struct.
/// The actor system uses these associated types to wire up message channels,
/// event broadcasts, and tracing spans.
#[async_trait]
pub trait Actor: Send + Sync + Sized + 'static + Handler<Self> {
    /// The type of messages this actor accepts.
    type Message: Message;

    /// The type of events this actor can broadcast to subscribers.
    type Event: Event;

    /// The type of events this actor sends to its sinks.
    type SinkEvent: Event;

    /// The type returned by the actor in response to each message.
    type Response: Response;

    /// The type of errors that children of this actor may report to it.
    type ChildError: Debug + Send + Sync + std::any::Any + 'static;

    /// The type of faults that children of this actor may report to it.
    type ChildFault: Debug
        + Clone
        + From<Error>
        + Send
        + Sync
        + std::any::Any
        + 'static;

    /// Creates the tracing span for this actor instance.
    ///
    /// `id` is the actor's path string; `parent` is the parent actor's span, if any.
    /// Return an `info_span!` or similar to attach all actor logs to this span.
    fn get_span(id: &str, parent_span: Option<Span>) -> tracing::Span;

    /// Maximum time to spend processing critical mailbox messages during
    /// shutdown before dropping them.
    fn mailbox_drain_timeout() -> std::time::Duration {
        std::time::Duration::from_secs(5)
    }

    /// Maximum time to spend draining pending published events during
    /// shutdown before giving up.
    fn event_drain_timeout() -> std::time::Duration {
        std::time::Duration::from_secs(5)
    }

    /// Maximum time to wait for `pre_start` to complete; `None` disables the startup timeout.
    fn startup_timeout() -> Option<Duration> {
        None
    }

    /// Maximum time a parent waits for this actor to acknowledge a stop request; `None` disables the stop timeout.
    fn stop_timeout() -> Option<Duration> {
        None
    }

    /// Returns the supervision strategy applied when this actor fails at startup.
    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Stop
    }

    /// Maximum number of pending timers this actor may have scheduled at once.
    /// Timers created beyond this limit are ignored and logged as a warning.
    fn max_timers() -> usize {
        usize::MAX
    }

    /// Maximum number of messages that can be queued in this actor's mailbox.
    fn mailbox_capacity() -> usize {
        1024
    }

    /// Strategy to apply when the mailbox is full.
    fn mailbox_overflow_strategy() -> OverflowStrategy {
        OverflowStrategy::Backpressure
    }

    /// Called once before the actor begins processing messages.
    ///
    /// Override to initialize resources, spawn child actors, or connect to external
    /// services. Return an error to abort startup; the supervision strategy determines
    /// whether a retry is attempted.
    async fn pre_start(
        &mut self,
        _context: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Called when the actor is about to be restarted after a failure.
    ///
    /// The default implementation delegates to `pre_start`, so any initialization
    /// logic defined there runs again on restart.
    async fn pre_restart(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        self.pre_start(ctx).await
    }

    /// Called when the actor is about to stop, before children are stopped.
    ///
    /// Override to flush state, emit a final event, or notify external services.
    /// Errors are logged but do not prevent the actor from stopping.
    async fn pre_stop(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Called after all children have stopped and the actor is fully shut down. Override for final cleanup.
    async fn post_stop(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Maps a handler response to an event; call explicitly when you need that conversion.
    fn from_response(_response: Self::Response) -> Result<Self::Event, Error> {
        Err(Error::Functional {
            description: "Not implemented".to_string(),
        })
    }
}

/// Application-defined values that an actor may publish, persist, or apply via `on_event`.
pub trait Event:
    Serialize + DeserializeOwned + Debug + Send + Sync + 'static
{
}

/// Defines the type of value an actor receives as a message.
pub trait Message: Clone + Send + Sync + 'static {
    /// Returns `true` if this message must be processed before the actor stops; defaults to `false`.
    fn is_critical(&self) -> bool {
        false
    }
}

/// Defines the type of value an actor returns in response to a message.
pub trait Response: Send + Sync + 'static {}

impl Response for () {}
impl Event for () {}
impl Message for () {}

/// Defines how an actor processes its incoming messages.
///
/// Implement this together with [`Actor`]. The actor system calls
/// `handle_message` for every message delivered to the actor.
#[async_trait]
pub trait Handler<A: Actor + Handler<A>>: Send + Sync {
    /// Processes `msg` sent by `sender` and returns a response.
    ///
    /// `ctx` gives access to the actor's context for spawning children, emitting events,
    /// or reporting errors. Return an error to signal a failure; the error is propagated
    /// back to the caller of [`ActorRef::ask`].
    async fn handle_message(
        &mut self,
        sender: ActorPath,
        msg: A::Message,
        ctx: &mut ActorContext<A>,
    ) -> Result<A::Response, Error>;

    /// Called when the actor wants to apply an event to its own state; not invoked automatically by the runtime.
    async fn on_event(&mut self, _event: A::Event, _ctx: &mut ActorContext<A>) {
        // Default implementation.
    }

    /// Called when a child actor reports an error via its parent's
    /// [`ParentRef::emit_error`].
    ///
    /// Override to inspect `error` and decide whether to escalate it. The default
    /// implementation does nothing.
    async fn on_child_error(
        &mut self,
        error: A::ChildError,
        _ctx: &mut ActorContext<A>,
    ) {
        tracing::error!(error = ?error, "Child actor error");
    }

    /// Called when a child actor fails unrecoverably and reports a fault via its
    /// parent's [`ParentRef::emit_fail`].
    ///
    /// Return [`ChildAction::Stop`] to propagate the failure up to this actor's parent,
    /// [`ChildAction::Restart`] to restart the child, or [`ChildAction::Delegate`]
    /// to let the child's own supervision strategy decide. The default returns `Stop`.
    async fn on_child_fault(
        &mut self,
        error: A::ChildFault,
        _ctx: &mut ActorContext<A>,
    ) -> ChildAction {
        tracing::error!(error = ?error, "Child actor fault, stopping child");
        // Default implementation from child actor errors.
        ChildAction::Stop
    }
}

/// Typed, cloneable handle to a running actor.
///
/// Use this to send messages with [`ask`](ActorRef::ask), subscribe to events
/// with [`subscribe`](ActorRef::subscribe), or stop the actor with
/// [`ask_stop`](ActorRef::ask_stop) or [`tell_stop`](ActorRef::tell_stop).
/// Cloning an `ActorRef` is cheap — all clones share the same underlying channels.
pub struct ActorRef<A>
where
    A: Actor + Handler<A>,
{
    /// The path of the actor.
    path: Arc<ActorPath>,
    /// The handle helper.
    sender: HandleHelper<A>,
    /// The actor stop sender.
    stop_sender: StopSender,
    /// Named sinks registered for this actor.
    sinks: Arc<DashMap<String, Sink<A::SinkEvent>>>,
}

impl<A> ActorRef<A>
where
    A: Actor + Handler<A>,
{
    pub const fn new(
        path: Arc<ActorPath>,
        sender: HandleHelper<A>,
        stop_sender: StopSender,
        sinks: Arc<DashMap<String, Sink<A::SinkEvent>>>,
    ) -> Self {
        Self {
            path,
            sender,
            stop_sender,
            sinks,
        }
    }

    /// Sends a message to the actor without waiting for a response (fire-and-forget).
    pub async fn tell(&self, message: A::Message) -> Result<(), Error> {
        self.sender.tell(self.path(), message).await
    }

    /// Sends `message` to the actor and waits for a response.
    ///
    /// Returns the actor's response on success, or an error if the actor has stopped
    /// or the message channel is full.
    pub async fn ask(&self, message: A::Message) -> Result<A::Response, Error> {
        self.sender.ask(self.path(), message).await
    }

    /// Sends `message` and waits up to `timeout` for a response, returning `Error::Timeout` if the deadline is exceeded.
    pub async fn ask_timeout(
        &self,
        message: A::Message,
        timeout: std::time::Duration,
    ) -> Result<A::Response, Error> {
        tokio::time::timeout(timeout, self.sender.ask(self.path(), message))
            .await
            .map_err(|_| Error::Timeout {
                ms: timeout.as_millis(),
            })?
    }

    /// Requests the actor to stop gracefully and waits for it to confirm shutdown.
    ///
    /// The actor will finish its current message, run `pre_stop` and `post_stop`,
    /// and stop its children before terminating. Returns an error if the actor has
    /// already stopped.
    pub async fn ask_stop(&self) -> Result<(), Error> {
        tracing::debug!("Stopping actor");
        let (response_sender, response_receiver) = oneshot::channel();

        if self
            .stop_sender
            .send(crate::runner::StopSignal::Stop(Some(response_sender)))
            .await
            .is_err()
        {
            Ok(())
        } else {
            response_receiver.await.map_err(|error| {
                tracing::error!(error = %error, "Failed to confirm actor stop");
                Error::Send {
                    reason: error.to_string(),
                }
            })
        }
    }

    /// Sends a stop signal without waiting for the actor to confirm shutdown (fire-and-forget).
    pub async fn tell_stop(&self) {
        let _ = self
            .stop_sender
            .send(crate::runner::StopSignal::Stop(None))
            .await;
    }

    /// Register a sink from external code.
    ///
    /// If a sink with the same name already exists it is replaced and the
    /// previous sink is returned.
    pub fn register_sink(
        &self,
        sink: Sink<A::SinkEvent>,
    ) -> Option<Sink<A::SinkEvent>> {
        self.sinks.insert(sink.name().to_string(), sink)
    }

    /// Remove a sink from external code.
    pub fn remove_sink(&self, name: &str) -> Option<Sink<A::SinkEvent>> {
        self.sinks.remove(name).map(|(_, v)| v)
    }

    /// Returns the hierarchical path of this actor.
    pub fn path(&self) -> ActorPath {
        (*self.path).clone()
    }

    /// Returns `true` if the actor's mailbox is closed, meaning the actor has stopped.
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Waits until the actor has fully terminated.
    pub async fn closed(&self) {
        self.sender.close().await;
    }
}

impl<A> Clone for ActorRef<A>
where
    A: Actor + Handler<A>,
{
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            sender: self.sender.clone(),
            stop_sender: self.stop_sender.clone(),
            sinks: self.sinks.clone(),
        }
    }
}

impl<A> std::fmt::Debug for ActorRef<A>
where
    A: Actor + Handler<A>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRef")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use test_log::test;

    use crate::sink::{Sink, Subscriber};

    use serde::{Deserialize, Serialize};
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;
    use tracing::info_span;

    #[derive(Debug, Clone)]
    struct TestActor {
        counter: usize,
    }

    impl crate::NotPersistentActor for TestActor {}

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestMessage(usize);

    impl Message for TestMessage {}

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestResponse(usize);

    impl Response for TestResponse {}

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestEvent(usize);

    impl Event for TestEvent {}

    #[async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;
        type Event = TestEvent;
        type SinkEvent = Self::Event;
        type Response = TestResponse;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("TestActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for TestActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: TestMessage,
            ctx: &mut ActorContext<Self>,
        ) -> Result<TestResponse, Error> {
            if ctx.get_parent::<Self>().await.is_ok() {
                panic!("Is not a root actor");
            }

            let value = msg.0;
            self.counter += value;
            ctx.publish_all(TestEvent(self.counter));
            Ok(TestResponse(self.counter))
        }
    }

    #[derive(Clone)]
    pub struct TestSubscriber {
        events: Arc<Mutex<Vec<TestEvent>>>,
    }

    impl TestSubscriber {
        pub fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl Subscriber<TestEvent> for TestSubscriber {
        async fn notify(&self, event: Arc<TestEvent>) -> Result<(), Error> {
            assert!(event.0 > 0);
            self.events.lock().await.push((*event).clone());
            Ok(())
        }
    }

    #[test(tokio::test)]
    async fn test_actor() {
        let (system, _) =
            SystemRef::new(CancellationToken::new(), CancellationToken::new());
        let actor = TestActor { counter: 0 };
        let actor_ref = system.create_root_actor("test", actor).await.unwrap();

        let subscriber = TestSubscriber::new();
        let mut sink = Sink::new("test_sink", None);
        sink.add("sub1", subscriber.clone());
        actor_ref.register_sink(sink);

        actor_ref.tell(TestMessage(10)).await.unwrap();
        let response = actor_ref.ask(TestMessage(10)).await.unwrap();
        assert_eq!(response.0, 20);

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        {
            let events = subscriber.events.lock().await;
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].0, 10);
            assert_eq!(events[1].0, 20);
            drop(events);
        }
        actor_ref.ask_stop().await.unwrap();
    }
}
