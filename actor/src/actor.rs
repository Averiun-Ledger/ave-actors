//! Core actor traits, types, and lifecycle hooks.

use crate::{
    ActorPath, Error,
    handler::HandleHelper,
    runner::{InnerAction, InnerSender, StopHandle, StopSender},
    supervision::SupervisionStrategy,
    system::SystemRef,
};

use tokio::sync::{broadcast::Receiver as EventReceiver, mpsc, oneshot};

use async_trait::async_trait;

use serde::{Serialize, de::DeserializeOwned};
use tracing::Span;

use std::{collections::HashMap, fmt::Debug, time::Duration};

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
    /// Error in the actor.
    error: Option<Error>,
    /// The error sender to send errors to the parent.
    error_sender: ChildErrorSender,
    /// Inner sender.
    inner_sender: InnerSender<A>,
    /// Child action senders.
    child_senders: HashMap<ActorPath, StopHandle>,

    span: tracing::Span,
}

impl<A> ActorContext<A>
where
    A: Actor + Handler<A>,
{
    pub(crate) fn new(
        stop: StopSender,
        path: ActorPath,
        system: SystemRef,
        error_sender: ChildErrorSender,
        inner_sender: InnerSender<A>,
        span: Span,
    ) -> Self {
        Self {
            span,
            stop,
            path,
            system,
            error: None,
            error_sender,
            inner_sender,
            child_senders: HashMap::new(),
        }
    }

    pub(crate) async fn restart(
        &mut self,
        actor: &mut A,
        error: Option<&Error>,
    ) -> Result<(), Error>
    where
        A: Actor,
    {
        tracing::warn!(error = ?error, "Actor restarting");
        let result = actor.pre_restart(self, error).await;
        if let Err(ref e) = result {
            tracing::error!(error = %e, "Actor restart failed");
        }
        result
    }
    /// Returns an `ActorRef` to this actor, or an error if it has already been removed from the system.
    pub async fn reference(&self) -> Result<ActorRef<A>, Error> {
        self.system.get_actor(&self.path).await
    }

    /// Returns the hierarchical path that uniquely identifies this actor in the system.
    pub const fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Returns a reference to the actor system this actor belongs to.
    pub const fn system(&self) -> &SystemRef {
        &self.system
    }

    /// Returns a typed handle to the parent actor, or an error if this is a root actor or the parent has stopped.
    pub async fn get_parent<P: Actor + Handler<P>>(
        &self,
    ) -> Result<ActorRef<P>, Error> {
        self.system.get_actor(&self.path.parent()).await
    }

    pub(crate) async fn stop_childs(&mut self) {
        let child_count = self.child_senders.len();
        if child_count > 0 {
            tracing::debug!(child_count, "Stopping child actors");
        }

        // Send all stop signals first so all children begin shutdown concurrently.
        let mut receivers = Vec::with_capacity(child_count);
        for (path, handle) in std::mem::take(&mut self.child_senders) {
            let (stop_sender, stop_receiver) = oneshot::channel();
            if handle.sender().send(Some(stop_sender)).await.is_ok() {
                receivers.push((path, handle.timeout(), stop_receiver));
            }
        }

        // Wait for all confirmations. Children shut down in parallel so the
        // total wait is max(child_shutdown_time) rather than the sum.
        for (path, timeout, receiver) in receivers {
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
        }
    }

    pub(crate) async fn remove_actor(&self) {
        self.system.remove_actor(&self.path).await;
    }

    /// Sends a stop signal to this actor. Pass `Some(sender)` to receive a confirmation when shutdown completes.
    pub async fn stop(&self, sender: Option<oneshot::Sender<()>>) {
        let _ = self.stop.send(sender).await;
    }

    /// Broadcasts `event` to all current subscribers of this actor's event channel.
    ///
    /// Returns an error if the broadcast channel is closed (i.e., the actor is stopping).
    pub async fn publish_event(&self, event: A::Event) -> Result<(), Error> {
        self.inner_sender
            .send(InnerAction::Event(event))
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to publish event");
                Error::SendEvent {
                    reason: e.to_string(),
                }
            })
    }

    /// Reports an error to this actor's parent so the parent can invoke `on_child_error`.
    ///
    /// Returns an error if the parent channel is no longer reachable.
    pub async fn emit_error(&mut self, error: Error) -> Result<(), Error> {
        tracing::warn!(error = %error, "Emitting error");
        self.inner_sender
            .send(InnerAction::Error(error))
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to emit error");
                Error::Send {
                    reason: e.to_string(),
                }
            })
    }

    /// Emits a fatal fault, halts message processing, and escalates to the parent via `on_child_fault`.
    ///
    /// Returns an error if the escalation channel is no longer reachable.
    pub async fn emit_fail(&mut self, error: Error) -> Result<(), Error> {
        tracing::error!(error = %error, "Actor failing");
        // Store error to stop message handling.
        self.set_error(error.clone());
        // Send fail to parent actor.
        self.inner_sender
            .send(InnerAction::Fail(error.clone()))
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "Failed to emit fail");
                Error::Send {
                    reason: e.to_string(),
                }
            })
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
        let result = self
            .system
            .create_actor_path(
                path.clone(),
                actor,
                Some(self.error_sender.clone()),
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
                let inner_sender = self.inner_sender.clone();
                tokio::spawn(async move {
                    stop_sender.closed().await;
                    let _ = inner_sender
                        .send(InnerAction::ChildStopped(child_path))
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
            .map(StopHandle::is_closed)
            .unwrap_or(false);
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

    pub(crate) fn error(&self) -> Option<Error> {
        self.error.clone()
    }

    pub(crate) fn set_error(&mut self, error: Error) {
        self.error = Some(error);
    }

    pub(crate) fn clean_error(&mut self) {
        self.error = None;
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
pub type ChildErrorReceiver = mpsc::Receiver<ChildError>;

/// Child error sender.
pub type ChildErrorSender = mpsc::Sender<ChildError>;

/// Message sent from a child to its parent on error or fault.
pub enum ChildError {
    /// Error in child.
    Error {
        /// The error that caused the failure.
        error: Error,
    },
    /// Fault in child.
    Fault {
        /// The error that caused the failure.
        error: Error,
        /// The sender will communicate the action to be carried out to the child.
        sender: oneshot::Sender<ChildAction>,
    },
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

    /// The type returned by the actor in response to each message.
    type Response: Response;

    /// Creates the tracing span for this actor instance.
    ///
    /// `id` is the actor's path string; `parent` is the parent actor's span, if any.
    /// Return an `info_span!` or similar to attach all actor logs to this span.
    fn get_span(id: &str, parent_span: Option<Span>) -> tracing::Span;

    /// Maximum time to spend processing critical messages during shutdown before dropping them.
    fn drain_timeout() -> std::time::Duration {
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
    /// `error` is the error that caused the restart, or `None` if the restart was manual.
    /// The default implementation delegates to `pre_start`, so any initialization
    /// logic defined there runs again on restart.
    async fn pre_restart(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _error: Option<&Error>,
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
    Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static
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

    /// Called when a child actor reports an error via [`ActorContext::emit_error`].
    ///
    /// Override to inspect `error` and decide whether to escalate it. The default
    /// implementation does nothing.
    async fn on_child_error(
        &mut self,
        error: Error,
        _ctx: &mut ActorContext<A>,
    ) {
        tracing::error!(error = %error, "Child actor error");
    }

    /// Called when a child actor fails unrecoverably (panics or exhausts retries).
    ///
    /// Return [`ChildAction::Stop`] to propagate the failure up to this actor's parent,
    /// [`ChildAction::Restart`] to restart the child, or [`ChildAction::Delegate`]
    /// to let the child's own supervision strategy decide. The default returns `Stop`.
    async fn on_child_fault(
        &mut self,
        error: Error,
        _ctx: &mut ActorContext<A>,
    ) -> ChildAction {
        tracing::error!(error = %error, "Child actor fault, stopping child");
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
    path: ActorPath,
    /// The handle helper.
    sender: HandleHelper<A>,
    /// The actor event receiver.
    event_receiver: EventReceiver<<A as Actor>::Event>,
    /// The actor stop sender.
    stop_sender: StopSender,
}

impl<A> ActorRef<A>
where
    A: Actor + Handler<A>,
{
    pub const fn new(
        path: ActorPath,
        sender: HandleHelper<A>,
        stop_sender: StopSender,
        event_receiver: EventReceiver<<A as Actor>::Event>,
    ) -> Self {
        Self {
            path,
            sender,
            stop_sender,
            event_receiver,
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

        if self.stop_sender.send(Some(response_sender)).await.is_err() {
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
        let _ = self.stop_sender.send(None).await;
    }

    /// Returns the hierarchical path of this actor.
    pub fn path(&self) -> ActorPath {
        self.path.clone()
    }

    /// Returns `true` if the actor's mailbox is closed, meaning the actor has stopped.
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Waits until the actor has fully terminated.
    pub async fn closed(&self) {
        self.sender.close().await;
    }

    /// Returns a broadcast receiver for this actor's events.
    ///
    /// Each subscriber receives every future event independently. Use this receiver
    /// directly or wrap it in a [`Sink`](crate::Sink) to process events asynchronously.
    pub fn subscribe(&self) -> EventReceiver<<A as Actor>::Event> {
        self.event_receiver.resubscribe()
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
            event_receiver: self.event_receiver.resubscribe(),
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use test_log::test;

    use crate::sink::{Sink, Subscriber};

    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc;
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
        type Response = TestResponse;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("TestActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<TestActor> for TestActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: TestMessage,
            ctx: &mut ActorContext<TestActor>,
        ) -> Result<TestResponse, Error> {
            if ctx.get_parent::<TestActor>().await.is_ok() {
                panic!("Is not a root actor");
            }

            let value = msg.0;
            self.counter += value;
            ctx.publish_event(TestEvent(self.counter)).await.unwrap();
            Ok(TestResponse(self.counter))
        }
    }

    pub struct TestSubscriber;

    #[async_trait]
    impl Subscriber<TestEvent> for TestSubscriber {
        async fn notify(&self, event: TestEvent) {
            assert!(event.0 > 0);
        }
    }

    #[test(tokio::test)]
    async fn test_actor() {
        let (event_sender, _event_receiver) = mpsc::channel(100);
        let system = SystemRef::new(
            event_sender,
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let actor = TestActor { counter: 0 };
        let actor_ref = system.create_root_actor("test", actor).await.unwrap();

        let sink = Sink::new(actor_ref.subscribe(), TestSubscriber);
        system.run_sink(sink).await;

        actor_ref.tell(TestMessage(10)).await.unwrap();
        let mut recv = actor_ref.subscribe();
        let response = actor_ref.ask(TestMessage(10)).await.unwrap();
        assert_eq!(response.0, 20);
        let event = recv.recv().await.unwrap();
        assert_eq!(event.0, 10);
        let event = recv.recv().await.unwrap();
        assert_eq!(event.0, 20);
        actor_ref.ask_stop().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
