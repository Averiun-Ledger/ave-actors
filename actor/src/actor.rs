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

/// Execution context passed to actors during lifecycle hooks and message handling.
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

    /// Invokes `pre_restart` on the actor; called by the supervision system after a failure.
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
    /// Returns an `ActorRef` to self, or `Error::NotFound` if already removed from the system.
    pub async fn reference(&self) -> Result<ActorRef<A>, Error> {
        self.system.get_actor(&self.path).await
    }

    /// Returns this actor's path.
    pub const fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Returns a reference to the actor system.
    pub const fn system(&self) -> &SystemRef {
        &self.system
    }

    /// Returns the parent actor's ref, or an error if this is a root actor.
    pub async fn get_parent<P: Actor + Handler<P>>(
        &self,
    ) -> Result<ActorRef<P>, Error> {
        self.system.get_actor(&self.path.parent()).await
    }

    /// Sends stop signals to all children concurrently and waits for confirmations.
    /// Respects each child's `stop_timeout()`. Failures and timeouts are ignored.
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

    /// Removes this actor from the system registry.
    pub(crate) async fn remove_actor(&self) {
        self.system.remove_actor(&self.path).await;
    }

    /// Sends a stop signal. Pass `Some(sender)` to await shutdown confirmation.
    pub async fn stop(&self, sender: Option<oneshot::Sender<()>>) {
        let _ = self.stop.send(sender).await;
    }

    /// Broadcasts an event to all subscribers. Not invoked automatically by the runtime.
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

    /// Emits a non-fatal error. Forwarded to the parent via [`Handler::on_child_error`];
    /// root actors emit it as [`SystemEvent::ActorError`].
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

    /// Emits a fatal fault, stops message processing, and escalates to the parent.
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

    /// Creates and registers a child actor at `{current_path}/{name}`.
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

    /// Removes a child stop sender only if it is already closed.
    /// This protects against stale close notifications from a previous child
    /// instance after a same-path child has been recreated.
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

    /// Returns the `ActorRef` for a child actor by name.
    pub async fn get_child<C>(&self, name: &str) -> Result<ActorRef<C>, Error>
    where
        C: Actor + Handler<C>,
    {
        let path = self.path.clone() / name;
        self.system.get_actor(&path).await
    }

    /// Returns the current error state, if any.
    pub(crate) fn error(&self) -> Option<Error> {
        self.error.clone()
    }

    /// Sets the actor's error state, called before emitting a fault.
    pub(crate) fn set_error(&mut self, error: Error) {
        self.error = Some(error);
    }

    /// Clears the actor's error state after a successful restart.
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

/// The `Actor` trait is the main trait that actors must implement.
#[async_trait]
pub trait Actor: Send + Sync + Sized + 'static + Handler<Self> {
    /// The `Message` type is the type of the messages that the actor can receive.
    type Message: Message;

    /// The `Event` type is the type of the events that the actor can emit.
    type Event: Event;

    /// The `Response` type is the type of the response that the actor can give when it receives a
    /// message.
    type Response: Response;

    fn get_span(id: &str, parent_span: Option<Span>) -> tracing::Span;

    /// Maximum time to spend processing critical messages during shutdown.
    /// After this duration, remaining critical messages are also dropped and
    /// their ask callers receive `Error::ActorStopped`.
    /// Default is 5 seconds.
    fn drain_timeout() -> std::time::Duration {
        std::time::Duration::from_secs(5)
    }

    /// Maximum time to wait for `pre_start()` to complete before actor creation
    /// fails. `None` disables the startup timeout.
    fn startup_timeout() -> Option<Duration> {
        None
    }

    /// Maximum time a parent or the system should wait for this actor to
    /// acknowledge a stop request. `None` disables the stop timeout.
    fn stop_timeout() -> Option<Duration> {
        None
    }

    /// Supervision strategy applied when the actor fails at startup. Defaults to `Stop`.
    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Stop
    }

    /// Called when the actor starts. Override to initialize resources.
    async fn pre_start(
        &mut self,
        _context: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Override this function if you want to define what should happen when an
    /// error occurs in [`Actor::pre_start()`]. By default it simply calls
    /// `pre_start()` again, but you can also choose to reinitialize the actor
    /// in some other way.
    async fn pre_restart(
        &mut self,
        ctx: &mut ActorContext<Self>,
        _error: Option<&Error>,
    ) -> Result<(), Error> {
        self.pre_start(ctx).await
    }

    /// Called before stopping the actor. Override to release resources.
    async fn pre_stop(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Called after the actor is fully stopped.
    async fn post_stop(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Ok(())
    }

    /// Optional helper for actors that want to map a handler response into an
    /// event before persisting or publishing it.
    ///
    /// This method is not invoked automatically by the runtime. Call it from
    /// your own actor logic when that mapping is useful.
    fn from_response(_response: Self::Response) -> Result<Self::Event, Error> {
        Err(Error::Functional {
            description: "Not implemented".to_string(),
        })
    }
}

/// Events managed by the actor.
///
/// Events are application-defined values that the actor may persist, pass to
/// `on_event()`, and/or publish through [`ActorContext::publish_event()`].
/// None of those steps happen automatically; the handler decides when they run.
///
/// Note: If you need persistence, the event type must also implement
/// `BorshSerialize` and `BorshDeserialize`.
pub trait Event:
    Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static
{
}

/// Defines what an actor will receive as its message, and with what it should respond.
pub trait Message: Clone + Send + Sync + 'static {
    /// Returns true if this message must be processed before the actor stops.
    /// Non-critical messages will be discarded during shutdown; their ask callers
    /// will receive `Error::ActorStopped`.
    /// Default is false.
    fn is_critical(&self) -> bool {
        false
    }
}

/// Defines the response of a message.
pub trait Response: Send + Sync + 'static {}

impl Response for () {}
impl Event for () {}
impl Message for () {}

/// This is the trait that allows an actor to handle the messages that they receive and,
/// if necessary, respond to them.
#[async_trait]
pub trait Handler<A: Actor + Handler<A>>: Send + Sync {
    /// Processes an incoming message and returns a response.
    async fn handle_message(
        &mut self,
        sender: ActorPath,
        msg: A::Message,
        ctx: &mut ActorContext<A>,
    ) -> Result<A::Response, Error>;

    /// Handles an actor-defined event inside the actor itself.
    ///
    /// This hook is not called automatically by the runtime. Invoke it
    /// explicitly from your actor logic when you want to apply/persist an event
    /// before publishing it or otherwise reacting to it.
    /// By default it does nothing.
    ///
    /// # Arguments
    ///
    /// * `event` - The event to handle.
    /// * `ctx` - The actor context.
    ///
    async fn on_event(&mut self, _event: A::Event, _ctx: &mut ActorContext<A>) {
        // Default implementation.
    }

    /// Called when a child emits a non-fatal error via [`ActorContext::emit_error`].
    async fn on_child_error(
        &mut self,
        error: Error,
        _ctx: &mut ActorContext<A>,
    ) {
        tracing::error!(error = %error, "Child actor error");
    }

    /// Called when a child emits a fatal fault via [`ActorContext::emit_fail`].
    /// Returns the action to apply: `Stop`, `Restart`, or `Delegate` to supervision strategy.
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

/// Typed handle to an actor. Use this to send messages, subscribe to events, or stop the actor.
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

    /// Sends a message without waiting for a response (fire-and-forget).
    pub async fn tell(&self, message: A::Message) -> Result<(), Error> {
        self.sender.tell(self.path(), message).await
    }

    /// Sends a message and waits for the actor's response.
    pub async fn ask(&self, message: A::Message) -> Result<A::Response, Error> {
        self.sender.ask(self.path(), message).await
    }

    /// Asks a message to the actor with a timeout.
    /// If the actor does not respond within the given duration,
    /// returns `Error::Timeout`.
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

    /// Sends a stop signal and waits for the actor to confirm shutdown.
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

    /// Stops the actor without waiting for confirmation (fire-and-forget).
    /// This is a non-blocking alternative to `ask_stop()` that doesn't wait
    /// for the actor to confirm its shutdown.
    ///
    /// # Behavior
    ///
    /// Sends a stop signal with no response channel, allowing the caller to
    /// continue immediately without waiting for the actor to complete its shutdown.
    ///
    pub async fn tell_stop(&self) {
        let _ = self.stop_sender.send(None).await;
    }

    /// Returns the actor's path.
    pub fn path(&self) -> ActorPath {
        self.path.clone()
    }

    /// Returns `true` if the actor's mailbox is closed (actor has stopped).
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Completes when the actor has fully terminated (mailbox receiver dropped).
    pub async fn closed(&self) {
        self.sender.close().await;
    }

    /// Returns a new broadcast receiver subscribed to this actor's event channel.
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

    use crate::{
        
        sink::{Sink, Subscriber},
    };

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
