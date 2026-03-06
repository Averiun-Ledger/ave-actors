//! # Actor runner
//!

use crate::{
    ActorPath,
    Error,
    actor::{
        Actor, ActorContext, ActorLifecycle, ActorRef, ChildAction, ChildError,
        ChildErrorReceiver, ChildErrorSender, Handler,
    },
    //error::{error_box, ErrorBoxReceiver, ErrorHelper, SystemError},
    handler::{BoxedMessageHandler, HandleHelper, MailboxReceiver, mailbox},
    supervision::{RetryStrategy, SupervisionStrategy},
    system::SystemRef,
};

use tokio::{
    select,
    sync::{
        broadcast::{self, Sender as EventSender},
        mpsc, oneshot,
    },
};
use tracing::{debug, error, warn};

/// Inner sender and receiver types.
pub type InnerSender<A> = mpsc::Sender<InnerAction<A>>;
pub type InnerReceiver<A> = mpsc::Receiver<InnerAction<A>>;

pub type StopReceiver = mpsc::Receiver<Option<oneshot::Sender<()>>>;
pub type StopSender = mpsc::Sender<Option<oneshot::Sender<()>>>;

/// Actor runner.
pub struct ActorRunner<A: Actor> {
    path: ActorPath,
    actor: A,
    lifecycle: ActorLifecycle,
    receiver: MailboxReceiver<A>,

    event_sender: EventSender<A::Event>,

    // si es root me para alguien y sino mi padre.
    stop_receiver: StopReceiver,
    // Sender para darselo a mi hijo, por eso lo guardo.
    error_sender: ChildErrorSender,
    // Sender para enviarle mensaje a mi padre en caso de que tenga padre.
    parent_sender: Option<ChildErrorSender>,
    // Escucho los errores de mis hijos.
    error_receiver: ChildErrorReceiver,

    inner_sender: InnerSender<A>,
    inner_receiver: InnerReceiver<A>,
    stop_signal: bool,
}

impl<A> ActorRunner<A>
where
    A: Actor + Handler<A>,
{
    /// Creates a new actor runner and the actor reference.
    pub(crate) fn create(
        path: ActorPath,
        actor: A,
        parent_sender: Option<ChildErrorSender>,
    ) -> (Self, ActorRef<A>, StopSender) {
        let (sender, receiver) = mailbox();
        let (stop_sender, stop_receiver) = mpsc::channel(4);
        let (error_sender, error_receiver) = mpsc::channel(256);
        let (event_sender, event_receiver) = broadcast::channel(1024);
        let (inner_sender, inner_receiver) = mpsc::channel(1024);
        let helper = HandleHelper::new(sender);

        //let error_helper = ErrorHelper::new(error_sender);
        let actor_ref = ActorRef::new(
            path.clone(),
            helper,
            stop_sender.clone(),
            event_receiver,
        );
        let runner: Self = Self {
            path,
            actor,
            lifecycle: ActorLifecycle::Created,
            receiver,
            stop_receiver,
            event_sender,
            error_sender,
            parent_sender,
            error_receiver,
            inner_sender,
            inner_receiver,
            stop_signal: false,
        };
        (runner, actor_ref, stop_sender)
    }

    /// Init the actor runner.
    pub(crate) async fn init(
        &mut self,
        system: SystemRef,
        stop_sender: StopSender,
        mut sender: Option<oneshot::Sender<bool>>,
        span: tracing::Span,
    ) {
        // Create the actor context.
        let mut ctx: ActorContext<A> = ActorContext::new(
            stop_sender,
            self.path.clone(),
            system.clone(),
            self.error_sender.clone(),
            self.inner_sender.clone(),
            span,
        );

        // Main loop of the actor.
        let mut retries = 0;
        loop {
            match self.lifecycle {
                // State: CREATED
                ActorLifecycle::Created => {
                    // Pre-start hook.
                    match self.actor.pre_start(&mut ctx).await {
                        Ok(_) => {
                            debug!("Actor started");
                            self.lifecycle = ActorLifecycle::Started;
                        }
                        Err(err) => {
                            error!(error = %err, "Actor failed to start");
                            ctx.set_error(err);
                            self.lifecycle = ActorLifecycle::Failed;
                        }
                    }
                }
                // State: STARTED
                ActorLifecycle::Started => {
                    if let Some(sender) = sender.take()
                        && let Err(err) = sender.send(true)
                    {
                        error!(error = %err, "Failed to send start signal");
                    }
                    self.run(&mut ctx).await;
                    if ctx.error().is_some() {
                        self.lifecycle = ActorLifecycle::Failed;
                    }
                }
                // State: RESTARTED
                ActorLifecycle::Restarted => {
                    // Apply supervision strategy.
                    self.apply_supervision_strategy(
                        A::supervision_strategy(),
                        &mut ctx,
                        &mut retries,
                    )
                    .await;
                }
                // State: STOPPED
                ActorLifecycle::Stopped => {
                    // Post stop hook.
                    if let Err(e) = self.actor.post_stop(&mut ctx).await {
                        error!(error = %e, "Actor failed post_stop");
                    }
                    self.lifecycle = ActorLifecycle::Terminated;
                }
                // State: FAILED
                ActorLifecycle::Failed => {
                    warn!("Actor failed");
                    if self.parent_sender.is_none() {
                        self.lifecycle = ActorLifecycle::Restarted;
                    } else {
                        // TODO aquí debería decir el padre el qué hacer.
                        self.lifecycle = ActorLifecycle::Terminated;
                    }
                }
                // State: TERMINATED
                ActorLifecycle::Terminated => {
                    debug!("Actor terminated");
                    ctx.system().remove_actor(&self.path.clone()).await;
                    if let Some(sender) = sender.take()
                        && let Err(err) = sender.send(false)
                    {
                        error!(error = %err, "Failed to send termination signal");
                    }
                    break;
                }
            }
        }
        self.receiver.close();
    }

    /// Main loop of the actor.
    /// It runs the actor until the actor is stopped.
    /// The actor runs as long as active references exist. If all references to the actor are
    /// removed or emit `self.token.cancel(), the execution ends.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The actor context.
    ///
    pub(crate) async fn run(&mut self, ctx: &mut ActorContext<A>) {
        loop {
            select! {
                biased;

                stop = self.stop_receiver.recv() => {
                    // 1. Drain mailbox: process critical, discard non-critical.
                    self.drain_mailbox(ctx).await;

                    // 2. Stop children.
                    ctx.stop_childs().await;

                    // 3. Pre-stop hook.
                    if let Err(e) = self.actor.pre_stop(ctx).await {
                        error!(error = %e, "pre_stop failed");
                        let _ = ctx.emit_fail(e).await;
                    }

                    ctx.remove_actor().await;

                    if let Some(Some(stop_sender)) = stop {
                        let _ = stop_sender.send(());
                    }

                    if self.lifecycle == ActorLifecycle::Started {
                        self.lifecycle = ActorLifecycle::Stopped;
                    }
                    break;
                }
                // Handle error from `ErrorBoxReceiver`.
                error = self.error_receiver.recv(), if !self.stop_signal => {
                    if let Some(error) = error {
                        match error {
                            ChildError::Error { error } => {
                                debug!(error = %error, "Child error received");
                                self.actor.on_child_error(error, ctx).await
                            },
                            ChildError::Fault { error, sender } => {
                                warn!(error = %error, "Child fault received");
                                let action = self.actor.on_child_fault(error, ctx).await;
                                if sender.send(action).is_err() {
                                    error!("Failed to send action to child");
                                }
                            },
                        }
                    } else {
                        ctx.stop(None).await;
                        self.stop_signal = true;
                    }
                }
                // Handle inner event from `inner_receiver`.
                recv = self.inner_receiver.recv(), if !self.stop_signal => {
                    if let Some(event) = recv {
                        self.inner_handle(event, ctx).await;
                    } else {
                        ctx.stop(None).await;
                        self.stop_signal = true;
                    }
                }
                // Gets message handler from mailbox receiver and push it to the messages queue.
                msg = self.receiver.recv(), if !self.stop_signal => {
                    if let Some(mut msg) = msg {
                        msg.handle(&mut self.actor, ctx).await;
                    } else {
                        ctx.stop(None).await;
                        self.stop_signal = true;
                    }
                }
            }
        }
    }

    /// Drains pending mailbox messages on shutdown.
    /// Critical messages are processed (with a timeout); non-critical messages
    /// are discarded and their ask callers receive `Error::ActorStopped`.
    ///
    /// The receiver is closed before draining so that any concurrent senders
    /// get an immediate error and no messages can sneak in between the drain
    /// and the channel being dropped at the end of `init()`.
    async fn drain_mailbox(&mut self, ctx: &mut ActorContext<A>) {
        self.receiver.close();

        let mut critical: Vec<BoxedMessageHandler<A>> = Vec::new();

        while let Ok(mut msg) = self.receiver.try_recv() {
            if msg.is_critical() {
                critical.push(msg);
            } else {
                msg.respond_stopped();
            }
        }

        if critical.is_empty() {
            return;
        }

        let deadline = tokio::time::Instant::now() + A::drain_timeout();
        let mut timed_out = false;

        for mut msg in critical {
            if timed_out {
                msg.respond_stopped();
                continue;
            }

            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                warn!("Drain timeout exceeded, dropping remaining critical messages");
                timed_out = true;
                msg.respond_stopped();
                continue;
            }

            if tokio::time::timeout(remaining, msg.handle(&mut self.actor, ctx))
                .await
                .is_err()
            {
                warn!("Critical message handling timed out");
                timed_out = true;
                msg.respond_stopped();
            }
        }
    }

    /// Inner message handler.
    async fn inner_handle(
        &mut self,
        event: InnerAction<A>,
        ctx: &mut ActorContext<A>,
    ) {
        match event {
            InnerAction::Event(event) => {
                if self.event_sender.send(event).is_err() {
                    error!("Failed to broadcast event");
                }
            }
            InnerAction::Error(error) => {
                if let Some(parent_helper) = self.parent_sender.as_mut()
                    && let Err(err) =
                        parent_helper.send(ChildError::Error { error }).await
                {
                    error!(error = %err, "Failed to send error to parent");
                }
            }
            InnerAction::Fail(error) => {
                // If the actor has a parent, send the fail to the parent.
                if let Some(parent_helper) = self.parent_sender.as_mut() {
                    let (action_sender, action_receiver) = oneshot::channel();
                    if let Err(err) = parent_helper
                        .send(ChildError::Fault {
                            error,
                            sender: action_sender,
                        })
                        .await
                    {
                        error!(error = %err, "Failed to send fail to parent");
                    } else {
                        // Sets the state from action.
                        if let Ok(action) = action_receiver.await {
                            ctx.clean_error();
                            match action {
                                ChildAction::Stop => {}
                                ChildAction::Restart
                                | ChildAction::Delegate => {
                                    debug!("Parent requested actor restart");
                                    self.lifecycle = ActorLifecycle::Restarted;
                                }
                            }
                        }
                    }
                }
                ctx.stop(None).await;
                self.stop_signal = true;
            }
        }
    }

    /// Apply supervision strategy.
    /// If the actor fails, the strategy is applied.
    ///
    async fn apply_supervision_strategy(
        &mut self,
        strategy: SupervisionStrategy,
        ctx: &mut ActorContext<A>,
        retries: &mut usize,
    ) {
        match strategy {
            SupervisionStrategy::Stop => {
                error!("Actor failed, supervision strategy is Stop");
                self.lifecycle = ActorLifecycle::Stopped;
            }
            SupervisionStrategy::Retry(mut retry_strategy) => {
                if *retries < retry_strategy.max_retries() {
                    debug!(
                        retries = *retries,
                        max_retries = retry_strategy.max_retries(),
                        "Applying retry strategy"
                    );
                    if let Some(duration) = retry_strategy.next_backoff() {
                        debug!(
                            backoff_ms = duration.as_millis(),
                            "Waiting before retry"
                        );
                        tokio::time::sleep(duration).await;
                    }
                    *retries += 1;
                    let error = ctx.error();
                    match ctx.restart(&mut self.actor, error.as_ref()).await {
                        Ok(_) => {
                            self.lifecycle = ActorLifecycle::Started;
                            *retries = 0;
                        }
                        Err(err) => {
                            ctx.set_error(err);
                        }
                    }
                } else {
                    error!(
                        retries = *retries,
                        "Max retries exceeded, stopping actor"
                    );
                    self.lifecycle = ActorLifecycle::Stopped;
                }
            }
        }
    }
}

/// Inner error.
#[derive(Debug, Clone)]
pub enum InnerAction<A: Actor> {
    /// Event
    Event(A::Event),
    /// Error
    Error(Error),
    /// Fail
    Fail(Error),
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::{
        Error,
        actor::{Actor, ActorContext, Event, Handler, Message},
        build_tracing_subscriber,
        supervision::{FixedIntervalStrategy, Strategy, SupervisionStrategy},
        system::SystemRef,
    };
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};

    use borsh::{BorshDeserialize, BorshSerialize};
    use tokio_util::sync::CancellationToken;
    use tracing::{Instrument, info, info_span};

    use std::time::Duration;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TestMessage(ErrorMessage);

    impl Message for TestMessage {}

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum ErrorMessage {
        Stop,
    }

    #[derive(
        Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
    )]
    pub struct TestEvent;

    impl Event for TestEvent {}

    #[derive(Debug, Clone)]
    pub struct TestActor {
        failed: bool,
    }

    #[async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;
        type Response = ();
        type Event = TestEvent;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("TestActor", id = %id)
        }

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::Retry(Strategy::FixedInterval(
                FixedIntervalStrategy::new(3, Duration::from_secs(1)),
            ))
        }

        async fn pre_start(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            if self.failed {
                Err(Error::FunctionalCritical {
                    description: "PreStart failed".to_owned(),
                })
            } else {
                Ok(())
            }
        }

        async fn pre_restart(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _error: Option<&Error>,
        ) -> Result<(), Error> {
            if self.failed {
                self.failed = false;
            }
            Ok(())
        }

        async fn post_stop(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            debug!("Post stop");
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<TestActor> for TestActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: TestMessage,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            debug!("Handling empty message");
            match msg {
                TestMessage(ErrorMessage::Stop) => {
                    info!("Stopped");
                    ctx.stop(None).await;
                    debug!("Actor stopped");
                }
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_actor_root_failed() {
        build_tracing_subscriber();
        let (event_sender, _) = mpsc::channel(100);

        let system = SystemRef::new(event_sender, CancellationToken::new(), CancellationToken::new());

        let actor = TestActor { failed: false };
        let (mut runner, actor_ref, stop_sender) =
            ActorRunner::create(ActorPath::from("/user/test"), actor, None);
        let inner_system = system.clone();

        // Init the actor runner.
        tokio::spawn(
            async move {
                runner
                    .init(
                        inner_system,
                        stop_sender,
                        None,
                        TestActor::get_span("id", None),
                    )
                    .await;
            }
            .instrument(TestActor::get_span("spawn", None)),
        );
        tokio::time::sleep(Duration::from_secs(1)).await;

        actor_ref
            .tell(TestMessage(ErrorMessage::Stop))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(
            system
                .get_actor::<TestActor>(&ActorPath::from("/user/test"))
                .await
                .is_err()
        );
    }

    // ========== Shutdown drain tests ==========

    use std::sync::Arc;
    use tokio::sync::{Mutex, Notify};

    // --- Shared types for drain tests ---

    #[derive(Debug, Clone)]
    enum DrainMsg {
        /// Blocks processing until `release` is notified.
        Block,
        /// Critical message: processed during drain.
        Critical,
        /// Non-critical message: discarded during drain.
        Normal,
    }

    impl Message for DrainMsg {
        fn is_critical(&self) -> bool {
            matches!(self, Self::Critical)
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DrainEvent;
    impl Event for DrainEvent {}

    struct DrainActor {
        started: Arc<Notify>,
        release: Arc<Notify>,
        processed: Arc<Mutex<Vec<&'static str>>>,
    }

    impl crate::NotPersistentActor for DrainActor {}

    impl Actor for DrainActor {
        type Message = DrainMsg;
        type Event = DrainEvent;
        type Response = ();

        fn get_span(id: &str, _parent: Option<tracing::Span>) -> tracing::Span {
            info_span!("DrainActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<DrainActor> for DrainActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: DrainMsg,
            _ctx: &mut ActorContext<DrainActor>,
        ) -> Result<(), Error> {
            match msg {
                DrainMsg::Block => {
                    self.started.notify_one();
                    self.release.notified().await;
                }
                DrainMsg::Critical => {
                    self.processed.lock().await.push("critical");
                }
                DrainMsg::Normal => {
                    self.processed.lock().await.push("normal");
                }
            }
            Ok(())
        }
    }

    // --- Actor with a very short drain_timeout for timeout test ---

    #[derive(Debug, Clone)]
    enum SlowMsg {
        /// Non-critical blocker for setup.
        Block,
        /// Critical but slow (exceeds drain_timeout).
        SlowCritical,
    }

    impl Message for SlowMsg {
        fn is_critical(&self) -> bool {
            matches!(self, Self::SlowCritical)
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct SlowEvent;
    impl Event for SlowEvent {}

    struct SlowActor {
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl crate::NotPersistentActor for SlowActor {}

    impl Actor for SlowActor {
        type Message = SlowMsg;
        type Event = SlowEvent;
        type Response = ();

        fn get_span(id: &str, _parent: Option<tracing::Span>) -> tracing::Span {
            info_span!("SlowActor", id = %id)
        }

        fn drain_timeout() -> Duration {
            Duration::from_millis(50)
        }
    }

    #[async_trait]
    impl Handler<SlowActor> for SlowActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: SlowMsg,
            _ctx: &mut ActorContext<SlowActor>,
        ) -> Result<(), Error> {
            match msg {
                SlowMsg::Block => {
                    self.started.notify_one();
                    self.release.notified().await;
                }
                SlowMsg::SlowCritical => {
                    // Sleeps well beyond drain_timeout (50ms)
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }
            Ok(())
        }
    }

    // --- Tests ---

    /// tell/ask to a fully stopped actor must return Error::ActorStopped.
    #[tokio::test]
    async fn test_send_to_stopped_actor_returns_actor_stopped() {
        build_tracing_subscriber();
        let (tx, _rx) = tokio::sync::mpsc::channel(100);
        let system = SystemRef::new(tx, CancellationToken::new(), CancellationToken::new());

        let actor = DrainActor {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            processed: Arc::new(Mutex::new(vec![])),
        };
        let actor_ref = system.create_root_actor("stopped", actor).await.unwrap();

        // ask_stop waits for the actor to confirm shutdown, so the channel is
        // already closed when this returns.
        actor_ref.ask_stop().await.unwrap();

        assert_eq!(
            actor_ref.tell(DrainMsg::Normal).await,
            Err(Error::ActorStopped)
        );
        assert_eq!(
            actor_ref.ask(DrainMsg::Normal).await,
            Err(Error::ActorStopped)
        );
    }

    /// During shutdown drain: critical messages are processed, non-critical ask
    /// callers receive Error::ActorStopped.
    ///
    /// Setup:
    ///  1. Block the actor (it's busy, stop signal won't be seen yet).
    ///  2. Queue a Normal ask and a Critical ask into the mailbox.
    ///  3. Send the stop signal.
    ///  4. Release the block → actor finishes, biased select picks stop →
    ///     drain runs → Normal discarded, Critical processed.
    #[tokio::test]
    async fn test_drain_critical_processed_normal_stopped() {
        build_tracing_subscriber();
        let (tx, _rx) = tokio::sync::mpsc::channel(100);
        let system = SystemRef::new(tx, CancellationToken::new(), CancellationToken::new());

        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let processed = Arc::new(Mutex::new(vec![]));

        let actor = DrainActor {
            started: started.clone(),
            release: release.clone(),
            processed: processed.clone(),
        };
        let actor_ref = system.create_root_actor("drain", actor).await.unwrap();

        // Step 1: block the actor
        actor_ref.tell(DrainMsg::Block).await.unwrap();
        started.notified().await; // wait until actor is inside the Block handler

        // Step 2: queue Normal and Critical asks concurrently
        let normal_join = tokio::spawn({
            let r = actor_ref.clone();
            async move { r.ask(DrainMsg::Normal).await }
        });
        let critical_join = tokio::spawn({
            let r = actor_ref.clone();
            async move { r.ask(DrainMsg::Critical).await }
        });

        // Give the spawned tasks time to place their messages in the mailbox.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Step 3: send stop signal (non-blocking, no confirmation wait)
        actor_ref.tell_stop().await;

        // Step 4: release the block
        release.notify_one();

        let normal_result = normal_join.await.unwrap();
        let critical_result = critical_join.await.unwrap();

        assert_eq!(normal_result, Err(Error::ActorStopped));
        assert!(critical_result.is_ok(), "critical message should be processed: {critical_result:?}");

        let done = processed.lock().await;
        assert_eq!(*done, vec!["critical"]);
    }

    /// When drain_timeout expires while processing a slow critical message, the
    /// remaining critical messages are dropped and their ask callers receive
    /// Error::ActorStopped.
    #[tokio::test]
    async fn test_drain_timeout_drops_slow_critical() {
        build_tracing_subscriber();
        let (tx, _rx) = tokio::sync::mpsc::channel(100);
        let system = SystemRef::new(tx, CancellationToken::new(), CancellationToken::new());

        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let actor = SlowActor {
            started: started.clone(),
            release: release.clone(),
        };
        let actor_ref = system.create_root_actor("slow_timeout", actor).await.unwrap();

        // Block the actor so we can queue the slow critical before stop runs
        actor_ref.tell(SlowMsg::Block).await.unwrap();
        started.notified().await;

        // Queue the slow critical ask while actor is blocked
        let slow_join = tokio::spawn({
            let r = actor_ref.clone();
            async move { r.ask(SlowMsg::SlowCritical).await }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        actor_ref.tell_stop().await;
        release.notify_one();

        // drain_timeout = 50ms, SlowCritical handler sleeps 300ms → timeout fires
        let result = slow_join.await.unwrap();
        assert_eq!(result, Err(Error::ActorStopped));
    }
}
