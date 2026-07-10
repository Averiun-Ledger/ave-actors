//! # Actor runner
//!

use crate::{
    ActorPath, Error,
    actor::{
        Actor, ActorContext, ActorLifecycle, ActorRef, ChildAction, ChildError,
        ChildErrorReceiver, ChildErrorSender, Handler,
        validate_mailbox_capacity, validate_max_timers,
        validate_optional_timeout, validate_timeout,
    },
    handler::{Envelope, HandleHelper, MailboxReceiver, mailbox},
    sink::Sink,
    supervision::{RetryStrategy, SupervisionStrategy},
    system::SystemRef,
};

use dashmap::DashMap;
use std::{
    any::Any,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tracing::{debug, error, warn};

#[cfg(feature = "prometheus")]
fn actor_type_name<A: Actor>() -> Arc<str> {
    let full = std::any::type_name::<A>();
    let short = full
        .rsplit("::")
        .next()
        .unwrap_or(full)
        .split('<')
        .next()
        .unwrap_or(full);
    Arc::from(short)
}

#[cfg(feature = "prometheus")]
fn strategy_label(strategy: &SupervisionStrategy) -> &'static str {
    match strategy {
        SupervisionStrategy::Stop => "Stop",
        SupervisionStrategy::Retry(strategy) => match strategy {
            crate::supervision::Strategy::NoInterval(_) => "NoInterval",
            crate::supervision::Strategy::Interval(_) => "Interval",
            crate::supervision::Strategy::Exponential(_) => "Exponential",
            crate::supervision::Strategy::CustomIntervalStrategy(_) => "Custom",
        },
    }
}

/// Signal received through the actor's stop channel.
pub enum StopSignal {
    /// Normal stop request with optional acknowledgement sender.
    Stop(Option<oneshot::Sender<()>>),
    /// Stop caused by a fatal fault reported to the parent.
    Fault(Box<dyn Any + Send + Sync>, Option<oneshot::Sender<()>>),
}

pub type StopReceiver = mpsc::Receiver<StopSignal>;
pub type StopSender = mpsc::Sender<StopSignal>;

/// Stop channel plus optional acknowledgement timeout for this actor.
#[derive(Clone)]
pub struct StopHandle {
    sender: StopSender,
    timeout: Option<Duration>,
}

impl StopHandle {
    pub const fn new(sender: StopSender, timeout: Option<Duration>) -> Self {
        Self { sender, timeout }
    }

    pub fn sender(&self) -> StopSender {
        self.sender.clone()
    }

    pub const fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

/// Actor runner.
pub struct ActorRunner<A: Actor> {
    path: ActorPath,
    actor: A,
    lifecycle: ActorLifecycle,
    supervision_strategy: SupervisionStrategy,
    receiver: MailboxReceiver<A>,

    // Root actors are stopped by operators/system shutdown; children by their parent.
    stop_receiver: StopReceiver,
    // Shared with children so they can report errors/faults back to this actor.
    error_sender: ChildErrorSender<A::ChildError, A::ChildFault>,
    // Parent information passed by this actor's parent; used by `get_parent` and
    // to report faults.
    parent_info: Option<crate::parent_ref::ParentInfo>,
    // Fault received through StopSignal::Fault; reported to the parent in Failed.
    pending_fault: Option<Box<dyn Any + Send + Sync>>,
    // Receives error/fault notifications from child actors.
    error_receiver: ChildErrorReceiver<A::ChildError, A::ChildFault>,

    stop_signal: bool,
    sinks: Arc<DashMap<String, Sink<A::SinkEvent>>>,
    /// Optional Prometheus metrics collection shared by the actor system.
    #[cfg(feature = "prometheus")]
    metrics: Option<Arc<crate::metrics::ActorMetrics>>,
    /// Cached short actor type name used as a Prometheus label.
    #[cfg(feature = "prometheus")]
    actor_type: Arc<str>,
    /// Cached root scope used as a Prometheus label.
    #[cfg(feature = "prometheus")]
    scope: Arc<str>,
}

impl<A> ActorRunner<A>
where
    A: Actor + Handler<A>,
{
    /// Creates a new actor runner and the actor reference.
    ///
    /// Returns an error if the actor configuration is invalid, for example if
    /// the requested mailbox capacity is outside the allowed range.
    pub(crate) fn create(
        path: ActorPath,
        actor: A,
        parent_info: Option<crate::parent_ref::ParentInfo>,
        stop_channel_size: usize,
        #[cfg(feature = "prometheus")] metrics: Option<
            Arc<crate::metrics::ActorMetrics>,
        >,
    ) -> Result<(Self, ActorRef<A>, StopSender), Error> {
        let mailbox_capacity = A::mailbox_capacity();
        validate_mailbox_capacity(mailbox_capacity)?;
        validate_optional_timeout(
            "actor startup_timeout",
            A::startup_timeout(),
        )?;
        validate_optional_timeout("actor stop_timeout", A::stop_timeout())?;
        validate_timeout(
            "actor mailbox_drain_timeout",
            A::mailbox_drain_timeout(),
        )?;
        validate_timeout(
            "actor event_drain_timeout",
            A::event_drain_timeout(),
        )?;
        validate_max_timers(A::max_timers())?;
        A::supervision_strategy().validate()?;

        #[cfg(feature = "prometheus")]
        let actor_type = actor_type_name::<A>();
        #[cfg(feature = "prometheus")]
        let scope = Arc::from(path.scope_key());

        let (sender, receiver) = mailbox(mailbox_capacity);
        let (stop_sender, stop_receiver) = mpsc::channel(stop_channel_size);
        let (error_sender, error_receiver) =
            crate::parent_ref::child_error_channel(stop_channel_size);
        let helper = HandleHelper::new(
            sender,
            A::mailbox_overflow_strategy(),
            #[cfg(feature = "prometheus")]
            path.clone(),
            #[cfg(feature = "prometheus")]
            metrics.clone(),
        );
        let sinks = Arc::new(DashMap::<String, Sink<A::SinkEvent>>::new());

        #[cfg(feature = "prometheus")]
        let actor_ref = ActorRef::new_with_metrics(
            Arc::new(path.clone()),
            helper,
            stop_sender.clone(),
            sinks.clone(),
            metrics.clone(),
        );
        #[cfg(not(feature = "prometheus"))]
        let actor_ref = ActorRef::new(
            Arc::new(path.clone()),
            helper,
            stop_sender.clone(),
            sinks.clone(),
        );
        let runner: Self = Self {
            path: path.clone(),
            actor,
            lifecycle: ActorLifecycle::Created,
            supervision_strategy: A::supervision_strategy(),
            receiver,
            stop_receiver,
            error_sender,
            parent_info,
            pending_fault: None,
            error_receiver,
            stop_signal: false,
            sinks,
            #[cfg(feature = "prometheus")]
            metrics: metrics.clone(),
            #[cfg(feature = "prometheus")]
            actor_type: Arc::clone(&actor_type),
            #[cfg(feature = "prometheus")]
            scope: Arc::clone(&scope),
        };

        Ok((runner, actor_ref, stop_sender))
    }

    /// Init the actor runner.
    pub(crate) async fn init(
        &mut self,
        system: SystemRef,
        stop_sender: StopSender,
        mut sender: Option<oneshot::Sender<Result<(), Error>>>,
        span: tracing::Span,
    ) {
        let timer_scheduler = crate::timer::TimerScheduler::<A>::new(
            system.clone(),
            self.path.clone(),
            A::max_timers(),
        );
        let spawned_tasks = Arc::new(Mutex::new(Vec::new()));

        // Create the actor context.
        let mut ctx: ActorContext<A> =
            ActorContext::new(crate::actor::ActorContextParams {
                stop: stop_sender,
                path: self.path.clone(),
                system,
                error_sender: self.error_sender.clone(),
                parent_info: self.parent_info.clone(),
                timer_scheduler,
                sinks: self.sinks.clone(),
                spawned_tasks: spawned_tasks.clone(),
                span,
            });

        // Main loop of the actor.
        let mut retries = 0;
        let mut pending_stop_ack: Option<oneshot::Sender<()>> = None;
        loop {
            match self.lifecycle {
                // State: CREATED
                ActorLifecycle::Created => {
                    ctx.timer_scheduler.set_accepting(true);
                    // Pre-start hook.
                    match self.actor.pre_start(&mut ctx).await {
                        Ok(_) => {
                            debug!("Actor started");
                            self.lifecycle = ActorLifecycle::Started;
                        }
                        Err(err) => {
                            error!(error = %err, "Actor failed to start");
                            #[cfg(feature = "prometheus")]
                            if let Some(m) = &self.metrics {
                                m.inc_actor_failed(
                                    &self.path,
                                    Arc::clone(&self.actor_type),
                                    "pre_start",
                                );
                            }
                            ctx.set_startup_error(err);
                            if self.parent_info.is_some() {
                                // Child actor: notify synchronously via the
                                // init oneshot only; the parent already sees
                                // the failure through create_child Err.
                                self.lifecycle = ActorLifecycle::Terminated;
                            } else {
                                self.lifecycle = ActorLifecycle::Failed;
                            }
                        }
                    }
                }
                // State: STARTED
                ActorLifecycle::Started => {
                    if let Some(sender) = sender.take()
                        && let Err(err) = sender.send(Ok(()))
                    {
                        error!(error = ?err, "Failed to send start signal");
                    }
                    #[cfg(feature = "prometheus")]
                    if let Some(m) = &self.metrics {
                        m.inc_actor_active(
                            Arc::clone(&self.scope),
                            Arc::clone(&self.actor_type),
                        );
                    }
                    pending_stop_ack = self.run(&mut ctx).await;
                    #[cfg(feature = "prometheus")]
                    if let Some(m) = &self.metrics {
                        m.dec_actor_active(
                            Arc::clone(&self.scope),
                            Arc::clone(&self.actor_type),
                        );
                    }
                    if self.pending_fault.is_some() {
                        #[cfg(feature = "prometheus")]
                        if let Some(m) = &self.metrics {
                            m.inc_actor_failed(
                                &self.path,
                                Arc::clone(&self.actor_type),
                                "fault",
                            );
                        }
                        self.lifecycle = ActorLifecycle::Failed;
                    }
                }
                // State: RESTARTED
                ActorLifecycle::Restarted => {
                    ctx.timer_scheduler.set_accepting(true);
                    // Apply supervision strategy.
                    self.apply_supervision_strategy(&mut ctx, &mut retries)
                        .await;
                }
                // State: STOPPED
                ActorLifecycle::Stopped => {
                    // Post stop hook.
                    if let Err(e) = self.actor.post_stop(&mut ctx).await {
                        error!(error = %e, "Actor failed post_stop");
                    }
                    if let Some(stop_sender) = pending_stop_ack.take() {
                        let _ = stop_sender.send(());
                    }
                    self.lifecycle = ActorLifecycle::Terminated;
                }
                // State: FAILED
                ActorLifecycle::Failed => {
                    warn!("Actor failed");
                    if self.parent_info.is_none() {
                        self.lifecycle = ActorLifecycle::Restarted;
                    } else {
                        let fault = self.pending_fault.take().unwrap_or_else(
                            || {
                                Box::new(Error::FunctionalCritical {
                                    description: format!(
                                        "Actor '{}' entered Failed without fault context",
                                        self.path
                                    ),
                                }) as Box<dyn Any + Send + Sync>
                            },
                        );

                        if let Some(parent_info) = self.parent_info.as_ref() {
                            match parent_info.notifier.notify_fault(fault).await
                            {
                                Ok(ChildAction::Stop) => {
                                    if let Some(ack) = pending_stop_ack.take() {
                                        let _ = ack.send(());
                                    }
                                    ctx.remove_actor().await;
                                    self.receiver.close();
                                    self.lifecycle = ActorLifecycle::Terminated;
                                }
                                Ok(
                                    ChildAction::Restart
                                    | ChildAction::Delegate,
                                ) => {
                                    debug!("Parent requested actor restart");
                                    self.lifecycle = ActorLifecycle::Restarted;
                                }
                                Err(err) => {
                                    error!(error = %err, "Failed to send fail to parent");
                                    if let Some(ack) = pending_stop_ack.take() {
                                        let _ = ack.send(());
                                    }
                                    ctx.remove_actor().await;
                                    self.receiver.close();
                                    self.lifecycle = ActorLifecycle::Terminated;
                                }
                            }
                        }
                    }
                }
                // State: TERMINATED
                ActorLifecycle::Terminated => {
                    debug!("Actor terminated");
                    ctx.timer_scheduler.shutdown();
                    ctx.abort_spawned_tasks();
                    ctx.system().notify_watchers(&self.path).await;
                    let init_err = ctx.startup_error().unwrap_or_else(|| {
                        Error::FunctionalCritical {
                            description: format!(
                                "Actor '{}' terminated without startup error context",
                                self.path
                            ),
                        }
                    });
                    if let Some(sender) = sender.take()
                        && let Err(err) = sender.send(Err(init_err))
                    {
                        error!(error = ?err, "Failed to send termination signal");
                    }
                    break;
                }
            }
        }
        self.receiver.close();
    }

    /// Runs the actor event loop until a stop signal wins the select.
    pub(crate) async fn run(
        &mut self,
        ctx: &mut ActorContext<A>,
    ) -> Option<oneshot::Sender<()>> {
        self.stop_signal = false;
        let mut stop_ack: Option<oneshot::Sender<()>> = None;
        loop {
            select! {
                biased;

                stop = self.stop_receiver.recv() => {
                    let (ack, is_fault) = match stop {
                        Some(StopSignal::Stop(ack)) => (ack, false),
                        Some(StopSignal::Fault(fault, ack)) => {
                            warn!("Actor received fatal fault, stopping");
                            self.pending_fault = Some(fault);
                            (ack, true)
                        }
                        None => {
                            ctx.stop(None).await;
                            self.stop_signal = true;
                            continue;
                        }
                    };

                    // 1. Cancel pending timers so they cannot fire while the
                    // actor is shutting down or after it restarts.
                    ctx.timer_scheduler.cancel_all();

                    // 2. Abort tasks spawned via ActorContext::spawn so they
                    // do not outlive the actor.
                    ctx.abort_spawned_tasks();

                    // 3. Pre-stop hook.
                    if let Err(e) = self.actor.pre_stop(ctx).await {
                        error!(error = %e, "pre_stop failed");
                    }

                    // 3. Drain mailbox: process critical, discard non-critical.
                    // For a fault we do not close the mailbox yet; the parent may
                    // decide to restart the actor, in which case message delivery
                    // must resume on the same channel.
                    self.drain_mailbox(ctx, !is_fault).await;

                    // 4. Stop children.
                    ctx.stop_children().await;

                    // 5. Drain sinks: give them a grace period to finish
                    // pending events.
                    if !is_fault {
                        self.drain_sinks().await;
                    }

                    // Keep the actor registered while it restarts so lookups by
                    // path and pre-existing ActorRef handles remain valid.
                    if !is_fault {
                        ctx.remove_actor().await;
                    }

                    if let Some(stop_sender) = ack {
                        stop_ack = Some(stop_sender);
                    }

                    if self.lifecycle == ActorLifecycle::Started {
                        self.lifecycle = if is_fault {
                            // Remain Started: init will transition to Failed and
                            // ask the parent for the ChildAction.
                            ActorLifecycle::Started
                        } else {
                            ActorLifecycle::Stopped
                        };
                    }
                    break;
                }
                // Handle an error or fault reported by a child actor.
                error = self.error_receiver.recv(), if !self.stop_signal => {
                    if let Some(error) = error {
                        match error {
                            ChildError::Error { error } => {
                                debug!(error = ?error, "Child error received");
                                self.actor.on_child_error(error, ctx).await
                            },
                            ChildError::Fault { error, sender } => {
                                warn!(error = ?error, "Child fault received");
                                let action = self.actor.on_child_fault(error, ctx).await;
                                if sender.send(action).is_err() {
                                    error!("Failed to send action to child");
                                }
                            },
                            ChildError::ChildStopped(path) => {
                                ctx.remove_closed_child(&path);
                            }
                        }
                    } else {
                        ctx.stop(None).await;
                        self.stop_signal = true;
                    }
                }
                // Receive the next message from the mailbox.
                msg = self.receiver.recv(), if !self.stop_signal => {
                    if let Some(mut envelope) = msg {
                        #[cfg(feature = "prometheus")]
                        let kind = match &envelope {
                            Envelope::Tell { .. } => "tell",
                            Envelope::Ask { .. } => "ask",
                        };
                        #[cfg(feature = "prometheus")]
                        let critical = envelope.is_critical();
                        #[cfg(feature = "prometheus")]
                        let start = std::time::Instant::now();
                        #[cfg(feature = "prometheus")]
                        let queued_at = envelope.queued_at();
                        #[cfg(feature = "prometheus")]
                        let wait_seconds = start
                            .saturating_duration_since(queued_at)
                            .as_secs_f64();
                        #[cfg(feature = "prometheus")]
                        let result =
                            envelope.handle(&mut self.actor, ctx).await;
                        #[cfg(not(feature = "prometheus"))]
                        let _ = envelope.handle(&mut self.actor, ctx).await;
                        #[cfg(feature = "prometheus")]
                        {
                            let duration = start.elapsed().as_secs_f64();
                            let result_label =
                                if result.is_ok() { "ok" } else { "err" };
                            if let Some(m) = &self.metrics {
                                m.inc_messages_processed(
                                    Arc::clone(&self.scope),
                                    Arc::clone(&self.actor_type),
                                    kind,
                                    result_label,
                                );
                                m.observe_message_duration(
                                    Arc::clone(&self.scope),
                                    Arc::clone(&self.actor_type),
                                    kind,
                                    critical,
                                    duration,
                                );
                                m.observe_message_wait(
                                    Arc::clone(&self.scope),
                                    Arc::clone(&self.actor_type),
                                    kind,
                                    critical,
                                    wait_seconds,
                                );
                            }
                        }
                    } else {
                        ctx.stop(None).await;
                        self.stop_signal = true;
                    }
                }
            }
        }
        stop_ack
    }

    /// Drains pending mailbox messages on stop.
    ///
    /// Terminal shutdown closes the receiver first so new sends fail fast. During
    /// restart the receiver stays open, allowing fresh messages to be processed
    /// once the actor is running again.
    async fn drain_mailbox(
        &mut self,
        ctx: &mut ActorContext<A>,
        close_receiver: bool,
    ) {
        if close_receiver {
            self.receiver.close();
        }

        let mut critical: Vec<Envelope<A>> = Vec::new();

        while let Ok(mut msg) = self.receiver.try_recv() {
            if msg.is_critical() {
                critical.push(msg);
            } else {
                msg.respond_stopped();
                #[cfg(feature = "prometheus")]
                if let Some(m) = &self.metrics {
                    m.inc_mailbox_dropped(&self.path, "drain_discard");
                }
            }
        }

        if critical.is_empty() {
            return;
        }

        let deadline = tokio::time::Instant::now() + A::mailbox_drain_timeout();
        let mut timed_out = false;

        for mut msg in critical {
            if timed_out {
                msg.respond_stopped();
                #[cfg(feature = "prometheus")]
                if let Some(m) = &self.metrics {
                    m.inc_mailbox_dropped(&self.path, "drain_timeout");
                }
                continue;
            }

            let remaining =
                deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                warn!(
                    "Drain timeout exceeded, dropping remaining critical messages"
                );
                timed_out = true;
                msg.respond_stopped();
                #[cfg(feature = "prometheus")]
                if let Some(m) = &self.metrics {
                    m.inc_mailbox_dropped(&self.path, "drain_timeout");
                }
                continue;
            }

            #[cfg(feature = "prometheus")]
            let kind = match &msg {
                Envelope::Tell { .. } => "tell",
                Envelope::Ask { .. } => "ask",
            };
            #[cfg(feature = "prometheus")]
            let start = std::time::Instant::now();
            #[cfg(feature = "prometheus")]
            let queued_at = msg.queued_at();
            #[cfg(feature = "prometheus")]
            let wait_seconds =
                start.saturating_duration_since(queued_at).as_secs_f64();

            match tokio::time::timeout(
                remaining,
                msg.handle(&mut self.actor, ctx),
            )
            .await
            {
                Ok(_result) => {
                    #[cfg(feature = "prometheus")]
                    {
                        let duration = start.elapsed().as_secs_f64();
                        let result_label =
                            if _result.is_ok() { "ok" } else { "err" };
                        if let Some(m) = &self.metrics {
                            m.inc_messages_processed(
                                Arc::clone(&self.scope),
                                Arc::clone(&self.actor_type),
                                kind,
                                result_label,
                            );
                            m.observe_message_duration(
                                Arc::clone(&self.scope),
                                Arc::clone(&self.actor_type),
                                kind,
                                true,
                                duration,
                            );
                            m.observe_message_wait(
                                Arc::clone(&self.scope),
                                Arc::clone(&self.actor_type),
                                kind,
                                true,
                                wait_seconds,
                            );
                        }
                    }
                }
                Err(_) => {
                    warn!("Critical message handling timed out");
                    timed_out = true;
                    msg.respond_stopped();
                    #[cfg(feature = "prometheus")]
                    if let Some(m) = &self.metrics {
                        m.inc_mailbox_dropped(&self.path, "drain_timeout");
                    }
                }
            }
        }
    }

    /// Gracefully drain all registered sinks.
    ///
    /// Closes each sink's channel and waits up to
    /// `A::event_drain_timeout()` for the worker to finish pending
    /// events.  All sinks are shut down concurrently; each gets its own
    /// independent deadline so one slow sink does not steal time from
    /// the others.
    async fn drain_sinks(&self) {
        // Collect sinks first so we release DashMap shards before awaiting.
        let sinks: Vec<Sink<A::SinkEvent>> =
            self.sinks.iter().map(|e| e.value().clone()).collect();

        let mut set = tokio::task::JoinSet::new();
        for sink in sinks {
            let deadline = std::time::Instant::now() + A::event_drain_timeout();
            set.spawn(async move {
                if !sink.shutdown(deadline).await {
                    warn!(
                        sink = %sink.name(),
                        timeout_ms = A::event_drain_timeout().as_millis(),
                        "Sink drain timed out, aborting worker"
                    );
                }
            });
        }

        while set.join_next().await.is_some() {}
    }

    /// Apply supervision strategy.
    /// If the actor fails, the strategy is applied.
    ///
    async fn apply_supervision_strategy(
        &mut self,
        ctx: &mut ActorContext<A>,
        retries: &mut usize,
    ) {
        let strategy = std::mem::replace(
            &mut self.supervision_strategy,
            SupervisionStrategy::Stop,
        );
        #[cfg(feature = "prometheus")]
        let restart_strategy_label = strategy_label(&strategy);

        match strategy {
            SupervisionStrategy::Stop => {
                error!("Actor failed, supervision strategy is Stop");
                ctx.remove_actor().await;
                self.lifecycle = ActorLifecycle::Stopped;
                self.supervision_strategy = SupervisionStrategy::Stop;
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
                    match ctx.restart(&mut self.actor).await {
                        Ok(_) => {
                            #[cfg(feature = "prometheus")]
                            if let Some(m) = &self.metrics {
                                m.inc_actor_restarted(
                                    Arc::clone(&self.scope),
                                    Arc::clone(&self.actor_type),
                                    restart_strategy_label,
                                );
                            }
                            self.pending_fault = None;
                            ctx.clean_startup_error();
                            self.lifecycle = ActorLifecycle::Started;
                            *retries = 0;
                            self.supervision_strategy =
                                A::supervision_strategy();
                        }
                        Err(err) => {
                            error!(error = %err, "Actor failed to restart");
                            #[cfg(feature = "prometheus")]
                            if let Some(m) = &self.metrics {
                                m.inc_actor_failed(
                                    &self.path,
                                    Arc::clone(&self.actor_type),
                                    "pre_restart",
                                );
                            }
                            ctx.set_startup_error(err);
                            self.supervision_strategy =
                                SupervisionStrategy::Retry(retry_strategy);
                        }
                    }
                } else {
                    error!(
                        retries = *retries,
                        "Max retries exceeded, stopping actor"
                    );
                    ctx.remove_actor().await;
                    self.lifecycle = ActorLifecycle::Stopped;
                    self.supervision_strategy = A::supervision_strategy();
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[cfg(not(feature = "prometheus"))]
    macro_rules! create_test_runner {
        ($path:expr, $actor:expr, $parent:expr, $stop_size:expr $(,)?) => {
            ActorRunner::create($path, $actor, $parent, $stop_size)
        };
    }

    #[cfg(feature = "prometheus")]
    macro_rules! create_test_runner {
        ($path:expr, $actor:expr, $parent:expr, $stop_size:expr $(,)?) => {
            ActorRunner::create($path, $actor, $parent, $stop_size, None)
        };
    }

    use crate::{
        Error,
        actor::{Actor, ActorContext, Event, Handler, Message},
        supervision::{
            IntervalStrategy, NoIntervalStrategy, Strategy, SupervisionStrategy,
        },
        system::{ActorSystemConfig, SystemRef},
    };
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use test_log::test;

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
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("TestActor", id = %id)
        }

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::Retry(Strategy::Interval(
                IntervalStrategy::new(3, Duration::from_secs(1)),
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
    impl Handler<Self> for TestActor {
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

    #[test(tokio::test)]
    async fn test_actor_root_failed() {
        let (system, _) = SystemRef::new(
            ActorSystemConfig::default(),
            CancellationToken::new(),
            CancellationToken::new(),
        );

        let actor = TestActor { failed: false };
        let (mut runner, actor_ref, stop_sender) = create_test_runner!(
            ActorPath::from("/user/test"),
            actor,
            None,
            ActorSystemConfig::default().actor_stop_channel_size,
        )
        .unwrap();
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
        type SinkEvent = Self::Event;
        type Response = ();
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(id: &str, _parent: Option<tracing::Span>) -> tracing::Span {
            info_span!("DrainActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for DrainActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: DrainMsg,
            _ctx: &mut ActorContext<Self>,
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

    // --- Actor with a very short mailbox_drain_timeout for timeout test ---

    #[derive(Debug, Clone)]
    enum SlowMsg {
        /// Non-critical blocker for setup.
        Block,
        /// Critical but slow (exceeds mailbox_drain_timeout).
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
        type SinkEvent = Self::Event;
        type Response = ();
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(id: &str, _parent: Option<tracing::Span>) -> tracing::Span {
            info_span!("SlowActor", id = %id)
        }

        fn mailbox_drain_timeout() -> Duration {
            Duration::from_millis(50)
        }
    }

    #[async_trait]
    impl Handler<Self> for SlowActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: SlowMsg,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            match msg {
                SlowMsg::Block => {
                    self.started.notify_one();
                    self.release.notified().await;
                }
                SlowMsg::SlowCritical => {
                    // Sleeps well beyond mailbox_drain_timeout (50ms)
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
            }
            Ok(())
        }
    }

    // --- Tests ---

    /// tell/ask to a fully stopped actor must return Error::ActorStopped.
    #[test(tokio::test)]
    async fn test_send_to_stopped_actor_returns_actor_stopped() {
        let (system, _) = SystemRef::new(
            ActorSystemConfig::default(),
            CancellationToken::new(),
            CancellationToken::new(),
        );

        let actor = DrainActor {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            processed: Arc::new(Mutex::new(vec![])),
        };
        let actor_ref =
            system.create_root_actor("stopped", actor).await.unwrap();

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
    #[test(tokio::test)]
    async fn test_drain_critical_processed_normal_stopped() {
        let (system, _) = SystemRef::new(
            ActorSystemConfig::default(),
            CancellationToken::new(),
            CancellationToken::new(),
        );

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
        assert!(
            critical_result.is_ok(),
            "critical message should be processed: {critical_result:?}"
        );

        {
            let done = processed.lock().await;
            assert_eq!(*done, vec!["critical"]);
            drop(done);
        }
    }

    /// When mailbox_drain_timeout expires while processing a slow critical
    /// message, the remaining critical messages are dropped and their ask
    /// callers receive Error::ActorStopped.
    #[test(tokio::test)]
    async fn test_mailbox_drain_timeout_drops_slow_critical() {
        let (system, _) = SystemRef::new(
            ActorSystemConfig::default(),
            CancellationToken::new(),
            CancellationToken::new(),
        );

        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());

        let actor = SlowActor {
            started: started.clone(),
            release: release.clone(),
        };
        let actor_ref = system
            .create_root_actor("slow_timeout", actor)
            .await
            .unwrap();

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

        // mailbox_drain_timeout = 50ms, SlowCritical handler sleeps 300ms
        // -> timeout fires
        let result = slow_join.await.unwrap();
        assert_eq!(result, Err(Error::ActorStopped));
    }

    // Actor that always fails pre_start with Retry strategy (1 retry).
    #[derive(Debug, Clone)]
    struct MaxRetriesActor;

    impl crate::NotPersistentActor for MaxRetriesActor {}

    #[async_trait]
    impl Actor for MaxRetriesActor {
        type Message = ();
        type Response = ();
        type Event = TestEvent;
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("MaxRetriesActor", id = %id)
        }

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::Retry(Strategy::NoInterval(
                NoIntervalStrategy::new(1),
            ))
        }

        async fn pre_start(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Err(Error::FunctionalCritical {
                description: "always fail".to_owned(),
            })
        }
    }

    #[async_trait]
    impl Handler<Self> for MaxRetriesActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: (),
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test(tokio::test)]
    async fn test_max_retries_exceeded() {
        let (system, _) = SystemRef::new(
            ActorSystemConfig::default(),
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let (mut runner, actor_ref, stop_sender) = create_test_runner!(
            ActorPath::from("/user/max_retries"),
            MaxRetriesActor,
            None,
            ActorSystemConfig::default().actor_stop_channel_size,
        )
        .unwrap();
        let inner_system = system.clone();
        let handle = tokio::spawn(async move {
            runner
                .init(
                    inner_system,
                    stop_sender,
                    None,
                    MaxRetriesActor::get_span("id", None),
                )
                .await;
        });
        tokio::time::timeout(Duration::from_secs(2), actor_ref.closed())
            .await
            .expect("actor should stop after max retries exceeded");
        handle.await.unwrap();
    }

    // Actor with Stop supervision that emits fail in pre_start.
    #[derive(Debug, Clone)]
    struct StopStrategyActor;

    impl crate::NotPersistentActor for StopStrategyActor {}

    #[async_trait]
    impl Actor for StopStrategyActor {
        type Message = ();
        type Response = ();
        type Event = TestEvent;
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("StopStrategyActor", id = %id)
        }

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::Stop
        }

        async fn pre_start(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Err(Error::FunctionalCritical {
                description: "fail".to_owned(),
            })
        }
    }

    #[async_trait]
    impl Handler<Self> for StopStrategyActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: (),
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test(tokio::test)]
    async fn test_apply_stop_strategy() {
        let (system, _) = SystemRef::new(
            ActorSystemConfig::default(),
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let (mut runner, actor_ref, stop_sender) = create_test_runner!(
            ActorPath::from("/user/stop_strategy"),
            StopStrategyActor,
            None,
            ActorSystemConfig::default().actor_stop_channel_size,
        )
        .unwrap();
        let inner_system = system.clone();
        let handle = tokio::spawn(async move {
            runner
                .init(
                    inner_system,
                    stop_sender,
                    None,
                    StopStrategyActor::get_span("id", None),
                )
                .await;
        });
        tokio::time::timeout(Duration::from_secs(2), actor_ref.closed())
            .await
            .expect("actor should stop with stop strategy");
        handle.await.unwrap();
    }

    // Simple actor for mailbox-close test.
    #[derive(Debug, Clone)]
    struct SimpleRunningActor;

    impl crate::NotPersistentActor for SimpleRunningActor {}

    #[async_trait]
    impl Actor for SimpleRunningActor {
        type Message = ();
        type Response = ();
        type Event = TestEvent;
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("SimpleRunningActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for SimpleRunningActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: (),
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test(tokio::test)]
    async fn test_mailbox_closed_stops_actor() {
        let (system, _) = SystemRef::new(
            ActorSystemConfig::default(),
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let (mut runner, actor_ref, stop_sender) = create_test_runner!(
            ActorPath::from("/user/simple"),
            SimpleRunningActor,
            None,
            ActorSystemConfig::default().actor_stop_channel_size,
        )
        .unwrap();
        let inner_system = system.clone();
        let handle = tokio::spawn(async move {
            runner
                .init(
                    inner_system,
                    stop_sender,
                    None,
                    SimpleRunningActor::get_span("id", None),
                )
                .await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(actor_ref);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("actor task should finish when mailbox closes")
            .unwrap();
    }

    // ========== pre_restart error path ==========

    #[derive(Debug, Clone)]
    struct PreRestartErrorActor;

    impl crate::NotPersistentActor for PreRestartErrorActor {}

    #[async_trait]
    impl Actor for PreRestartErrorActor {
        type Message = ();
        type Response = ();
        type Event = TestEvent;
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("PreRestartErrorActor", id = %id)
        }

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::Retry(Strategy::NoInterval(
                NoIntervalStrategy::new(3),
            ))
        }

        async fn pre_start(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Err(Error::FunctionalCritical {
                description: "pre_start fail".to_owned(),
            })
        }

        async fn pre_restart(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Err(Error::FunctionalCritical {
                description: "pre_restart fail".to_owned(),
            })
        }
    }

    #[async_trait]
    impl Handler<Self> for PreRestartErrorActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: (),
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test(tokio::test)]
    async fn test_pre_restart_error() {
        let (system, _) = SystemRef::new(
            ActorSystemConfig::default(),
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let (mut runner, actor_ref, stop_sender) = create_test_runner!(
            ActorPath::from("/user/pre_restart_err"),
            PreRestartErrorActor,
            None,
            ActorSystemConfig::default().actor_stop_channel_size,
        )
        .unwrap();
        let inner_system = system.clone();
        let handle = tokio::spawn(async move {
            runner
                .init(
                    inner_system,
                    stop_sender,
                    None,
                    PreRestartErrorActor::get_span("id", None),
                )
                .await;
        });
        tokio::time::timeout(Duration::from_secs(2), actor_ref.closed())
            .await
            .expect("actor should stop after max retries exceeded");
        handle.await.unwrap();
    }

    // ========== Retry strategy success path ==========

    #[derive(Debug, Clone)]
    struct RetryOnceActor {
        failed: Arc<Mutex<bool>>,
    }

    impl crate::NotPersistentActor for RetryOnceActor {}

    #[async_trait]
    impl Actor for RetryOnceActor {
        type Message = ();
        type Response = ();
        type Event = TestEvent;
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("RetryOnceActor", id = %id)
        }

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::Retry(Strategy::NoInterval(
                NoIntervalStrategy::new(3),
            ))
        }

        async fn pre_start(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            {
                let mut guard = self.failed.lock().await;
                if !*guard {
                    return Ok(());
                }
                *guard = false;
            }
            Err(Error::FunctionalCritical {
                description: "fail once".into(),
            })
        }
    }

    #[async_trait]
    impl Handler<Self> for RetryOnceActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: (),
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test(tokio::test)]
    async fn test_apply_retry_strategy_success() {
        let (system, _) = SystemRef::new(
            ActorSystemConfig::default(),
            CancellationToken::new(),
            CancellationToken::new(),
        );
        let actor = RetryOnceActor {
            failed: Arc::new(Mutex::new(true)),
        };
        let (mut runner, actor_ref, stop_sender) = create_test_runner!(
            ActorPath::from("/user/retry_success"),
            actor,
            None,
            ActorSystemConfig::default().actor_stop_channel_size,
        )
        .unwrap();
        let inner_system = system.clone();
        let handle = tokio::spawn(async move {
            runner
                .init(
                    inner_system,
                    stop_sender,
                    None,
                    RetryOnceActor::get_span("id", None),
                )
                .await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        actor_ref.tell_stop().await;

        tokio::time::timeout(Duration::from_secs(2), actor_ref.closed())
            .await
            .expect("actor should stop");
        handle.await.unwrap();
    }
}

#[cfg(all(test, feature = "prometheus"))]
mod prometheus_tests {
    use crate::metrics::{
        ActorActiveLabels, ActorFailureLabels, ActorMetrics,
        ActorRestartLabels, MessageLabels,
    };
    use crate::supervision::{
        NoIntervalStrategy, Strategy, SupervisionStrategy,
    };
    use crate::{
        Actor, ActorContext, ActorPath, ActorSystemConfig, Error, Handler,
        Message, NotPersistentActor, SystemRef, SystemRunner,
    };
    use async_trait::async_trait;
    use prometheus_client::registry::Registry;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use test_log::test;
    use tokio_util::sync::CancellationToken;
    use tracing::info_span;

    #[derive(Debug, Clone)]
    struct Ping;

    impl Message for Ping {}

    #[derive(Debug, Clone)]
    struct MetricsActor;

    impl NotPersistentActor for MetricsActor {}

    #[async_trait]
    impl Actor for MetricsActor {
        type Message = Ping;
        type Response = ();
        type Event = ();
        type SinkEvent = ();
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("MetricsActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for MetricsActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: Ping,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    fn encode(registry: &Registry) -> String {
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, registry)
            .expect("prometheus registry should encode to text");
        buf
    }

    fn system_with_metrics(
        registry: &mut Registry,
    ) -> (
        SystemRef,
        SystemRunner,
        Arc<ActorMetrics>,
        CancellationToken,
        CancellationToken,
    ) {
        let metrics = Arc::new(ActorMetrics::new());
        metrics.register_into(registry);
        let graceful = CancellationToken::new();
        let crash = CancellationToken::new();
        let (mut system, shutdown_complete) = SystemRef::new(
            ActorSystemConfig::default(),
            graceful.clone(),
            crash.clone(),
        );
        system.actor_metrics = Some(metrics.clone());
        let runner = SystemRunner::new(
            graceful.clone(),
            crash.clone(),
            shutdown_complete,
        );
        (system, runner, metrics, graceful, crash)
    }

    #[test(tokio::test)]
    async fn message_metrics_are_emitted() {
        let mut registry = Registry::default();
        let (system, _runner, metrics, _graceful, _crash) =
            system_with_metrics(&mut registry);

        let actor_ref = system
            .create_root_actor("message_metrics", MetricsActor)
            .await
            .expect("root actor should be created");

        actor_ref
            .ask(Ping)
            .await
            .expect("ask should receive a response");
        actor_ref
            .ask_stop()
            .await
            .expect("actor should stop gracefully");

        assert_eq!(
            metrics
                .actor_messages_processed_total
                .get_or_create(&MessageLabels {
                    scope: Arc::from("user"),
                    actor_type: Arc::from("MetricsActor"),
                    kind: "ask",
                    result: "ok",
                })
                .get(),
            1
        );

        let output = encode(&registry);
        assert!(output.contains("ave_actors_actor_messages_processed_total"));
        assert!(output.contains("ave_actors_actor_message_duration_seconds"));
    }

    #[derive(Debug, Clone)]
    struct FailingPreStartActor;

    impl NotPersistentActor for FailingPreStartActor {}

    #[async_trait]
    impl Actor for FailingPreStartActor {
        type Message = ();
        type Response = ();
        type Event = ();
        type SinkEvent = ();
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("FailingPreStartActor", id = %id)
        }

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::Stop
        }

        async fn pre_start(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Err(Error::FunctionalCritical {
                description: "pre_start failure".to_owned(),
            })
        }
    }

    #[async_trait]
    impl Handler<Self> for FailingPreStartActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: (),
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct Crash;

    impl Message for Crash {}

    #[derive(Debug, Clone)]
    struct FailingHandleActor;

    impl NotPersistentActor for FailingHandleActor {}

    #[async_trait]
    impl Actor for FailingHandleActor {
        type Message = Crash;
        type Response = ();
        type Event = ();
        type SinkEvent = ();
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("FailingHandleActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for FailingHandleActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: Crash,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Err(Error::FunctionalCritical {
                description: "handle failure".to_owned(),
            })
        }
    }

    #[test(tokio::test)]
    async fn pre_start_failure_emits_failed_metric() {
        let mut registry = Registry::default();
        let (system, _runner, metrics, _graceful, _crash) =
            system_with_metrics(&mut registry);

        let result = system
            .create_root_actor("pre_start_failure", FailingPreStartActor)
            .await;
        assert!(result.is_err());

        assert_eq!(
            metrics
                .actor_failed_total
                .get_or_create(&ActorFailureLabels {
                    path: "/user/pre_start_failure".to_owned(),
                    actor_type: Arc::from("FailingPreStartActor"),
                    phase: "pre_start",
                })
                .get(),
            1
        );

        let output = encode(&registry);
        assert!(output.contains("ave_actors_actor_failed_total"));
        assert!(output.contains("phase=\"pre_start\""));
    }

    #[test(tokio::test)]
    async fn handle_failure_counts_processed_metric_not_failed_metric() {
        let mut registry = Registry::default();
        let (system, _runner, metrics, _graceful, _crash) =
            system_with_metrics(&mut registry);

        let actor_ref = system
            .create_root_actor("handle_failure", FailingHandleActor)
            .await
            .expect("root actor should be created");

        // The ask will return the error produced by handle_message.
        let _ = actor_ref.ask(Crash).await;
        actor_ref
            .ask_stop()
            .await
            .expect("actor should stop gracefully");

        assert_eq!(
            metrics
                .actor_messages_processed_total
                .get_or_create(&MessageLabels {
                    scope: Arc::from("user"),
                    actor_type: Arc::from("FailingHandleActor"),
                    kind: "ask",
                    result: "err",
                })
                .get(),
            1
        );
        assert_eq!(
            metrics
                .actor_failed_total
                .get_or_create(&ActorFailureLabels {
                    path: "/user/handle_failure".to_owned(),
                    actor_type: Arc::from("FailingHandleActor"),
                    phase: "handle",
                })
                .get(),
            0
        );
    }

    #[derive(Debug, Clone)]
    struct RestartingActor {
        first_attempt: Arc<AtomicBool>,
    }

    impl NotPersistentActor for RestartingActor {}

    #[async_trait]
    impl Actor for RestartingActor {
        type Message = ();
        type Response = ();
        type Event = ();
        type SinkEvent = ();
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("RestartingActor", id = %id)
        }

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::Retry(Strategy::NoInterval(
                NoIntervalStrategy::new(1),
            ))
        }

        async fn pre_start(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            if self.first_attempt.load(Ordering::SeqCst) {
                self.first_attempt.store(false, Ordering::SeqCst);
                Err(Error::FunctionalCritical {
                    description: "first pre_start fails".to_owned(),
                })
            } else {
                Ok(())
            }
        }
    }

    #[async_trait]
    impl Handler<Self> for RestartingActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: (),
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test(tokio::test)]
    async fn actor_restarted_total_is_incremented_on_restart() {
        let mut registry = Registry::default();
        let (system, _runner, metrics, _graceful, _crash) =
            system_with_metrics(&mut registry);

        let actor_ref = system
            .create_root_actor(
                "restarted_actor",
                RestartingActor {
                    first_attempt: Arc::new(AtomicBool::new(true)),
                },
            )
            .await
            .expect("root actor should be created after retry");

        actor_ref
            .ask_stop()
            .await
            .expect("actor should stop gracefully");

        assert_eq!(
            metrics
                .actor_restarted_total
                .get_or_create(&ActorRestartLabels {
                    scope: Arc::from("user"),
                    actor_type: Arc::from("RestartingActor"),
                    strategy: "NoInterval",
                })
                .get(),
            1
        );

        let output = encode(&registry);
        assert!(output.contains("ave_actors_actor_restarted_total"));
        assert!(output.contains("strategy=\"NoInterval\""));
    }

    #[derive(Debug, Clone)]
    struct PreRestartFailingActor;

    impl NotPersistentActor for PreRestartFailingActor {}

    #[async_trait]
    impl Actor for PreRestartFailingActor {
        type Message = ();
        type Response = ();
        type Event = ();
        type SinkEvent = ();
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("PreRestartFailingActor", id = %id)
        }

        fn supervision_strategy() -> SupervisionStrategy {
            SupervisionStrategy::Retry(Strategy::NoInterval(
                NoIntervalStrategy::new(1),
            ))
        }

        async fn pre_start(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Err(Error::FunctionalCritical {
                description: "pre_start fails".to_owned(),
            })
        }

        async fn pre_restart(
            &mut self,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Err(Error::FunctionalCritical {
                description: "pre_restart fails".to_owned(),
            })
        }
    }

    #[async_trait]
    impl Handler<Self> for PreRestartFailingActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: (),
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    #[test(tokio::test)]
    async fn pre_restart_failure_emits_failed_metric() {
        let mut registry = Registry::default();
        let (system, _runner, metrics, _graceful, _crash) =
            system_with_metrics(&mut registry);

        let result = system
            .create_root_actor("pre_restart_failure", PreRestartFailingActor)
            .await;
        assert!(result.is_err());

        assert_eq!(
            metrics
                .actor_failed_total
                .get_or_create(&ActorFailureLabels {
                    path: "/user/pre_restart_failure".to_owned(),
                    actor_type: Arc::from("PreRestartFailingActor"),
                    phase: "pre_start",
                })
                .get(),
            1
        );
        assert_eq!(
            metrics
                .actor_failed_total
                .get_or_create(&ActorFailureLabels {
                    path: "/user/pre_restart_failure".to_owned(),
                    actor_type: Arc::from("PreRestartFailingActor"),
                    phase: "pre_restart",
                })
                .get(),
            1
        );

        let output = encode(&registry);
        assert!(output.contains("ave_actors_actor_failed_total"));
        assert!(output.contains("phase=\"pre_restart\""));
    }

    #[test(tokio::test)]
    async fn actor_active_increments_on_start_and_decrements_on_stop() {
        let mut registry = Registry::default();
        let (system, _runner, metrics, _graceful, _crash) =
            system_with_metrics(&mut registry);

        let actor_ref = system
            .create_root_actor("active_actor", MetricsActor)
            .await
            .expect("root actor should be created");

        assert_eq!(
            metrics
                .actor_active
                .get_or_create(&ActorActiveLabels {
                    scope: Arc::from("user"),
                    actor_type: Arc::from("MetricsActor"),
                })
                .get(),
            1
        );

        actor_ref
            .ask_stop()
            .await
            .expect("actor should stop gracefully");
        actor_ref.closed().await;

        assert_eq!(
            metrics
                .actor_active
                .get_or_create(&ActorActiveLabels {
                    scope: Arc::from("user"),
                    actor_type: Arc::from("MetricsActor"),
                })
                .get(),
            0
        );

        let output = encode(&registry);
        assert!(output.contains("ave_actors_actor_active"));
    }

    #[test(tokio::test)]
    async fn message_wait_metric_is_emitted() {
        let mut registry = Registry::default();
        let (system, _runner, _metrics, _graceful, _crash) =
            system_with_metrics(&mut registry);

        let actor_ref = system
            .create_root_actor("wait_metric", MetricsActor)
            .await
            .expect("root actor should be created");

        actor_ref
            .ask(Ping)
            .await
            .expect("ask should receive a response");
        actor_ref
            .ask_stop()
            .await
            .expect("actor should stop gracefully");

        let output = encode(&registry);
        assert!(output.contains("ave_actors_actor_message_wait_seconds"));
    }
}
