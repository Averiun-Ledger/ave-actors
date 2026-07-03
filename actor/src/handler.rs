use crate::{
    ActorPath, Error, OverflowStrategy,
    actor::{Actor, ActorContext, Handler, Message},
};

#[cfg(feature = "prometheus")]
use std::sync::Arc;
#[cfg(feature = "prometheus")]
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};

use tracing::error;

/// Internal message envelope delivered to an actor's mailbox.
///
/// Uses an enum instead of a trait object to avoid one heap allocation
/// and vtable indirection per message.
pub enum Envelope<A: Actor + Handler<A>> {
    /// Fire-and-forget message.
    Tell {
        /// The actual message to be processed by the actor.
        message: A::Message,
        /// The path of the actor that sent this message.
        sender: ActorPath,
        /// Time when the envelope was placed in the mailbox.
        #[cfg(feature = "prometheus")]
        queued_at: Instant,
    },
    /// Request-response message.
    Ask {
        /// The actual message to be processed by the actor.
        message: A::Message,
        /// The path of the actor that sent this message.
        sender: ActorPath,
        /// Response channel for the ask pattern.
        rsvp: Option<oneshot::Sender<Result<A::Response, Error>>>,
        /// Time when the envelope was placed in the mailbox.
        #[cfg(feature = "prometheus")]
        queued_at: Instant,
    },
}

impl<A: Actor + Handler<A>> Envelope<A> {
    pub fn tell(message: A::Message, sender: ActorPath) -> Self {
        Self::Tell {
            message,
            sender,
            #[cfg(feature = "prometheus")]
            queued_at: Instant::now(),
        }
    }

    pub fn ask(
        message: A::Message,
        sender: ActorPath,
        rsvp: oneshot::Sender<Result<A::Response, Error>>,
    ) -> Self {
        Self::Ask {
            message,
            sender,
            rsvp: Some(rsvp),
            #[cfg(feature = "prometheus")]
            queued_at: Instant::now(),
        }
    }

    #[cfg(feature = "prometheus")]
    pub fn queued_at(&self) -> Instant {
        match self {
            Self::Tell { queued_at, .. } | Self::Ask { queued_at, .. } => {
                *queued_at
            }
        }
    }

    pub fn is_critical(&self) -> bool {
        match self {
            Self::Tell { message, .. } | Self::Ask { message, .. } => {
                message.is_critical()
            }
        }
    }

    pub fn respond_stopped(&mut self) {
        let rsvp = match self {
            Self::Ask { rsvp, .. } => rsvp.take(),
            _ => None,
        };
        let Some(r) = rsvp else { return };
        if r.send(Err(Error::ActorStopped)).is_err() {
            error!("Failed to send ActorStopped response to caller");
        }
    }

    pub async fn handle(
        &mut self,
        actor: &mut A,
        ctx: &mut ActorContext<A>,
    ) -> Result<(), Error> {
        match self {
            Self::Tell {
                message, sender, ..
            } => {
                let message = message.clone();
                let sender = sender.clone();
                actor.handle_message(sender, message, ctx).await.map(|_| ())
            }
            Self::Ask {
                message,
                sender,
                rsvp,
                ..
            } => {
                let message = message.clone();
                let sender = sender.clone();
                let result = actor.handle_message(sender, message, ctx).await;
                let outcome =
                    result.as_ref().map(|_| ()).map_err(|err| err.clone());
                if let Some(r) = rsvp.take()
                    && r.send(result).is_err()
                {
                    error!("Failed to send response back to caller");
                }
                outcome
            }
        }
    }
}

/// Mailbox receiver side for consuming messages from the actor's queue.
pub type MailboxReceiver<A> = mpsc::Receiver<Envelope<A>>;

/// Mailbox sender side for sending messages to an actor's queue.
pub type MailboxSender<A> = mpsc::Sender<Envelope<A>>;

/// Complete mailbox tuple containing both sender and receiver sides.
pub type Mailbox<A> = (MailboxSender<A>, MailboxReceiver<A>);

/// Creates a new mailbox for an actor with the given capacity.
pub fn mailbox<A>(capacity: usize) -> Mailbox<A>
where
    A: Actor + Handler<A>,
{
    mpsc::channel(capacity)
}

/// Handle helper for sending messages to an actor.
pub struct HandleHelper<A>
where
    A: Actor + Handler<A>,
{
    /// The underlying mailbox sender for this actor.
    sender: MailboxSender<A>,
    /// Strategy to apply when the mailbox is full.
    strategy: OverflowStrategy,
    /// The path of the actor this helper targets.
    #[cfg(feature = "prometheus")]
    path: ActorPath,
    /// Optional Prometheus metrics collection shared by the actor system.
    #[cfg(feature = "prometheus")]
    metrics: Option<Arc<crate::metrics::ActorMetrics>>,
}

impl<A> HandleHelper<A>
where
    A: Actor + Handler<A>,
{
    pub(crate) fn new(
        sender: MailboxSender<A>,
        strategy: OverflowStrategy,
        #[cfg(feature = "prometheus")] path: ActorPath,
        #[cfg(feature = "prometheus")] metrics: Option<
            Arc<crate::metrics::ActorMetrics>,
        >,
    ) -> Self {
        Self {
            sender,
            strategy,
            #[cfg(feature = "prometheus")]
            path,
            #[cfg(feature = "prometheus")]
            metrics,
        }
    }

    /// Sends a message to the actor without expecting a response
    /// (fire-and-forget).
    pub(crate) async fn tell(
        &self,
        sender: ActorPath,
        message: A::Message,
    ) -> Result<(), Error> {
        match self.strategy {
            OverflowStrategy::Backpressure => self
                .sender
                .send(Envelope::tell(message, sender))
                .await
                .map_err(|_| Error::ActorStopped),
            OverflowStrategy::DropNewest => {
                match self.sender.try_send(Envelope::tell(message, sender)) {
                    Ok(()) => Ok(()),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        #[cfg(feature = "prometheus")]
                        if let Some(m) = &self.metrics {
                            m.inc_mailbox_dropped(&self.path, "overflow_drop");
                        }
                        tracing::debug!(
                            strategy = ?self.strategy,
                            "Mailbox full, dropping message"
                        );
                        Ok(())
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        #[cfg(feature = "prometheus")]
                        if let Some(m) = &self.metrics {
                            m.inc_mailbox_dropped(&self.path, "closed");
                        }
                        Err(Error::ActorStopped)
                    }
                }
            }
            OverflowStrategy::Fail => {
                match self.sender.try_send(Envelope::tell(message, sender)) {
                    Ok(()) => Ok(()),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        #[cfg(feature = "prometheus")]
                        if let Some(m) = &self.metrics {
                            m.inc_mailbox_full(&self.path);
                        }
                        Err(Error::MailboxFull)
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        #[cfg(feature = "prometheus")]
                        if let Some(m) = &self.metrics {
                            m.inc_mailbox_dropped(&self.path, "closed");
                        }
                        Err(Error::ActorStopped)
                    }
                }
            }
        }
    }

    /// Sends a message to the actor and waits for a response
    /// (request-response).
    pub(crate) async fn ask(
        &self,
        sender: ActorPath,
        message: A::Message,
    ) -> Result<A::Response, Error> {
        // Ask requires a response, so `DropNewest` cannot silently discard the
        // message. Use backpressure for asks under `DropNewest`; only `Fail`
        // returns `MailboxFull` immediately.
        let (response_sender, response_receiver) = oneshot::channel();
        match self.strategy {
            OverflowStrategy::Backpressure | OverflowStrategy::DropNewest => {
                if self
                    .sender
                    .send(Envelope::ask(message, sender, response_sender))
                    .await
                    .is_err()
                {
                    #[cfg(feature = "prometheus")]
                    if let Some(m) = &self.metrics {
                        m.inc_mailbox_dropped(&self.path, "closed");
                    }
                    return Err(Error::ActorStopped);
                }
            }
            OverflowStrategy::Fail => {
                match self.sender.try_send(Envelope::ask(
                    message,
                    sender,
                    response_sender,
                )) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        #[cfg(feature = "prometheus")]
                        if let Some(m) = &self.metrics {
                            m.inc_mailbox_full(&self.path);
                        }
                        return Err(Error::MailboxFull);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        #[cfg(feature = "prometheus")]
                        if let Some(m) = &self.metrics {
                            m.inc_mailbox_dropped(&self.path, "closed");
                        }
                        return Err(Error::ActorStopped);
                    }
                }
            }
        }
        response_receiver.await.map_err(|_| Error::ActorStopped)?
    }

    /// Waits for the sender to be closed.
    pub async fn close(&self) {
        self.sender.closed().await;
    }

    /// Checks if the sender is closed.
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl<A> Clone for HandleHelper<A>
where
    A: Actor + Handler<A>,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            strategy: self.strategy,
            #[cfg(feature = "prometheus")]
            path: self.path.clone(),
            #[cfg(feature = "prometheus")]
            metrics: self.metrics.clone(),
        }
    }
}

#[cfg(all(test, feature = "prometheus"))]
mod prometheus_tests {
    use super::*;
    use crate::{Actor, Handler, NotPersistentActor, metrics::ActorMetrics};
    use async_trait::async_trait;
    use std::sync::Arc;
    use test_log::test;
    use tracing::info_span;

    #[derive(Debug, Clone)]
    struct MetricsTestActor;

    impl NotPersistentActor for MetricsTestActor {}

    #[async_trait]
    impl Actor for MetricsTestActor {
        type Message = ();
        type Event = ();
        type SinkEvent = ();
        type Response = ();
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("MetricsTestActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for MetricsTestActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: (),
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    fn helper_with_strategy(
        strategy: OverflowStrategy,
        metrics: Option<Arc<ActorMetrics>>,
    ) -> (
        HandleHelper<MetricsTestActor>,
        MailboxReceiver<MetricsTestActor>,
    ) {
        let (sender, receiver) = mailbox(1);
        let helper = HandleHelper::new(
            sender,
            strategy,
            ActorPath::from("/test"),
            metrics,
        );
        (helper, receiver)
    }

    #[test(tokio::test)]
    async fn tell_fail_increments_mailbox_full_total() {
        let metrics = Arc::new(ActorMetrics::new());
        let (helper, _receiver) =
            helper_with_strategy(OverflowStrategy::Fail, Some(metrics.clone()));

        // Fill the single-slot mailbox.
        assert!(helper.tell(ActorPath::from("/sender"), ()).await.is_ok());

        // The next tell sees a full mailbox.
        let result = helper.tell(ActorPath::from("/sender"), ()).await;

        assert!(matches!(result, Err(Error::MailboxFull)));
        assert_eq!(metrics.mailbox_full_count(&ActorPath::from("/test")), 1);
        assert_eq!(
            metrics.mailbox_dropped_count(&ActorPath::from("/test"), "closed"),
            0
        );
    }

    #[test(tokio::test)]
    async fn tell_drop_newest_increments_mailbox_dropped_total() {
        let metrics = Arc::new(ActorMetrics::new());
        let (helper, _receiver) = helper_with_strategy(
            OverflowStrategy::DropNewest,
            Some(metrics.clone()),
        );

        // Fill the single-slot mailbox.
        assert!(helper.tell(ActorPath::from("/sender"), ()).await.is_ok());

        // The next tell is silently dropped.
        let result = helper.tell(ActorPath::from("/sender"), ()).await;

        assert!(result.is_ok());
        assert_eq!(
            metrics.mailbox_dropped_count(
                &ActorPath::from("/test"),
                "overflow_drop"
            ),
            1
        );
        assert_eq!(
            metrics.mailbox_dropped_count(&ActorPath::from("/test"), "closed"),
            0
        );
    }

    #[test(tokio::test)]
    async fn ask_fail_increments_mailbox_full_total() {
        let metrics = Arc::new(ActorMetrics::new());
        let (helper, _receiver) =
            helper_with_strategy(OverflowStrategy::Fail, Some(metrics.clone()));

        // Fill the single-slot mailbox with a tell.
        assert!(helper.tell(ActorPath::from("/sender"), ()).await.is_ok());

        // The ask sees a full mailbox and returns immediately.
        let result = helper.ask(ActorPath::from("/sender"), ()).await;

        assert!(matches!(result, Err(Error::MailboxFull)));
        assert_eq!(metrics.mailbox_full_count(&ActorPath::from("/test")), 1);
    }

    #[test(tokio::test)]
    async fn tell_fail_closed_increments_mailbox_dropped_total() {
        let metrics = Arc::new(ActorMetrics::new());
        let (helper, receiver) =
            helper_with_strategy(OverflowStrategy::Fail, Some(metrics.clone()));

        drop(receiver);

        let result = helper.tell(ActorPath::from("/sender"), ()).await;

        assert!(matches!(result, Err(Error::ActorStopped)));
        assert_eq!(
            metrics.mailbox_dropped_count(&ActorPath::from("/test"), "closed"),
            1
        );
    }

    #[test(tokio::test)]
    async fn tell_drop_newest_closed_increments_mailbox_dropped_total() {
        let metrics = Arc::new(ActorMetrics::new());
        let (helper, receiver) = helper_with_strategy(
            OverflowStrategy::DropNewest,
            Some(metrics.clone()),
        );

        drop(receiver);

        let result = helper.tell(ActorPath::from("/sender"), ()).await;

        assert!(matches!(result, Err(Error::ActorStopped)));
        assert_eq!(
            metrics.mailbox_dropped_count(&ActorPath::from("/test"), "closed"),
            1
        );
    }

    #[test(tokio::test)]
    async fn ask_fail_closed_increments_mailbox_dropped_total() {
        let metrics = Arc::new(ActorMetrics::new());
        let (helper, receiver) =
            helper_with_strategy(OverflowStrategy::Fail, Some(metrics.clone()));

        drop(receiver);

        let result = helper.ask(ActorPath::from("/sender"), ()).await;

        assert!(matches!(result, Err(Error::ActorStopped)));
        assert_eq!(
            metrics.mailbox_dropped_count(&ActorPath::from("/test"), "closed"),
            1
        );
    }

    #[test(tokio::test)]
    async fn ask_backpressure_closed_increments_mailbox_dropped_total() {
        let metrics = Arc::new(ActorMetrics::new());
        let (helper, receiver) = helper_with_strategy(
            OverflowStrategy::Backpressure,
            Some(metrics.clone()),
        );

        drop(receiver);

        let result = helper.ask(ActorPath::from("/sender"), ()).await;

        assert!(matches!(result, Err(Error::ActorStopped)));
        assert_eq!(
            metrics.mailbox_dropped_count(&ActorPath::from("/test"), "closed"),
            1
        );
    }
}
