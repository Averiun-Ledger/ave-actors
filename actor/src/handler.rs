use crate::{
    ActorPath, Error,
    actor::{Actor, ActorContext, Handler, Message},
};

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
    },
    /// Request-response message.
    Ask {
        /// The actual message to be processed by the actor.
        message: A::Message,
        /// The path of the actor that sent this message.
        sender: ActorPath,
        /// Response channel for the ask pattern.
        rsvp: Option<oneshot::Sender<Result<A::Response, Error>>>,
    },
}

impl<A: Actor + Handler<A>> Envelope<A> {
    pub const fn tell(message: A::Message, sender: ActorPath) -> Self {
        Self::Tell { message, sender }
    }

    pub const fn ask(
        message: A::Message,
        sender: ActorPath,
        rsvp: oneshot::Sender<Result<A::Response, Error>>,
    ) -> Self {
        Self::Ask {
            message,
            sender,
            rsvp: Some(rsvp),
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

    pub async fn handle(&mut self, actor: &mut A, ctx: &mut ActorContext<A>) {
        match self {
            Self::Tell { message, sender } => {
                let message = message.clone();
                let sender = sender.clone();
                let _ = actor.handle_message(sender, message, ctx).await;
            }
            Self::Ask {
                message,
                sender,
                rsvp,
            } => {
                let message = message.clone();
                let sender = sender.clone();
                let result = actor.handle_message(sender, message, ctx).await;
                if let Some(r) = rsvp.take()
                    && r.send(result).is_err()
                {
                    error!("Failed to send response back to caller");
                }
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

/// Creates a new mailbox for an actor.
pub fn mailbox<A>() -> Mailbox<A>
where
    A: Actor + Handler<A>,
{
    mpsc::channel(1024)
}

/// Handle helper for sending messages to an actor.
pub struct HandleHelper<A>
where
    A: Actor + Handler<A>,
{
    /// The underlying mailbox sender for this actor.
    sender: MailboxSender<A>,
}

impl<A> HandleHelper<A>
where
    A: Actor + Handler<A>,
{
    pub(crate) const fn new(sender: MailboxSender<A>) -> Self {
        Self { sender }
    }

    /// Sends a message to the actor without expecting a response
    /// (fire-and-forget).
    pub(crate) async fn tell(
        &self,
        sender: ActorPath,
        message: A::Message,
    ) -> Result<(), Error> {
        self.sender
            .send(Envelope::tell(message, sender))
            .await
            .map_err(|_| Error::ActorStopped)
    }

    /// Sends a message to the actor and waits for a response
    /// (request-response).
    pub(crate) async fn ask(
        &self,
        sender: ActorPath,
        message: A::Message,
    ) -> Result<A::Response, Error> {
        let (response_sender, response_receiver) = oneshot::channel();
        self.sender
            .send(Envelope::ask(message, sender, response_sender))
            .await
            .map_err(|_| Error::ActorStopped)?;
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
        }
    }
}
