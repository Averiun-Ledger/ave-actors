//! Parent reference for child-to-parent error escalation.
//!
//! Only a child actor can obtain a [`ParentRef`] to its parent. The reference is
//! typed with the parent's actor type, so the child can send ordinary messages
//! and escalate errors/faults that match the types the parent declared in its
//! [`Actor::ChildError`] and [`Actor::ChildFault`] associated types.

use async_trait::async_trait;

use std::{any::Any, fmt::Debug, sync::Arc};

use tokio::sync::{mpsc, oneshot};

use crate::{
    Actor, ActorRef, ChildAction, Error, Handler,
    actor::{ChildError, ChildErrorReceiver, ChildErrorSender},
};

/// Notifier used internally by the framework to send child errors/faults to a
/// parent actor whose concrete type is not known at the call site.
#[async_trait]
pub trait ParentNotifier: Send + Sync {
    /// Sends a non-fatal error to the parent.
    async fn notify_error(
        &self,
        error: Box<dyn Any + Send>,
    ) -> Result<(), Error>;

    /// Sends a fatal fault to the parent and returns the parent's decision.
    async fn notify_fault(
        &self,
        fault: Box<dyn Any + Send>,
    ) -> Result<ChildAction, Error>;
}

/// Parent information passed from a parent actor to its child.
///
/// Kept type-erased so the child runner does not depend on the concrete parent
/// type, while still allowing the child to obtain a typed `ParentRef<P>` when
/// it knows the parent's type.
#[derive(Clone)]
pub struct ParentInfo {
    /// Type-erased `ActorRef<P>` to the parent.
    pub actor_ref: Arc<dyn Any + Send + Sync>,
    /// Type-erased notifier used to send errors/faults to the parent.
    pub notifier: Arc<dyn ParentNotifier>,
}

/// Concrete notifier wrapping a typed `ChildErrorSender`.
pub struct TypedParentNotifier<E, F>
where
    E: Debug + Send + Sync + 'static,
    F: Debug + Clone + From<Error> + Send + Sync + 'static,
{
    sender: ChildErrorSender<E, F>,
}

impl<E, F> TypedParentNotifier<E, F>
where
    E: Debug + Send + Sync + 'static,
    F: Debug + Clone + From<Error> + Send + Sync + 'static,
{
    pub const fn new(sender: ChildErrorSender<E, F>) -> Self {
        Self { sender }
    }
}

impl<E, F> Clone for TypedParentNotifier<E, F>
where
    E: Debug + Send + Sync + 'static,
    F: Debug + Clone + From<Error> + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

#[async_trait]
impl<E, F> ParentNotifier for TypedParentNotifier<E, F>
where
    E: Debug + Send + Sync + 'static,
    F: Debug + Clone + From<Error> + Send + Sync + 'static,
{
    async fn notify_error(
        &self,
        error: Box<dyn Any + Send>,
    ) -> Result<(), Error> {
        let error = *error.downcast::<E>().map_err(|_| {
            Error::Functional {
                description: "Child error type does not match parent expectation"
                    .to_string(),
            }
        })?;
        self.sender
            .send(ChildError::Error { error })
            .await
            .map_err(|e| Error::Send {
                reason: e.to_string(),
            })
    }

    async fn notify_fault(
        &self,
        fault: Box<dyn Any + Send>,
    ) -> Result<ChildAction, Error> {
        let fault = *fault.downcast::<F>().map_err(|_| {
            Error::Functional {
                description: "Child fault type does not match parent expectation"
                    .to_string(),
            }
        })?;
        let (action_sender, action_receiver) = oneshot::channel();
        self.sender
            .send(ChildError::Fault {
                error: fault,
                sender: action_sender,
            })
            .await
            .map_err(|e| Error::Send {
                reason: e.to_string(),
            })?;
        action_receiver.await.map_err(|e| Error::Send {
            reason: e.to_string(),
        })
    }

}

/// Typed handle that allows a child actor to send messages to its parent and
/// escalate errors/faults to it.
///
/// Obtain a `ParentRef<P>` via [`ActorContext::get_parent`]. The parent actor
/// must be of type `P`; otherwise the lookup returns an error.
pub struct ParentRef<P: Actor + Handler<P>> {
    actor_ref: ActorRef<P>,
    notifier: Arc<dyn ParentNotifier>,
    stop: crate::runner::StopSender,
}

impl<P: Actor + Handler<P>> ParentRef<P> {
    pub(crate) fn new(
        actor_ref: ActorRef<P>,
        notifier: Arc<dyn ParentNotifier>,
        stop: crate::runner::StopSender,
    ) -> Self {
        Self {
            actor_ref,
            notifier,
            stop,
        }
    }

    /// Sends a message to the parent actor without waiting for a response.
    pub async fn tell(&self, message: P::Message) -> Result<(), Error> {
        self.actor_ref.tell(message).await
    }

    /// Sends a message to the parent actor and waits for a response.
    pub async fn ask(&self, message: P::Message) -> Result<P::Response, Error> {
        self.actor_ref.ask(message).await
    }

    /// Sends a message to the parent actor and waits up to `timeout`.
    pub async fn ask_timeout(
        &self,
        message: P::Message,
        timeout: std::time::Duration,
    ) -> Result<P::Response, Error> {
        self.actor_ref.ask_timeout(message, timeout).await
    }

    /// Reports a non-fatal error to the parent, which will invoke
    /// `on_child_error`.
    pub async fn emit_error(
        &self,
        error: P::ChildError,
    ) -> Result<(), Error> {
        tracing::warn!(error = ?error, "Escalating error to parent");
        self.notifier
            .notify_error(Box::new(error) as Box<dyn Any + Send>)
            .await
    }

    /// Reports a fatal fault to the parent. The actor is stopped and the runner
    /// will ask the parent for the `ChildAction` when entering the `Failed`
    /// state.
    pub async fn emit_fail(&self, fault: P::ChildFault) -> Result<(), Error> {
        tracing::error!(error = ?fault, "Escalating fault to parent, stopping actor");
        self.stop
            .send(crate::runner::StopSignal::Fault(
                Box::new(fault) as Box<dyn Any + Send + Sync>,
                None,
            ))
            .await
            .map_err(|e| Error::Send {
                reason: e.to_string(),
            })
    }
}

impl<P: Actor + Handler<P>> Clone for ParentRef<P> {
    fn clone(&self) -> Self {
        Self {
            actor_ref: self.actor_ref.clone(),
            notifier: Arc::clone(&self.notifier),
            stop: self.stop.clone(),
        }
    }
}

/// Helper to wrap a typed sender into a type-erased notifier.
pub fn boxed_notifier<E, F>(
    sender: ChildErrorSender<E, F>,
) -> Arc<dyn ParentNotifier>
where
    E: Debug + Send + Sync + 'static,
    F: Debug + Clone + From<Error> + Send + Sync + 'static,
{
    Arc::new(TypedParentNotifier::new(sender))
}

/// Helper to create a typed `ChildErrorSender` / `ChildErrorReceiver` pair.
pub fn child_error_channel<E, F>() -> (
    ChildErrorSender<E, F>,
    ChildErrorReceiver<E, F>,
)
where
    E: Debug + Send + Sync + 'static,
    F: Debug + Clone + From<Error> + Send + Sync + 'static,
{
    mpsc::channel(256)
}
