//! Retries module.
//!
//! This module provides the necessary components for retrying messages through a backoff strategy.
//!

use crate::{
    Actor, ActorContext, ActorPath, ActorRef, Error, Event, Handler, Message,
    NotPersistentActor, Response,
    supervision::{RetryStrategy, Strategy},
};

use async_trait::async_trait;

use std::fmt::Debug;
use tracing::{debug, error, info_span};

/// Retry actor.
pub struct RetryActor<T>
where
    T: Actor + Handler<T> + Clone + NotPersistentActor,
{
    target: T,
    message: T::Message,
    retry_strategy: Strategy,
    retries: usize,
    is_end: bool,
}

impl<T> RetryActor<T>
where
    T: Actor + Handler<T> + Clone + NotPersistentActor,
{
    /// Create a new RetryActor.
    pub const fn new(
        target: T,
        message: T::Message,
        retry_strategy: Strategy,
    ) -> Self {
        Self {
            target,
            message,
            retry_strategy,
            retries: 0,
            is_end: false,
        }
    }
}
#[derive(Debug, Clone)]
pub enum RetryMessage {
    Retry,
    End,
}

impl Message for RetryMessage {}

impl Response for () {}

impl Event for () {}

impl<T> NotPersistentActor for RetryActor<T> where
    T: Actor + Handler<T> + Clone + NotPersistentActor
{
}

#[async_trait]
impl<T> Actor for RetryActor<T>
where
    T: Actor + Handler<T> + Clone + NotPersistentActor,
{
    type Message = RetryMessage;
    type Response = ();
    type Event = ();

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("RetryActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        ctx.create_child("target", self.target.clone()).await?;
        Ok(())
    }
}

#[async_trait]
impl<T> Handler<Self> for RetryActor<T>
where
    T: Actor + Handler<T> + Clone + NotPersistentActor,
{
    async fn handle_message(
        &mut self,
        _path: ActorPath,
        message: RetryMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        match message {
            RetryMessage::Retry => {
                if !self.is_end {
                    self.retries += 1;
                    if self.retries <= self.retry_strategy.max_retries() {
                        debug!(
                            retry = self.retries,
                            max_retries = self.retry_strategy.max_retries(),
                            "Executing retry"
                        );

                        // Send retry to parent.
                        if let Ok(child) = ctx.get_child::<T>("target").await
                            && child.tell(self.message.clone()).await.is_err()
                        {
                            error!("Failed to send retry message to target");
                        }

                        if let Ok(actor) = ctx.reference().await {
                            if let Some(duration) =
                                self.retry_strategy.next_backoff()
                            {
                                let actor: ActorRef<Self> = actor;
                                tokio::spawn(async move {
                                    tokio::time::sleep(duration).await;
                                    let _ =
                                        actor.tell(RetryMessage::Retry).await;
                                });
                            }
                        } else {
                            error!("Failed to get retry actor reference");
                            let _ = ctx
                                .emit_error(Error::Send {
                                    reason: "Cannot get retry actor reference"
                                        .to_owned(),
                                })
                                .await;
                        };
                        // Next retry
                    } else if let Err(e) = ctx.emit_error(Error::Retry).await {
                        error!(error = %e, "Failed to emit retry error");
                    };
                }
            }
            RetryMessage::End => {
                self.is_end = true;
                ctx.stop(None).await;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use tokio_util::sync::CancellationToken;
    use tracing::info_span;

    use super::*;

    use crate::{
        ActorSystem, Error, FixedIntervalStrategy, build_tracing_subscriber,
    };

    use std::time::Duration;

    pub struct SourceActor;

    impl NotPersistentActor for SourceActor {}

    #[derive(Debug, Clone)]
    pub struct SourceMessage(pub String);

    impl Message for SourceMessage {}

    #[async_trait]
    impl Actor for SourceActor {
        type Message = SourceMessage;
        type Response = ();
        type Event = ();

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("SourceActor", id = %id)
        }

        async fn pre_start(
            &mut self,
            ctx: &mut ActorContext<SourceActor>,
        ) -> Result<(), Error> {
            println!("SourceActor pre_start");
            let target = TargetActor { counter: 0 };

            let strategy = Strategy::FixedInterval(FixedIntervalStrategy::new(
                3,
                Duration::from_secs(1),
            ));

            let retry_actor = RetryActor::new(
                target,
                TargetMessage {
                    source: ctx.path().clone(),
                    message: "Hello from parent".to_owned(),
                },
                strategy,
            );
            let retry: ActorRef<RetryActor<TargetActor>> =
                ctx.create_child("retry", retry_actor).await.unwrap();

            retry.tell(RetryMessage::Retry).await.unwrap();
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<SourceActor> for SourceActor {
        async fn handle_message(
            &mut self,
            _path: ActorPath,
            message: SourceMessage,
            ctx: &mut ActorContext<SourceActor>,
        ) -> Result<(), Error> {
            println!("Message: {:?}", message);
            assert_eq!(message.0, "Hello from child");

            let retry = ctx
                .get_child::<RetryActor<TargetActor>>("retry")
                .await
                .unwrap();
            retry.tell(RetryMessage::End).await.unwrap();

            Ok(())
        }
    }

    #[derive(Clone)]
    pub struct TargetActor {
        counter: usize,
    }

    #[derive(Debug, Clone)]
    pub struct TargetMessage {
        pub source: ActorPath,
        pub message: String,
    }

    impl Message for TargetMessage {}

    impl NotPersistentActor for TargetActor {}

    impl Actor for TargetActor {
        type Message = TargetMessage;
        type Response = ();
        type Event = ();

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("TargetActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<TargetActor> for TargetActor {
        async fn handle_message(
            &mut self,
            _path: ActorPath,
            message: TargetMessage,
            ctx: &mut ActorContext<TargetActor>,
        ) -> Result<(), Error> {
            assert_eq!(message.message, "Hello from parent");
            self.counter += 1;
            println!("Counter: {}", self.counter);
            if self.counter == 2 {
                let source = ctx
                    .system()
                    .get_actor::<SourceActor>(&message.source)
                    .await
                    .unwrap();
                source
                    .tell(SourceMessage("Hello from child".to_owned()))
                    .await?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_retry_actor() {
        build_tracing_subscriber();
        let (system, mut runner) =
            ActorSystem::create(CancellationToken::new());

        tokio::spawn(async move {
            runner.run().await;
        });

        let _: ActorRef<SourceActor> = system
            .create_root_actor("source", SourceActor)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
