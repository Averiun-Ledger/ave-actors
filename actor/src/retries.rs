//! Retries module.
//!
//! This module provides a helper actor that re-sends the same message according
//! to a retry schedule.
//!

use crate::{
    Actor, ActorContext, ActorPath, Error, Handler, Message,
    NotPersistentActor,
    supervision::{RetryStrategy, Strategy},
};

use async_trait::async_trait;

use std::{fmt::Debug, marker::PhantomData};
use tracing::{debug, error, info_span};

#[async_trait]
trait CompletionNotifier<T>: Send + Sync
where
    T: Actor + Handler<T> + Clone + NotPersistentActor,
{
    async fn notify(&self, ctx: &ActorContext<RetryActor<T>>);
}

struct ParentMessageNotifier<T, P>
where
    T: Actor + Handler<T> + Clone + NotPersistentActor,
    P: Actor + Handler<P>,
{
    message: P::Message,
    _phantom: PhantomData<(T, P)>,
}

#[async_trait]
impl<T, P> CompletionNotifier<T> for ParentMessageNotifier<T, P>
where
    T: Actor + Handler<T> + Clone + NotPersistentActor,
    P: Actor + Handler<P>,
{
    async fn notify(&self, ctx: &ActorContext<RetryActor<T>>) {
        if let Ok(parent) = ctx.get_parent::<P>().await {
            let _ = parent.tell(self.message.clone()).await;
        }
    }
}

/// Retry actor.
///
/// `RetryActor` re-sends the same message to a managed child actor according to
/// the configured schedule. A successful `tell()` does not end the sequence by
/// itself; retries continue until:
///
/// - `RetryMessage::End` is received
/// - `max_retries` is reached
/// - the target child can no longer accept messages
///
/// The retry cycle can only be started once with [`RetryMessage::Retry`]. Any
/// later `Retry` messages are ignored.
///
/// `RetryMessage::End` is always terminal:
/// - before the cycle starts, it stops the actor without sending the target message
/// - while a retry is scheduled, it cancels the pending retry and finishes the cycle
pub struct RetryActor<T>
where
    T: Actor + Handler<T> + Clone + NotPersistentActor,
{
    target: T,
    message: T::Message,
    retry_strategy: Strategy,
    retries: usize,
    started: bool,
    is_end: bool,
    completion_pending: bool,
    finish_started: bool,
    on_finished: Option<Box<dyn CompletionNotifier<T>>>,
    /// Timer key for the next scheduled retry attempt.
    pending_retry: Option<crate::TimerKey>,
    /// Timer key for the scheduled completion (stop) marker.
    completion_timer: Option<crate::TimerKey>,
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
            started: false,
            is_end: false,
            completion_pending: false,
            finish_started: false,
            on_finished: None,
            pending_retry: None,
            completion_timer: None,
        }
    }

    /// Creates a `RetryActor` that notifies its parent with `completion_message`
    /// when the retry cycle finishes, either because:
    ///
    /// - the retry budget is exhausted
    /// - `RetryMessage::End` stops it explicitly
    /// - the managed child no longer accepts messages
    pub fn new_with_parent_message<P>(
        target: T,
        message: T::Message,
        retry_strategy: Strategy,
        completion_message: P::Message,
    ) -> Self
    where
        P: Actor + Handler<P>,
    {
        Self {
            target,
            message,
            retry_strategy,
            retries: 0,
            started: false,
            is_end: false,
            completion_pending: false,
            finish_started: false,
            on_finished: Some(Box::new(ParentMessageNotifier::<T, P> {
                message: completion_message,
                _phantom: PhantomData,
            })),
            pending_retry: None,
            completion_timer: None,
        }
    }

    async fn finish_retry_cycle(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), Error> {
        self.is_end = true;
        if let Some(key) = self.pending_retry.take() {
            ctx.cancel_timer(key);
        }
        if !self.finish_started {
            self.finish_started = true;
        } else {
            ctx.stop(None).await;
            return Ok(());
        }

        if let Some(notifier) = self.on_finished.as_ref() {
            notifier.notify(ctx).await;
        }

        self.schedule_completion(ctx).await?;
        Ok(())
    }

    async fn schedule_completion(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), Error> {
        self.completion_pending = true;
        let key = ctx.schedule_once(
            std::time::Duration::from_millis(1),
            RetryMessage::Complete,
        )?;
        self.completion_timer = Some(key);
        Ok(())
    }

    async fn handle_retry_attempt(
        &mut self,
        ctx: &ActorContext<Self>,
    ) -> Result<(), Error> {
        if self.is_end {
            return Ok(());
        }

        self.retries += 1;
        if self.retries > self.retry_strategy.max_retries() {
            self.finish_retry_cycle(ctx).await?;
            return Ok(());
        }

        debug!(
            retry = self.retries,
            max_retries = self.retry_strategy.max_retries(),
            "Executing retry"
        );

        // Re-send to the managed target child. Successful delivery does not stop
        // the schedule; only explicit End, max retries or target unavailability do.
        let child = match ctx.get_child::<T>("target").await {
            Ok(child) => child,
            Err(err) => {
                error!(error = %err, "Retry target is not available");
                self.finish_retry_cycle(ctx).await?;
                return Ok(());
            }
        };

        if let Err(err) = child.tell(self.message.clone()).await {
            error!(error = %err, "Failed to send retry message to target");
            self.finish_retry_cycle(ctx).await?;
            return Ok(());
        }

        match self.retry_strategy.next_backoff() {
            Some(duration) => {
                let key =
                    ctx.schedule_once(duration, RetryMessage::Continue)?;
                self.pending_retry = Some(key);
            }
            None => {
                let actor = ctx.reference().await?;
                actor.tell(RetryMessage::Continue).await?;
            }
        }

        Ok(())
    }
}
#[derive(Debug, Clone)]
pub enum RetryMessage {
    /// Starts the retry cycle. Later `Retry` messages are ignored.
    Retry,
    /// Internal scheduled retry attempt after the cycle has already started.
    Continue,
    /// Explicitly finishes the retry cycle.
    End,
    /// Internal completion marker used to stop after the final target delivery
    /// had a chance to run.
    Complete,
}

impl Message for RetryMessage {}

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
    type SinkEvent = ();
    type ChildError = Error;
    type ChildFault = Error;

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

    async fn pre_stop(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        if let Some(key) = self.pending_retry.take() {
            ctx.cancel_timer(key);
        }
        if let Some(key) = self.completion_timer.take() {
            ctx.cancel_timer(key);
        }
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
                if self.started {
                    debug!(
                        "Retry cycle already started, ignoring duplicate start"
                    );
                } else {
                    self.started = true;
                    self.handle_retry_attempt(ctx).await?;
                }
            }
            RetryMessage::Continue => {
                self.handle_retry_attempt(ctx).await?;
            }
            RetryMessage::End => {
                self.finish_retry_cycle(ctx).await?;
            }
            RetryMessage::Complete => {
                if self.completion_pending {
                    self.completion_pending = false;
                    ctx.stop(None).await;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use test_log::test;
    use tokio_util::sync::CancellationToken;
    use tracing::info_span;

    use super::*;

    use crate::{ActorRef, ActorSystem, Error, IntervalStrategy};

    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
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
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("SourceActor", id = %id)
        }

        async fn pre_start(
            &mut self,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            println!("SourceActor pre_start");
            let target = TargetActor { counter: 0 };

            let strategy = Strategy::Interval(IntervalStrategy::new(
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
    impl Handler<Self> for SourceActor {
        async fn handle_message(
            &mut self,
            _path: ActorPath,
            message: SourceMessage,
            ctx: &mut ActorContext<Self>,
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

    #[derive(Debug, Clone)]
    enum ParentMsg {
        Start,
        RetryFinished,
    }

    impl Message for ParentMsg {}

    #[derive(Clone)]
    struct CompletionParent {
        completions: Arc<AtomicUsize>,
    }

    impl NotPersistentActor for CompletionParent {}

    #[async_trait]
    impl Actor for CompletionParent {
        type Message = ParentMsg;
        type Response = ();
        type Event = ();
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("CompletionParent", id = %id)
        }

        async fn pre_start(
            &mut self,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            let retry = RetryActor::new_with_parent_message::<Self>(
                PassiveTarget,
                PassiveMessage,
                Strategy::Interval(IntervalStrategy::new(
                    2,
                    Duration::from_millis(10),
                )),
                ParentMsg::RetryFinished,
            );
            let _: ActorRef<RetryActor<PassiveTarget>> =
                ctx.create_child("retry", retry).await?;
            Ok(())
        }
    }

    #[async_trait]
    impl Handler<Self> for CompletionParent {
        async fn handle_message(
            &mut self,
            _path: ActorPath,
            message: ParentMsg,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            match message {
                ParentMsg::Start => {
                    let retry = ctx
                        .get_child::<RetryActor<PassiveTarget>>("retry")
                        .await?;
                    retry.tell(RetryMessage::Retry).await?;
                }
                ParentMsg::RetryFinished => {
                    self.completions.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(())
        }
    }

    #[derive(Clone)]
    struct PassiveTarget;

    impl NotPersistentActor for PassiveTarget {}

    #[derive(Debug, Clone)]
    struct PassiveMessage;

    impl Message for PassiveMessage {}

    impl Actor for PassiveTarget {
        type Message = PassiveMessage;
        type Response = ();
        type Event = ();
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("PassiveTarget", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for PassiveTarget {
        async fn handle_message(
            &mut self,
            _path: ActorPath,
            _message: PassiveMessage,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct CountingTarget {
        deliveries: Arc<AtomicUsize>,
    }

    impl NotPersistentActor for CountingTarget {}

    #[derive(Debug, Clone)]
    struct CountMessage;

    impl Message for CountMessage {}

    impl Actor for CountingTarget {
        type Message = CountMessage;
        type Response = ();
        type Event = ();
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("CountingTarget", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for CountingTarget {
        async fn handle_message(
            &mut self,
            _path: ActorPath,
            _message: CountMessage,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            self.deliveries.fetch_add(1, Ordering::SeqCst);
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
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("TargetActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for TargetActor {
        async fn handle_message(
            &mut self,
            _path: ActorPath,
            message: TargetMessage,
            ctx: &mut ActorContext<Self>,
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

    #[test(tokio::test)]
    async fn test_retry_actor() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );

        tokio::spawn(async move {
            runner.run().await;
        });

        let _: ActorRef<SourceActor> = system
            .create_root_actor("source", SourceActor)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    #[derive(Clone)]
    struct StopAfterFirstTarget {
        deliveries: Arc<AtomicUsize>,
    }

    impl NotPersistentActor for StopAfterFirstTarget {}

    #[derive(Debug, Clone)]
    struct StopAfterFirstMessage;

    impl Message for StopAfterFirstMessage {}

    impl Actor for StopAfterFirstTarget {
        type Message = StopAfterFirstMessage;
        type Response = ();
        type Event = ();
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("StopAfterFirstTarget", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for StopAfterFirstTarget {
        async fn handle_message(
            &mut self,
            _path: ActorPath,
            _message: StopAfterFirstMessage,
            ctx: &mut ActorContext<Self>,
        ) -> Result<(), Error> {
            let count = self.deliveries.fetch_add(1, Ordering::SeqCst) + 1;
            if count == 1 {
                ctx.stop(None).await;
            }
            Ok(())
        }
    }

    #[test(tokio::test)]
    async fn test_retry_actor_stops_when_target_unavailable() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );

        tokio::spawn(async move {
            runner.run().await;
        });

        let deliveries = Arc::new(AtomicUsize::new(0));
        let retry_actor = RetryActor::new(
            StopAfterFirstTarget {
                deliveries: deliveries.clone(),
            },
            StopAfterFirstMessage,
            Strategy::Interval(IntervalStrategy::new(
                5,
                Duration::from_millis(20),
            )),
        );

        let retry_ref: ActorRef<RetryActor<StopAfterFirstTarget>> = system
            .create_root_actor("retry_stop_on_send_failure", retry_actor)
            .await
            .unwrap();

        retry_ref.tell(RetryMessage::Retry).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), retry_ref.closed())
            .await
            .expect("retry actor should stop after target becomes unavailable");

        assert_eq!(deliveries.load(Ordering::SeqCst), 1);
    }

    #[test(tokio::test)]
    async fn test_retry_actor_notifies_parent_when_retries_finish() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );

        tokio::spawn(async move {
            runner.run().await;
        });

        let completions = Arc::new(AtomicUsize::new(0));
        let parent = CompletionParent {
            completions: completions.clone(),
        };

        let parent_ref: ActorRef<CompletionParent> = system
            .create_root_actor("completion_parent", parent)
            .await
            .unwrap();

        parent_ref.tell(ParentMsg::Start).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if completions.load(Ordering::SeqCst) == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("parent should receive completion notification");
    }

    #[test(tokio::test)]
    async fn test_retry_actor_ignores_duplicate_retry_start() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );

        tokio::spawn(async move {
            runner.run().await;
        });

        let deliveries = Arc::new(AtomicUsize::new(0));
        let retry_actor = RetryActor::new(
            CountingTarget {
                deliveries: deliveries.clone(),
            },
            CountMessage,
            Strategy::NoInterval(crate::NoIntervalStrategy::new(3)),
        );

        let retry_ref: ActorRef<RetryActor<CountingTarget>> = system
            .create_root_actor::<RetryActor<CountingTarget>, _>(
                "retry_duplicate_start",
                retry_actor,
            )
            .await
            .unwrap();

        retry_ref.tell(RetryMessage::Retry).await.unwrap();
        retry_ref.tell(RetryMessage::Retry).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if deliveries.load(Ordering::SeqCst) == 3 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("retry actor should deliver exactly one retry cycle");

        tokio::time::timeout(Duration::from_secs(1), retry_ref.closed())
            .await
            .expect("retry actor should stop after exhausting retries");
    }

    #[test(tokio::test)]
    async fn test_retry_end_with_pending_retry() {
        let (system, mut runner) = ActorSystem::create(
            CancellationToken::new(),
            CancellationToken::new(),
        );

        tokio::spawn(async move {
            runner.run().await;
        });

        let retry_actor = RetryActor::new(
            CountingTarget {
                deliveries: Arc::new(AtomicUsize::new(0)),
            },
            CountMessage,
            Strategy::Interval(IntervalStrategy::new(
                5,
                Duration::from_secs(10),
            )),
        );

        let retry_ref: ActorRef<RetryActor<CountingTarget>> = system
            .create_root_actor("retry_end_pending", retry_actor)
            .await
            .unwrap();

        retry_ref.tell(RetryMessage::Retry).await.unwrap();
        retry_ref.tell(RetryMessage::End).await.unwrap();

        tokio::time::timeout(Duration::from_secs(1), retry_ref.closed())
            .await
            .expect("retry actor should stop after End");
    }
}
