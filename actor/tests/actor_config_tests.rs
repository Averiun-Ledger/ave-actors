//! Tests for actor configuration validation.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error, Handler, Message,
    NotPersistentActor, Response,
};
use std::time::Duration;
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Clone)]
struct Msg;
impl Message for Msg {}

#[derive(Clone, PartialEq, Eq, Debug)]
struct EmptyResponse;
impl Response for EmptyResponse {}

async fn join_runner(
    handle: tokio::task::JoinHandle<ave_actors_actor::ShutdownReason>,
) -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .map_err(|_| Error::Functional {
            description: "runner timed out".to_owned(),
        })?
        .map_err(|_| Error::Functional {
            description: "runner panicked".to_owned(),
        })
        .map(|_| ())
}

// ===== Valid actor ==========================================================

#[derive(Clone)]
struct ValidActor;

impl NotPersistentActor for ValidActor {}

#[async_trait]
impl Actor for ValidActor {
    type Message = Msg;
    type Response = EmptyResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ValidActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for ValidActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: Msg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<EmptyResponse, Error> {
        Ok(EmptyResponse)
    }
}

#[test(tokio::test)]
async fn test_valid_actor_configuration_is_accepted() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor::<ValidActor, _>("valid", ValidActor)
        .await?;

    let response = actor.ask(Msg).await?;
    assert_eq!(response, EmptyResponse);

    system.stop_system();
    join_runner(runner_handle).await
}

// ===== Invalid timeout actors ===============================================

macro_rules! define_timeout_actor {
    ($name:ident, $method:ident, $value:expr) => {
        #[derive(Clone)]
        struct $name;

        impl NotPersistentActor for $name {}

        #[async_trait]
        impl Actor for $name {
            type Message = Msg;
            type Response = EmptyResponse;
            type Event = ();
            type SinkEvent = Self::Event;
            type ChildError = Error;
            type ChildFault = Error;

            fn $method() -> Duration {
                $value
            }

            fn get_span(
                id: &str,
                _parent_span: Option<tracing::Span>,
            ) -> tracing::Span {
                info_span!(stringify!($name), id = %id)
            }
        }

        #[async_trait]
        impl Handler<Self> for $name {
            async fn handle_message(
                &mut self,
                _sender: ActorPath,
                _msg: Msg,
                _ctx: &mut ActorContext<Self>,
            ) -> Result<EmptyResponse, Error> {
                Ok(EmptyResponse)
            }
        }
    };
}

macro_rules! define_optional_timeout_actor {
    ($name:ident, $method:ident, $value:expr) => {
        #[derive(Clone)]
        struct $name;

        impl NotPersistentActor for $name {}

        #[async_trait]
        impl Actor for $name {
            type Message = Msg;
            type Response = EmptyResponse;
            type Event = ();
            type SinkEvent = Self::Event;
            type ChildError = Error;
            type ChildFault = Error;

            fn $method() -> Option<Duration> {
                Some($value)
            }

            fn get_span(
                id: &str,
                _parent_span: Option<tracing::Span>,
            ) -> tracing::Span {
                info_span!(stringify!($name), id = %id)
            }
        }

        #[async_trait]
        impl Handler<Self> for $name {
            async fn handle_message(
                &mut self,
                _sender: ActorPath,
                _msg: Msg,
                _ctx: &mut ActorContext<Self>,
            ) -> Result<EmptyResponse, Error> {
                Ok(EmptyResponse)
            }
        }
    };
}

define_timeout_actor!(
    ZeroMailboxDrainActor,
    mailbox_drain_timeout,
    Duration::ZERO
);
define_timeout_actor!(
    MaxMailboxDrainActor,
    mailbox_drain_timeout,
    Duration::MAX
);
define_timeout_actor!(ZeroEventDrainActor, event_drain_timeout, Duration::ZERO);
define_timeout_actor!(MaxEventDrainActor, event_drain_timeout, Duration::MAX);
define_optional_timeout_actor!(
    ZeroStartupActor,
    startup_timeout,
    Duration::ZERO
);
define_optional_timeout_actor!(MaxStartupActor, startup_timeout, Duration::MAX);
define_optional_timeout_actor!(ZeroStopActor, stop_timeout, Duration::ZERO);
define_optional_timeout_actor!(MaxStopActor, stop_timeout, Duration::MAX);

macro_rules! assert_invalid_configuration {
    ($system:expr, $actor:path, $name:expr) => {
        let result =
            $system.create_root_actor::<$actor, _>($name, $actor).await;
        assert!(
            matches!(result, Err(Error::InvalidConfiguration { .. })),
            "expected InvalidConfiguration for {}, got {:?}",
            $name,
            result
        );
    };
}

#[test(tokio::test)]
async fn test_zero_mailbox_drain_timeout_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(system, ZeroMailboxDrainActor, "zero_drain");

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_max_mailbox_drain_timeout_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(system, MaxMailboxDrainActor, "max_drain");

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_zero_event_drain_timeout_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        ZeroEventDrainActor,
        "zero_event_drain"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_max_event_drain_timeout_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        MaxEventDrainActor,
        "max_event_drain"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_zero_startup_timeout_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(system, ZeroStartupActor, "zero_startup");

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_max_startup_timeout_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(system, MaxStartupActor, "max_startup");

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_zero_stop_timeout_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(system, ZeroStopActor, "zero_stop");

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_max_stop_timeout_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(system, MaxStopActor, "max_stop");

    system.stop_system();
    join_runner(runner_handle).await
}

// ===== max_timers validation =================================================

macro_rules! define_max_timers_actor {
    ($name:ident, $value:expr) => {
        #[derive(Clone)]
        struct $name;

        impl NotPersistentActor for $name {}

        #[async_trait]
        impl Actor for $name {
            type Message = Msg;
            type Response = EmptyResponse;
            type Event = ();
            type SinkEvent = Self::Event;
            type ChildError = Error;
            type ChildFault = Error;

            fn max_timers() -> usize {
                $value
            }

            fn get_span(
                id: &str,
                _parent_span: Option<tracing::Span>,
            ) -> tracing::Span {
                info_span!(stringify!($name), id = %id)
            }
        }

        #[async_trait]
        impl Handler<Self> for $name {
            async fn handle_message(
                &mut self,
                _sender: ActorPath,
                _msg: Msg,
                _ctx: &mut ActorContext<Self>,
            ) -> Result<EmptyResponse, Error> {
                Ok(EmptyResponse)
            }
        }
    };
}

define_max_timers_actor!(ZeroTimersActor, 0);
define_max_timers_actor!(TooManyTimersActor, 100_001);

#[test(tokio::test)]
async fn test_zero_max_timers_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(system, ZeroTimersActor, "zero_timers");

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_above_max_timers_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        TooManyTimersActor,
        "too_many_timers"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

// ===== ask_timeout validation =================================================

#[test(tokio::test)]
async fn test_ask_timeout_zero_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor::<ValidActor, _>("ask_timeout_zero", ValidActor)
        .await?;

    let result = actor.ask_timeout(Msg, Duration::ZERO).await;
    assert!(
        matches!(result, Err(Error::InvalidConfiguration { .. })),
        "expected InvalidConfiguration for ask_timeout zero, got {:?}",
        result
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_ask_timeout_max_is_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor::<ValidActor, _>("ask_timeout_max", ValidActor)
        .await?;

    let result = actor.ask_timeout(Msg, Duration::MAX).await;
    assert!(
        matches!(result, Err(Error::InvalidConfiguration { .. })),
        "expected InvalidConfiguration for ask_timeout max, got {:?}",
        result
    );

    system.stop_system();
    join_runner(runner_handle).await
}
