//! Tests for supervision strategy configuration validation.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, CustomIntervalStrategy, Error,
    ExponentialBackoffStrategy, Handler, IntervalStrategy, Message,
    NoIntervalStrategy, NotPersistentActor, Response, Strategy,
    SupervisionStrategy,
};
use std::{collections::VecDeque, time::Duration};
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

macro_rules! define_strategy_actor {
    ($name:ident, $strategy:expr) => {
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

            fn supervision_strategy() -> SupervisionStrategy {
                $strategy
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

// ===== Valid strategy =======================================================

#[derive(Clone)]
struct ValidStrategyActor;

impl NotPersistentActor for ValidStrategyActor {}

#[async_trait]
impl Actor for ValidStrategyActor {
    type Message = Msg;
    type Response = EmptyResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Retry(Strategy::Interval(IntervalStrategy::new(
            3,
            Duration::from_secs(1),
        )))
    }

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ValidStrategyActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for ValidStrategyActor {
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
async fn test_valid_supervision_strategy_is_accepted() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor::<ValidStrategyActor, _>(
            "valid_strategy",
            ValidStrategyActor,
        )
        .await?;
    let _ = actor.ask(Msg).await?;

    system.stop_system();
    join_runner(runner_handle).await
}

// ===== Invalid strategies ===================================================

define_strategy_actor!(
    InfiniteRetriesActor,
    SupervisionStrategy::Retry(Strategy::NoInterval(NoIntervalStrategy::new(
        usize::MAX
    )))
);

define_strategy_actor!(
    ZeroIntervalActor,
    SupervisionStrategy::Retry(Strategy::Interval(IntervalStrategy::new(
        3,
        Duration::ZERO
    )))
);

define_strategy_actor!(
    MaxIntervalActor,
    SupervisionStrategy::Retry(Strategy::Interval(IntervalStrategy::new(
        3,
        Duration::MAX
    )))
);

define_strategy_actor!(
    TooManyRetriesIntervalActor,
    SupervisionStrategy::Retry(Strategy::Interval(IntervalStrategy::new(
        1_001,
        Duration::from_secs(1)
    )))
);

define_strategy_actor!(
    ZeroBaseExponentialActor,
    SupervisionStrategy::Retry(Strategy::Exponential(
        ExponentialBackoffStrategy::new(
            3,
            Duration::ZERO,
            Duration::from_secs(1),
            2,
        )
    ))
);

define_strategy_actor!(
    MaxBaseExponentialActor,
    SupervisionStrategy::Retry(Strategy::Exponential(
        ExponentialBackoffStrategy::new(3, Duration::MAX, Duration::MAX, 2,)
    ))
);

define_strategy_actor!(
    ZeroMultiplierExponentialActor,
    SupervisionStrategy::Retry(Strategy::Exponential(
        ExponentialBackoffStrategy::new(
            3,
            Duration::from_millis(100),
            Duration::from_secs(1),
            0,
        )
    ))
);

define_strategy_actor!(
    OneMultiplierExponentialActor,
    SupervisionStrategy::Retry(Strategy::Exponential(
        ExponentialBackoffStrategy::new(
            3,
            Duration::from_millis(100),
            Duration::from_secs(1),
            1,
        )
    ))
);

define_strategy_actor!(
    BaseGreaterThanMaxExponentialActor,
    SupervisionStrategy::Retry(Strategy::Exponential(
        ExponentialBackoffStrategy::new(
            3,
            Duration::from_secs(10),
            Duration::from_secs(1),
            2,
        )
    ))
);

define_strategy_actor!(
    InfiniteRetriesExponentialActor,
    SupervisionStrategy::Retry(Strategy::Exponential(
        ExponentialBackoffStrategy::new(
            usize::MAX,
            Duration::from_millis(100),
            Duration::from_secs(1),
            2,
        )
    ))
);

define_strategy_actor!(
    ZeroCustomIntervalActor,
    SupervisionStrategy::Retry(Strategy::CustomIntervalStrategy(
        CustomIntervalStrategy::new(VecDeque::from([Duration::ZERO]))
    ))
);

#[test(tokio::test)]
async fn test_infinite_retries_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        InfiniteRetriesActor,
        "infinite_retries"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_zero_interval_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(system, ZeroIntervalActor, "zero_interval");

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_max_interval_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(system, MaxIntervalActor, "max_interval");

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_too_many_retries_interval_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        TooManyRetriesIntervalActor,
        "too_many_retries_interval"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_zero_base_exponential_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        ZeroBaseExponentialActor,
        "zero_base_exp"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_max_base_exponential_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        MaxBaseExponentialActor,
        "max_base_exp"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_zero_multiplier_exponential_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        ZeroMultiplierExponentialActor,
        "zero_multiplier_exp"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_one_multiplier_exponential_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        OneMultiplierExponentialActor,
        "one_multiplier_exp"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_base_greater_than_max_exponential_rejected() -> Result<(), Error>
{
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        BaseGreaterThanMaxExponentialActor,
        "base_gt_max_exp"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_infinite_retries_exponential_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        InfiniteRetriesExponentialActor,
        "infinite_retries_exp"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_zero_custom_interval_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    assert_invalid_configuration!(
        system,
        ZeroCustomIntervalActor,
        "zero_custom_interval"
    );

    system.stop_system();
    join_runner(runner_handle).await
}
