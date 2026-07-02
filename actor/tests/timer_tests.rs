//! Tests for `ActorContext::schedule_once`, `schedule` and `cancel_timer`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Error, Handler, Message,
    NotPersistentActor, Response, TimerKey,
};
use serde::{Deserialize, Serialize};
use test_log::test;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TimerMsg {
    ScheduleOnce,
    SchedulePeriodic,
    Cancel,
    Tick,
    Fire,
    GetCounts,
}

impl Message for TimerMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimerResponse {
    fires: usize,
    ticks: usize,
}

impl Response for TimerResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimerEvent;

impl ave_actors_actor::Event for TimerEvent {}

#[derive(Clone)]
struct TimerActor {
    fires: Arc<Mutex<usize>>,
    ticks: Arc<Mutex<usize>>,
    last_key: Arc<Mutex<Option<TimerKey>>>,
}

impl NotPersistentActor for TimerActor {}

#[async_trait]
impl Actor for TimerActor {
    type Message = TimerMsg;
    type Response = TimerResponse;
    type Event = TimerEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TimerActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for TimerActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: TimerMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<TimerResponse, Error> {
        match msg {
            TimerMsg::ScheduleOnce => {
                let key = ctx
                    .schedule_once(Duration::from_millis(50), TimerMsg::Fire)?;
                *self.last_key.lock().await = Some(key);
            }
            TimerMsg::SchedulePeriodic => {
                let key =
                    ctx.schedule(Duration::from_millis(20), TimerMsg::Tick)?;
                *self.last_key.lock().await = Some(key);
            }
            TimerMsg::Cancel => {
                let value = self.last_key.lock().await.take();
                if let Some(key) = value {
                    ctx.cancel_timer(key);
                }
            }
            TimerMsg::Tick => {
                *self.ticks.lock().await += 1;
            }
            TimerMsg::Fire => {
                *self.fires.lock().await += 1;
            }
            TimerMsg::GetCounts => {
                return Ok(TimerResponse {
                    fires: *self.fires.lock().await,
                    ticks: *self.ticks.lock().await,
                });
            }
        }
        Ok(TimerResponse { fires: 0, ticks: 0 })
    }
}

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
        })?;
    Ok(())
}

#[test(tokio::test)]
async fn test_schedule_once_delivers_message() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = TimerActor {
        fires: Arc::new(Mutex::new(0)),
        ticks: Arc::new(Mutex::new(0)),
        last_key: Arc::new(Mutex::new(None)),
    };
    let actor_ref = system.create_root_actor("schedule_once", actor).await?;

    actor_ref.tell(TimerMsg::ScheduleOnce).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = actor_ref.ask(TimerMsg::GetCounts).await?;
        if resp.fires == 1 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "schedule_once did not fire".to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_schedule_periodic_and_cancel() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = TimerActor {
        fires: Arc::new(Mutex::new(0)),
        ticks: Arc::new(Mutex::new(0)),
        last_key: Arc::new(Mutex::new(None)),
    };
    let actor_ref =
        system.create_root_actor("schedule_periodic", actor).await?;

    actor_ref.tell(TimerMsg::SchedulePeriodic).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = actor_ref.ask(TimerMsg::GetCounts).await?;
        if resp.ticks >= 3 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "periodic schedule did not tick enough".to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    actor_ref.tell(TimerMsg::Cancel).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let resp = actor_ref.ask(TimerMsg::GetCounts).await?;
    assert!(
        resp.ticks < 10,
        "timer should have been cancelled, got {} ticks",
        resp.ticks
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_timers_are_cancelled_on_actor_stop() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = TimerActor {
        fires: Arc::new(Mutex::new(0)),
        ticks: Arc::new(Mutex::new(0)),
        last_key: Arc::new(Mutex::new(None)),
    };
    let fires = actor.fires.clone();
    let actor_ref = system.create_root_actor("cancel_on_stop", actor).await?;

    actor_ref.tell(TimerMsg::ScheduleOnce).await?;
    actor_ref.ask_stop().await?;

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(*fires.lock().await, 0);

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_cancel_timer_before_fire() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = TimerActor {
        fires: Arc::new(Mutex::new(0)),
        ticks: Arc::new(Mutex::new(0)),
        last_key: Arc::new(Mutex::new(None)),
    };
    let actor_ref = system
        .create_root_actor("cancel_before_fire", actor)
        .await?;

    actor_ref.tell(TimerMsg::ScheduleOnce).await?;
    actor_ref.tell(TimerMsg::Cancel).await?;

    tokio::time::sleep(Duration::from_millis(150)).await;
    let resp = actor_ref.ask(TimerMsg::GetCounts).await?;
    assert_eq!(resp.fires, 0, "cancelled timer should not fire");

    system.stop_system();
    join_runner(runner_handle).await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LimitedMsg {
    Fire,
    GetCount,
}

impl Message for LimitedMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LimitedResponse {
    fires: usize,
}

impl Response for LimitedResponse {}

#[derive(Clone)]
struct LimitedTimerActor {
    fires: Arc<Mutex<usize>>,
}

impl NotPersistentActor for LimitedTimerActor {}

#[async_trait]
impl Actor for LimitedTimerActor {
    type Message = LimitedMsg;
    type Response = LimitedResponse;
    type Event = TimerEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn max_timers() -> usize {
        2
    }

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("LimitedTimerActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        for _ in 0..3 {
            ctx.schedule_once(Duration::from_millis(20), LimitedMsg::Fire)?;
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<Self> for LimitedTimerActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: LimitedMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<LimitedResponse, Error> {
        match msg {
            LimitedMsg::Fire => {
                *self.fires.lock().await += 1;
            }
            LimitedMsg::GetCount => {}
        }
        Ok(LimitedResponse {
            fires: *self.fires.lock().await,
        })
    }
}

#[test(tokio::test)]
async fn test_max_timers_limits_new_timers() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = LimitedTimerActor {
        fires: Arc::new(Mutex::new(0)),
    };
    let actor_ref = system.create_root_actor("max_timers", actor).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = actor_ref.ask(LimitedMsg::GetCount).await?;
    assert_eq!(resp.fires, 2);

    system.stop_system();
    join_runner(runner_handle).await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ValidateMsg {
    ZeroDelay,
    ZeroPeriod,
    ExcessiveDelay,
}

impl Message for ValidateMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidateResponse {
    error: Option<String>,
}

impl Response for ValidateResponse {}

#[derive(Clone)]
struct ValidateTimerActor;

impl NotPersistentActor for ValidateTimerActor {}

#[async_trait]
impl Actor for ValidateTimerActor {
    type Message = ValidateMsg;
    type Response = ValidateResponse;
    type Event = TimerEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ValidateTimerActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for ValidateTimerActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: ValidateMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<ValidateResponse, Error> {
        let result = match msg {
            ValidateMsg::ZeroDelay => {
                ctx.schedule_once(Duration::ZERO, ValidateMsg::ZeroDelay)
            }
            ValidateMsg::ZeroPeriod => {
                ctx.schedule(Duration::ZERO, ValidateMsg::ZeroPeriod)
            }
            ValidateMsg::ExcessiveDelay => ctx.schedule_once(
                Duration::from_secs(366 * 24 * 60 * 60),
                ValidateMsg::ExcessiveDelay,
            ),
        };
        Ok(ValidateResponse {
            error: result.err().map(|e| e.to_string()),
        })
    }
}

#[test(tokio::test)]
async fn test_schedule_once_zero_delay_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor("validate_zero_delay", ValidateTimerActor)
        .await?;

    let resp = actor.ask(ValidateMsg::ZeroDelay).await?;
    assert!(
        resp.error.is_some(),
        "zero delay schedule_once should be rejected"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_schedule_zero_period_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor("validate_zero_period", ValidateTimerActor)
        .await?;

    let resp = actor.ask(ValidateMsg::ZeroPeriod).await?;
    assert!(
        resp.error.is_some(),
        "zero period schedule should be rejected"
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_schedule_once_excessive_delay_rejected() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = system
        .create_root_actor("validate_excessive", ValidateTimerActor)
        .await?;

    let resp = actor.ask(ValidateMsg::ExcessiveDelay).await?;
    assert!(
        resp.error.is_some(),
        "excessive delay schedule_once should be rejected"
    );

    system.stop_system();
    join_runner(runner_handle).await
}
