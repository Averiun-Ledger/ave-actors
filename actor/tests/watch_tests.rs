//! Tests for `ActorContext::watch` / `unwatch` (Death Watch).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, ActorSystemConfig,
    Error, Handler, IntervalStrategy, Message, NotPersistentActor, Response,
    ShutdownReason, Strategy, SupervisionStrategy,
};
use test_log::test;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

mod helpers;

#[derive(Debug, Clone)]
enum WatchMsg {
    Watch(ActorRef<TargetActor>),
    Unwatch(ActorRef<TargetActor>),
    Terminated(ActorPath),
    GetNotifications,
}

impl Message for WatchMsg {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WatchResponse {
    notifications: Vec<ActorPath>,
}

impl Response for WatchResponse {}

#[derive(Clone)]
struct WatchActor {
    notifications: Arc<Mutex<Vec<ActorPath>>>,
}

impl NotPersistentActor for WatchActor {}

#[async_trait]
impl Actor for WatchActor {
    type Message = WatchMsg;
    type Response = WatchResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("WatchActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for WatchActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: WatchMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<WatchResponse, Error> {
        match msg {
            WatchMsg::Watch(target) => {
                ctx.watch(&target, WatchMsg::Terminated).await?;
            }
            WatchMsg::Unwatch(target) => {
                ctx.unwatch(&target);
            }
            WatchMsg::Terminated(path) => {
                self.notifications.lock().await.push(path);
            }
            WatchMsg::GetNotifications => {
                return Ok(WatchResponse {
                    notifications: self.notifications.lock().await.clone(),
                });
            }
        }
        Ok(WatchResponse {
            notifications: vec![],
        })
    }
}

#[derive(Debug, Clone)]
enum TargetMsg {
    Stop,
    Fail,
    GetStopped,
}

impl Message for TargetMsg {}

#[derive(Debug, Clone)]
struct TargetResponse {
    stopped: bool,
}

impl Response for TargetResponse {}

#[derive(Clone)]
struct TargetActor {
    stopped: Arc<Mutex<bool>>,
}

impl NotPersistentActor for TargetActor {}

#[async_trait]
impl Actor for TargetActor {
    type Message = TargetMsg;
    type Response = TargetResponse;
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

    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Retry(Strategy::Interval(IntervalStrategy::new(
            3,
            Duration::from_millis(10),
        )))
    }
}

#[async_trait]
impl Handler<Self> for TargetActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: TargetMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<TargetResponse, Error> {
        match msg {
            TargetMsg::Stop => {
                *self.stopped.lock().await = true;
                ctx.stop(None).await;
            }
            TargetMsg::Fail => {
                return Err(Error::FunctionalCritical {
                    description: "forced failure".to_owned(),
                });
            }
            TargetMsg::GetStopped => {
                return Ok(TargetResponse {
                    stopped: *self.stopped.lock().await,
                });
            }
        }
        Ok(TargetResponse { stopped: false })
    }
}

async fn join_runner(
    handle: tokio::task::JoinHandle<ShutdownReason>,
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
async fn test_watch_notifies_when_target_stops() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let watcher = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_ref = system.create_root_actor("watcher", watcher).await?;

    let target = TargetActor {
        stopped: Arc::new(Mutex::new(false)),
    };
    let target_path = ActorPath::from("/user/target");
    let target_ref = system.create_root_actor("target", target).await?;

    watcher_ref
        .tell(WatchMsg::Watch(target_ref.clone()))
        .await?;
    target_ref.tell(TargetMsg::Stop).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = watcher_ref.ask(WatchMsg::GetNotifications).await?;
        if resp.notifications.contains(&target_path) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "watcher was not notified".to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_unwatch_prevents_notification() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let watcher = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_ref = system.create_root_actor("watcher", watcher).await?;

    let target = TargetActor {
        stopped: Arc::new(Mutex::new(false)),
    };
    let target_ref = system.create_root_actor("target", target).await?;

    watcher_ref
        .tell(WatchMsg::Watch(target_ref.clone()))
        .await?;
    watcher_ref
        .tell(WatchMsg::Unwatch(target_ref.clone()))
        .await?;
    target_ref.tell(TargetMsg::Stop).await?;

    // timing: absence check — wait a full window so a stray notification
    // would have arrived before asserting none did.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let resp = watcher_ref.ask(WatchMsg::GetNotifications).await?;
    assert!(resp.notifications.is_empty());

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_multiple_watchers_receive_notification() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let watcher_a = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_a_ref =
        system.create_root_actor("watcher_a", watcher_a).await?;

    let watcher_b = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_b_ref =
        system.create_root_actor("watcher_b", watcher_b).await?;

    let target = TargetActor {
        stopped: Arc::new(Mutex::new(false)),
    };
    let target_path = ActorPath::from("/user/target");
    let target_ref = system.create_root_actor("target", target).await?;

    watcher_a_ref
        .tell(WatchMsg::Watch(target_ref.clone()))
        .await?;
    watcher_b_ref
        .tell(WatchMsg::Watch(target_ref.clone()))
        .await?;
    target_ref.tell(TargetMsg::Stop).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let a = watcher_a_ref.ask(WatchMsg::GetNotifications).await?;
        let b = watcher_b_ref.ask(WatchMsg::GetNotifications).await?;
        if a.notifications.contains(&target_path)
            && b.notifications.contains(&target_path)
        {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "not all watchers were notified".to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_watch_is_idempotent() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let watcher = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_ref = system.create_root_actor("watcher", watcher).await?;

    let target = TargetActor {
        stopped: Arc::new(Mutex::new(false)),
    };
    let target_path = ActorPath::from("/user/target");
    let target_ref = system.create_root_actor("target", target).await?;

    watcher_ref
        .tell(WatchMsg::Watch(target_ref.clone()))
        .await?;
    watcher_ref
        .tell(WatchMsg::Watch(target_ref.clone()))
        .await?;
    target_ref.tell(TargetMsg::Stop).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = watcher_ref.ask(WatchMsg::GetNotifications).await?;
        if resp
            .notifications
            .iter()
            .filter(|p| **p == target_path)
            .count()
            == 1
        {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "watch was not idempotent".to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_watch_already_stopped_target_notifies() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let target = TargetActor {
        stopped: Arc::new(Mutex::new(false)),
    };
    let target_path = ActorPath::from("/user/target");
    let target_ref = system.create_root_actor("target", target).await?;
    target_ref.ask_stop().await?;

    let watcher = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_ref = system.create_root_actor("watcher", watcher).await?;

    // Watching a stopped target succeeds and delivers the termination
    // message immediately (courtesy notification, no lost watch).
    watcher_ref.ask(WatchMsg::Watch(target_ref.clone())).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = watcher_ref.ask(WatchMsg::GetNotifications).await?;
        if resp.notifications.contains(&target_path) {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "watcher was not notified for stopped target"
                    .to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_watcher_termination_does_not_crash() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let watcher = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_ref = system.create_root_actor("watcher", watcher).await?;

    let target = TargetActor {
        stopped: Arc::new(Mutex::new(false)),
    };
    let target_ref = system.create_root_actor("target", target).await?;

    watcher_ref
        .tell(WatchMsg::Watch(target_ref.clone()))
        .await?;
    watcher_ref.ask_stop().await?;
    target_ref.tell(TargetMsg::Stop).await?;

    // timing: pacing pause so the target termination races a dead watcher
    // delivery before shutdown; absence of a crash is the assertion.
    tokio::time::sleep(Duration::from_millis(100)).await;

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_no_notification_on_target_restart() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let watcher = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_ref = system.create_root_actor("watcher", watcher).await?;

    let target = TargetActor {
        stopped: Arc::new(Mutex::new(false)),
    };
    let target_ref = system.create_root_actor("target", target).await?;

    watcher_ref
        .tell(WatchMsg::Watch(target_ref.clone()))
        .await?;
    target_ref.tell(TargetMsg::Fail).await?;

    // Poll until the actor restarts and answers instead of a fixed sleep.
    let resp = helpers::assert_eventually(
        "target restarts after failure",
        Duration::from_secs(2),
        || async { target_ref.ask(TargetMsg::GetStopped).await.ok() },
    )
    .await;
    assert!(!resp.stopped);

    let notifications = watcher_ref.ask(WatchMsg::GetNotifications).await?;
    assert!(notifications.notifications.is_empty());

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_watch_limit_rejected() -> Result<(), Error> {
    let config = ActorSystemConfig {
        max_watchers_per_actor: 1,
        ..ActorSystemConfig::default()
    };
    let (system, mut runner) = ActorSystem::create_with_config(
        CancellationToken::new(),
        CancellationToken::new(),
        config,
    )?;
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let watcher_a = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_a_ref =
        system.create_root_actor("watcher_a", watcher_a).await?;

    let watcher_b = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_b_ref =
        system.create_root_actor("watcher_b", watcher_b).await?;

    let target = TargetActor {
        stopped: Arc::new(Mutex::new(false)),
    };
    let target_ref = system.create_root_actor("target", target).await?;

    watcher_a_ref
        .tell(WatchMsg::Watch(target_ref.clone()))
        .await?;
    let result = watcher_b_ref.ask(WatchMsg::Watch(target_ref.clone())).await;
    assert!(
        matches!(result, Err(Error::InvalidConfiguration { .. })),
        "expected InvalidConfiguration when watcher limit exceeded, got {:?}",
        result
    );

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_concurrent_watch_and_stop_delivers_exactly_once()
-> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let watcher = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_ref = system.create_root_actor("watcher", watcher).await?;

    let target = TargetActor {
        stopped: Arc::new(Mutex::new(false)),
    };
    let target_path = ActorPath::from("/user/target");
    let target_ref = system.create_root_actor("target", target).await?;

    // Race the watch registration against the target's termination: the
    // old check-then-register order could lose the notification entirely.
    // Exactly one delivery is expected in every interleaving.
    watcher_ref
        .tell(WatchMsg::Watch(target_ref.clone()))
        .await?;
    target_ref.tell(TargetMsg::Stop).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = watcher_ref.ask(WatchMsg::GetNotifications).await?;
        let count = resp
            .notifications
            .iter()
            .filter(|p| **p == target_path)
            .count();
        if count == 1 {
            break;
        }
        if count > 1 {
            return Err(Error::Functional {
                description: "watcher was notified more than once".to_owned(),
            });
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "watcher was not notified during concurrent stop"
                    .to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    join_runner(runner_handle).await
}
