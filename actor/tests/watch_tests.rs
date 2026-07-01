//! Tests for `ActorContext::watch` / `unwatch` (Death Watch).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, Error, Handler,
    IntervalStrategy, Message, NotPersistentActor, Response, ShutdownReason,
    Strategy, SupervisionStrategy,
};
use test_log::test;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

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
                ctx.unwatch(&target).await;
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
async fn test_watch_already_stopped_target_fails() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let target = TargetActor {
        stopped: Arc::new(Mutex::new(false)),
    };
    let target_ref = system.create_root_actor("target", target).await?;
    target_ref.ask_stop().await?;

    let watcher = WatchActor {
        notifications: Arc::new(Mutex::new(vec![])),
    };
    let watcher_ref = system.create_root_actor("watcher", watcher).await?;

    let result = watcher_ref.ask(WatchMsg::Watch(target_ref.clone())).await;
    assert_eq!(result, Err(Error::ActorStopped));

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

    // Wait enough time for a retry/restart cycle to happen.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Ensure the target is still alive (it restarted).
    let resp = target_ref.ask(TargetMsg::GetStopped).await?;
    assert!(!resp.stopped);

    let notifications = watcher_ref.ask(WatchMsg::GetNotifications).await?;
    assert!(notifications.notifications.is_empty());

    system.stop_system();
    join_runner(runner_handle).await
}
