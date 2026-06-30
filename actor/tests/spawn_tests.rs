//! Tests for `ActorContext::spawn`, a `tokio::spawn` wrapper whose tasks are
//! aborted when the actor stops.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, Error, Handler,
    Message, NotPersistentActor, Response, ShutdownReason,
};
use serde::{Deserialize, Serialize};
use test_log::test;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SpawnMsg {
    RunTask,
    RunLongTask,
    SendToOther,
    GetFlag,
}

impl Message for SpawnMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpawnResponse {
    value: bool,
}

impl Response for SpawnResponse {}

#[derive(Clone)]
struct SpawnActor {
    flag: Arc<Mutex<bool>>,
    target: Option<ActorRef<TargetActor>>,
}

impl NotPersistentActor for SpawnActor {}

#[async_trait]
impl Actor for SpawnActor {
    type Message = SpawnMsg;
    type Response = SpawnResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("SpawnActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for SpawnActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: SpawnMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<SpawnResponse, Error> {
        match msg {
            SpawnMsg::RunTask => {
                let flag = self.flag.clone();
                ctx.spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    *flag.lock().await = true;
                });
            }
            SpawnMsg::RunLongTask => {
                let flag = self.flag.clone();
                ctx.spawn(async move {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    *flag.lock().await = true;
                });
            }
            SpawnMsg::SendToOther => {
                if let Some(target) = self.target.clone() {
                    ctx.spawn(async move {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        let _ = target.tell(TargetMsg::Ping).await;
                    });
                }
            }
            SpawnMsg::GetFlag => {
                return Ok(SpawnResponse {
                    value: *self.flag.lock().await,
                });
            }
        }
        Ok(SpawnResponse { value: false })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TargetMsg {
    Ping,
    GetReceived,
}

impl Message for TargetMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TargetResponse {
    received: bool,
}

impl Response for TargetResponse {}

#[derive(Clone)]
struct TargetActor {
    received: Arc<Mutex<bool>>,
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
}

#[async_trait]
impl Handler<Self> for TargetActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: TargetMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<TargetResponse, Error> {
        match msg {
            TargetMsg::Ping => {
                *self.received.lock().await = true;
            }
            TargetMsg::GetReceived => {
                return Ok(TargetResponse {
                    received: *self.received.lock().await,
                });
            }
        }
        Ok(TargetResponse { received: false })
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
async fn test_spawn_runs_task() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = SpawnActor {
        flag: Arc::new(Mutex::new(false)),
        target: None,
    };
    let actor_ref = system.create_root_actor("spawn_runs", actor).await?;

    actor_ref.tell(SpawnMsg::RunTask).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = actor_ref.ask(SpawnMsg::GetFlag).await?;
        if resp.value {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "spawned task did not run".to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_spawn_aborted_on_actor_stop() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let actor = SpawnActor {
        flag: Arc::new(Mutex::new(false)),
        target: None,
    };
    let flag = actor.flag.clone();
    let actor_ref = system.create_root_actor("spawn_aborted", actor).await?;

    actor_ref.tell(SpawnMsg::RunLongTask).await?;
    // Give the spawned task time to start, then stop the actor.
    tokio::time::sleep(Duration::from_millis(50)).await;
    actor_ref.ask_stop().await?;

    // Wait briefly; the task would have set the flag only after 10 seconds.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!*flag.lock().await, "spawned task should have been aborted");

    system.stop_system();
    join_runner(runner_handle).await
}

#[test(tokio::test)]
async fn test_spawn_can_send_to_other_actor() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    let runner_handle = tokio::spawn(async move { runner.run().await });

    let target = TargetActor {
        received: Arc::new(Mutex::new(false)),
    };
    let target_ref = system.create_root_actor("spawn_target", target).await?;

    let actor = SpawnActor {
        flag: Arc::new(Mutex::new(false)),
        target: Some(target_ref.clone()),
    };
    let actor_ref = system.create_root_actor("spawn_sender", actor).await?;

    actor_ref.tell(SpawnMsg::SendToOther).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let resp = target_ref.ask(TargetMsg::GetReceived).await?;
        if resp.received {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: "target actor did not receive delayed message"
                    .to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    system.stop_system();
    join_runner(runner_handle).await
}
