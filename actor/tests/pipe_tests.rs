//! `pipe_to` tests: delivery, non-blocking, dead-target drop, abort.

use std::time::Duration;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, Error, Event, Handler, Message,
    NotPersistentActor, TestProbe, TestSystem, pipe_to,
};
use serde::{Deserialize, Serialize};
use test_log::test;
use tracing::info_span;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum PipeEv {
    Ping,
    Done(u64),
}

impl Message for PipeEv {}

impl Event for PipeEv {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CtlMsg {
    Begin,
    Ping,
}

impl Message for CtlMsg {}

#[derive(Debug, Clone)]
struct Controller {
    probe: ActorRef<ave_actors_actor::ProbeActor<PipeEv>>,
}

impl NotPersistentActor for Controller {}

#[async_trait]
impl Actor for Controller {
    type Message = CtlMsg;
    type Response = ();
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("PipeController", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for Controller {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: CtlMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        match msg {
            // The slow work leaves the actor: handled as a detached
            // task, never awaited here.
            CtlMsg::Begin => {
                pipe_to(
                    async {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        PipeEv::Done(1)
                    },
                    self.probe.clone(),
                );
                Ok(())
            }
            CtlMsg::Ping => {
                self.probe.tell(PipeEv::Ping).await?;
                Ok(())
            }
        }
    }
}

#[test(tokio::test)]
async fn test_pipe_delivers_future_output() {
    let harness = TestSystem::start();
    let probe = TestProbe::new();
    let probe_ref = probe.spawn(harness.system(), "probe").await.unwrap();

    pipe_to(async { PipeEv::Done(41) }, probe_ref);
    let got = probe.expect_msg(Duration::from_secs(2)).await.unwrap();
    assert_eq!(got, PipeEv::Done(41));

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_pipe_does_not_block_actor() {
    let harness = TestSystem::start();
    let probe = TestProbe::new();
    let probe_ref = probe.spawn(harness.system(), "probe").await.unwrap();
    let controller = harness
        .system()
        .create_root_actor("controller", Controller { probe: probe_ref })
        .await
        .unwrap();

    // Start the 200ms background job, then talk immediately: the Ping
    // must arrive first even though it was sent second.
    controller.tell(CtlMsg::Begin).await.unwrap();
    controller.tell(CtlMsg::Ping).await.unwrap();

    let got = probe.expect_count(2, Duration::from_secs(2)).await.unwrap();
    assert_eq!(got, vec![PipeEv::Ping, PipeEv::Done(1)]);

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_pipe_to_dead_actor_drops_silently() {
    let harness = TestSystem::start();
    let probe = TestProbe::<PipeEv>::new();
    let probe_ref = probe.spawn(harness.system(), "probe").await.unwrap();
    probe_ref.ask_stop().await.unwrap();

    let handle = pipe_to(
        async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            PipeEv::Done(9)
        },
        probe_ref,
    );
    // The task itself still completes fine; only delivery is skipped.
    handle.await.unwrap();

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_pipe_abort_cancels_delivery() {
    let harness = TestSystem::start();
    let probe = TestProbe::new();
    let probe_ref = probe.spawn(harness.system(), "probe").await.unwrap();

    let handle = pipe_to(
        async {
            std::future::pending::<()>().await;
            PipeEv::Done(1)
        },
        probe_ref,
    );
    handle.abort();
    probe
        .expect_no_msg(Duration::from_millis(200))
        .await
        .unwrap();

    harness.shutdown().await;
}
