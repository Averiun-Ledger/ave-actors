use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, Error, Event, Handler, Message,
    NotPersistentActor, TestProbe, TestSystem,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::info_span;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InternalEvent(usize);
impl Event for InternalEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExternalNotification(String);
impl Event for ExternalNotification {}
impl Message for ExternalNotification {}

struct CustomSinkActor {
    counter: usize,
}

impl NotPersistentActor for CustomSinkActor {}

#[async_trait]
impl Actor for CustomSinkActor {
    type Message = ();
    type Event = InternalEvent;
    type SinkEvent = ExternalNotification;
    type Response = ();
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(id: &str, _parent: Option<tracing::Span>) -> tracing::Span {
        info_span!("CustomSinkActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for CustomSinkActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: (),
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        self.counter += 1;
        // Internal event (could be for persistence, though this actor
        // is not persistent)
        // ctx.on_event(InternalEvent(self.counter), ctx).await;

        // External notification to sink
        ctx.publish_all(ExternalNotification(format!(
            "Counter is now {}",
            self.counter
        )));
        Ok(())
    }
}

#[tokio::test]
async fn test_custom_sink_event() {
    let harness = TestSystem::start();
    let system = harness.system();

    let actor = CustomSinkActor { counter: 0 };
    let actor_ref = system
        .create_root_actor("custom_sink", actor)
        .await
        .unwrap();

    let probe = TestProbe::<ExternalNotification>::new();
    let sink = actor_ref
        .register_sink("notifications", None)
        .expect("valid sink");
    sink.add("sub1", probe.clone());

    actor_ref.ask(()).await.unwrap();
    actor_ref.ask(()).await.unwrap();

    let received = probe.expect_count(2, Duration::from_secs(2)).await.unwrap();
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].0, "Counter is now 1");
    assert_eq!(received[1].0, "Counter is now 2");

    harness.shutdown().await;
}
