use async_trait::async_trait;
use ave_actors_actor::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InternalEvent(usize);
impl Event for InternalEvent {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExternalNotification(String);
impl Event for ExternalNotification {}

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
        // Internal event (could be for persistence, though this actor is not persistent)
        // ctx.on_event(InternalEvent(self.counter), ctx).await;

        // External notification to sink
        ctx.publish_all(ExternalNotification(format!(
            "Counter is now {}",
            self.counter
        )));
        Ok(())
    }
}

struct TestSubscriber {
    notifications: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Subscriber<ExternalNotification> for TestSubscriber {
    async fn notify(
        &self,
        event: Arc<ExternalNotification>,
    ) -> Result<(), Error> {
        self.notifications.lock().await.push(event.0.clone());
        Ok(())
    }
}

#[tokio::test]
async fn test_custom_sink_event() {
    let (system, _) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());

    let actor = CustomSinkActor { counter: 0 };
    let actor_ref = system
        .create_root_actor("custom_sink", actor)
        .await
        .unwrap();

    let notifications = Arc::new(Mutex::new(Vec::new()));
    let subscriber = TestSubscriber {
        notifications: notifications.clone(),
    };

    let mut sink = Sink::new("notifications", None);
    sink.add("sub1", subscriber);
    actor_ref.register_sink(sink);

    actor_ref.ask(()).await.unwrap();
    actor_ref.ask(()).await.unwrap();

    // Give some time for the sink to process
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let received = notifications.lock().await;
    assert_eq!(received.len(), 2);
    assert_eq!(received[0], "Counter is now 1");
    assert_eq!(received[1], "Counter is now 2");
}
