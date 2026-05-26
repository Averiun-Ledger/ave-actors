// Integrations tests for the actor module

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, ChildAction, Error,
    Event, Handler, Message, Response, Sink, Subscriber,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// Defines parent actor
#[derive(Debug, Clone)]
pub struct TestActor {
    pub state: usize,
}

impl ave_actors_actor::NotPersistentActor for TestActor {}

// Defines parent command
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TestCommand {
    Increment(usize),
    Decrement(usize),
    GetState,
}

// Implements message for parent command.
impl Message for TestCommand {}

// Defines parent response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TestResponse {
    State(usize),
    None,
}

// Implements response for parent response.
impl Response for TestResponse {}

// Defines parent event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestEvent(usize);

// Implements event for parent event.
impl Event for TestEvent {}

// Implements actor for parent actor.
#[async_trait]
impl Actor for TestActor {
    type Message = TestCommand;
    type Response = TestResponse;
    type Event = TestEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TestActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        let child = ChildActor { state: 0 };
        ctx.create_child("child", child).await?;
        Ok(())
    }
}

// Implements handler for parent actor.
#[async_trait]
impl Handler<TestActor> for TestActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        message: TestCommand,
        ctx: &mut ActorContext<TestActor>,
    ) -> Result<TestResponse, Error> {
        match message {
            TestCommand::Increment(value) => {
                self.state += value;
                let child: ActorRef<ChildActor> =
                    ctx.get_child("child").await.unwrap();
                child
                    .tell(ChildCommand::SetState(self.state))
                    .await
                    .unwrap();
                Ok(TestResponse::None)
            }
            TestCommand::Decrement(value) => {
                self.state -= value;
                ctx.publish_event(TestEvent(self.state));

                let child: ActorRef<ChildActor> =
                    ctx.get_child("child").await.unwrap();
                child
                    .tell(ChildCommand::SetState(self.state))
                    .await
                    .unwrap();
                Ok(TestResponse::None)
            }
            TestCommand::GetState => Ok(TestResponse::State(self.state)),
        }
    }

    // Handles child error.
    async fn on_child_error(
        &mut self,
        error: Error,
        ctx: &mut ActorContext<TestActor>,
    ) {
        assert_eq!(
            error,
            Error::Functional {
                description: "Value is too high".to_owned()
            }
        );
        ctx.publish_event(TestEvent(0));
    }

    // Handles child fault.
    async fn on_child_fault(
        &mut self,
        error: Error,
        ctx: &mut ActorContext<TestActor>,
    ) -> ChildAction {
        assert_eq!(
            error,
            Error::Functional {
                description: "Value produces a fault".to_owned()
            }
        );
        ctx.publish_event(TestEvent(100));
        ChildAction::Stop
    }
}

// Defines child actor.
#[derive(Debug, Clone)]
pub struct ChildActor {
    pub state: usize,
}

impl ave_actors_actor::NotPersistentActor for ChildActor {}

// Defines child command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChildCommand {
    SetState(usize),
    GetState,
}

// Implements message for child command.
impl Message for ChildCommand {}

// Defines child response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ChildResponse {
    State(usize),
    None,
}

// Implements response for child response.
impl Response for ChildResponse {}

// Defines child event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildEvent(usize);

// Implements event for child event.
impl Event for ChildEvent {}

// Implements actor for child actor.
#[async_trait]
impl Actor for ChildActor {
    type Message = ChildCommand;
    type Response = ChildResponse;
    type Event = ChildEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ChildActor", id = %id)
    }
}

// Implements handler for child actor.
#[async_trait]
impl Handler<ChildActor> for ChildActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        message: ChildCommand,
        ctx: &mut ActorContext<ChildActor>,
    ) -> Result<ChildResponse, Error> {
        match message {
            ChildCommand::SetState(value) => {
                if value <= 10 {
                    self.state = value;
                    ctx.publish_event(ChildEvent(self.state));
                    Ok(ChildResponse::None)
                } else if value > 10 && value < 100 {
                    ctx.emit_error(Error::Functional {
                        description: "Value is too high".to_owned(),
                    })
                    .await
                    .unwrap();
                    Ok(ChildResponse::State(100))
                } else {
                    ctx.emit_fail(Error::Functional {
                        description: "Value produces a fault".to_owned(),
                    })
                    .await
                    .unwrap();
                    Ok(ChildResponse::None)
                }
            }
            ChildCommand::GetState => Ok(ChildResponse::State(self.state)),
        }
    }
}

#[derive(Clone)]
struct CollectingChildSubscriber {
    events: Arc<Mutex<Vec<ChildEvent>>>,
}

impl CollectingChildSubscriber {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl Subscriber<ChildEvent> for CollectingChildSubscriber {
    async fn notify(&self, event: Arc<ChildEvent>) -> Result<(), Error> {
        self.events.lock().await.push((*event).clone());
        Ok(())
    }
}

#[derive(Clone)]
struct CollectingParentSubscriber {
    events: Arc<Mutex<Vec<TestEvent>>>,
}

impl CollectingParentSubscriber {
    fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl Subscriber<TestEvent> for CollectingParentSubscriber {
    async fn notify(&self, event: Arc<TestEvent>) -> Result<(), Error> {
        self.events.lock().await.push((*event).clone());
        Ok(())
    }
}

#[test(tokio::test)]
async fn test_actor() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move {
        runner.run().await;
    });

    let parent = TestActor { state: 0 };
    let parent_ref = system.create_root_actor("parent", parent).await.unwrap();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let child_actor = system
        .get_actor::<ChildActor>(&ActorPath::from("/user/parent/child"))
        .await
        .unwrap();

    let child_sub = CollectingChildSubscriber::new();
    let mut sink = Sink::new("child_events");
    sink.add("sub1", child_sub.clone());
    child_actor.register_sink(sink);

    parent_ref.tell(TestCommand::Increment(10)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(10));

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    {
        let events = child_sub.events.lock().await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, 10);
    }
    let response = child_actor.ask(ChildCommand::GetState).await.unwrap();
    assert_eq!(response, ChildResponse::State(10));

    parent_ref.tell(TestCommand::Decrement(2)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(8));

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    {
        let events = child_sub.events.lock().await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].0, 8);
    }
    let response = child_actor.ask(ChildCommand::GetState).await.unwrap();
    assert_eq!(response, ChildResponse::State(8));
}

#[test(tokio::test)]
async fn test_actor_error() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move {
        runner.run().await;
    });

    let parent = TestActor { state: 0 };
    let parent_ref = system.create_root_actor("parent", parent).await.unwrap();

    let parent_sub = CollectingParentSubscriber::new();
    let mut sink = Sink::new("parent_events");
    sink.add("sub1", parent_sub.clone());
    parent_ref.register_sink(sink);

    parent_ref.tell(TestCommand::Increment(50)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(50));

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let events = parent_sub.events.lock().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, 0);
}

#[test(tokio::test)]
async fn test_actor_fault() {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move {
        runner.run().await;
    });
    let parent = TestActor { state: 0 };
    let parent_ref = system.create_root_actor("parent", parent).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let child_ref = system
        .get_actor::<ChildActor>(&ActorPath::from("/user/parent/child"))
        .await;
    assert!(child_ref.is_ok());

    let parent_sub = CollectingParentSubscriber::new();
    let mut sink = Sink::new("parent_events");
    sink.add("sub1", parent_sub.clone());
    parent_ref.register_sink(sink);

    parent_ref.tell(TestCommand::Increment(110)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(110));

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let events = parent_sub.events.lock().await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, 100);

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    let child_ref = system
        .get_actor::<ChildActor>(&ActorPath::from("/user/parent/child"))
        .await;
    assert!(child_ref.is_err());
}
