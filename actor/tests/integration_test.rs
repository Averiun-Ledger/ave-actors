// Integrations tests for the actor module

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, ChildAction, Error,
    Event, Handler, Message, Response, Subscriber,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use test_log::test;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

mod helpers;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

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
impl Handler<Self> for TestActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        message: TestCommand,
        ctx: &mut ActorContext<Self>,
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
                ctx.publish_all(TestEvent(self.state));

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
        ctx: &mut ActorContext<Self>,
    ) {
        assert!(matches!(
            error,
            Error::Functional { ref description }
                if description == "Value is too high"
        ));
        ctx.publish_all(TestEvent(0));
    }

    // Handles child fault.
    async fn on_child_fault(
        &mut self,
        error: Error,
        ctx: &mut ActorContext<Self>,
    ) -> ChildAction {
        assert!(matches!(
            error,
            Error::Functional { ref description }
                if description == "Value produces a fault"
        ));
        ctx.publish_all(TestEvent(100));
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ChildActor", id = %id)
    }
}

// Implements handler for child actor.
#[async_trait]
impl Handler<Self> for ChildActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        message: ChildCommand,
        ctx: &mut ActorContext<Self>,
    ) -> Result<ChildResponse, Error> {
        match message {
            ChildCommand::SetState(value) => {
                if value <= 10 {
                    self.state = value;
                    ctx.publish_all(ChildEvent(self.state));
                    Ok(ChildResponse::None)
                } else if value > 10 && value < 100 {
                    ctx.get_parent::<TestActor>()
                        .await?
                        .emit_error(Error::Functional {
                            description: "Value is too high".to_owned(),
                        })
                        .await?;
                    Ok(ChildResponse::State(100))
                } else {
                    ctx.get_parent::<TestActor>()
                        .await?
                        .emit_fail(Error::Functional {
                            description: "Value produces a fault".to_owned(),
                        })
                        .await?;
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

    // Poll until pre_start has created the child instead of a fixed sleep.
    helpers::assert_eventually(
        "child created by parent pre_start",
        Duration::from_secs(2),
        || async {
            system
                .get_actor::<ChildActor>(&ActorPath::from("/user/parent/child"))
                .await
                .ok()
        },
    )
    .await;

    let child_actor = system
        .get_actor::<ChildActor>(&ActorPath::from("/user/parent/child"))
        .await
        .unwrap();

    let child_sub = CollectingChildSubscriber::new();
    let mut sink = child_actor
        .register_sink("child_events", None)
        .expect("valid sink");
    sink.add("sub1", child_sub.clone());

    parent_ref.tell(TestCommand::Increment(10)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(10));

    helpers::assert_eventually(
        "child receives Increment event",
        Duration::from_secs(2),
        || async {
            let events = child_sub.events.lock().await;
            if events.len() == 1 && events[0].0 == 10 {
                Some(())
            } else {
                None
            }
        },
    )
    .await;
    let response = child_actor.ask(ChildCommand::GetState).await.unwrap();
    assert_eq!(response, ChildResponse::State(10));

    parent_ref.tell(TestCommand::Decrement(2)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(8));

    helpers::assert_eventually(
        "child receives Decrement event",
        Duration::from_secs(2),
        || async {
            let events = child_sub.events.lock().await;
            if events.len() == 2 && events[1].0 == 8 {
                Some(())
            } else {
                None
            }
        },
    )
    .await;
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
    let mut sink = parent_ref
        .register_sink("parent_events", None)
        .expect("valid sink");
    sink.add("sub1", parent_sub.clone());

    parent_ref.tell(TestCommand::Increment(50)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(50));

    helpers::assert_eventually(
        "parent publishes child-error event",
        Duration::from_secs(2),
        || async {
            let events = parent_sub.events.lock().await;
            if events.len() == 1 && events[0].0 == 0 {
                Some(())
            } else {
                None
            }
        },
    )
    .await;
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
    helpers::assert_eventually(
        "child created by parent pre_start",
        Duration::from_secs(2),
        || async {
            system
                .get_actor::<ChildActor>(&ActorPath::from("/user/parent/child"))
                .await
                .ok()
        },
    )
    .await;
    let child_ref = system
        .get_actor::<ChildActor>(&ActorPath::from("/user/parent/child"))
        .await;
    assert!(child_ref.is_ok());

    let parent_sub = CollectingParentSubscriber::new();
    let mut sink = parent_ref
        .register_sink("parent_events", None)
        .expect("valid sink");
    sink.add("sub1", parent_sub.clone());

    parent_ref.tell(TestCommand::Increment(110)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(110));

    helpers::assert_eventually(
        "parent publishes child-fault event",
        Duration::from_secs(2),
        || async {
            let events = parent_sub.events.lock().await;
            if events.len() == 1 && events[0].0 == 100 {
                Some(())
            } else {
                None
            }
        },
    )
    .await;

    helpers::assert_eventually(
        "faulted child is removed",
        Duration::from_secs(2),
        || async {
            if system
                .get_actor::<ChildActor>(&ActorPath::from("/user/parent/child"))
                .await
                .is_err()
            {
                Some(())
            } else {
                None
            }
        },
    )
    .await;
    let child_ref = system
        .get_actor::<ChildActor>(&ActorPath::from("/user/parent/child"))
        .await;
    assert!(child_ref.is_err());
}
