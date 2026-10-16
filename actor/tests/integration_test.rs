// Integrations tests for the actor module

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ChildAction, Error, Event,
    Handler, Message, Response, TestProbe, TestSystem,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use test_log::test;
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
impl Message for TestEvent {}

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
impl Message for ChildEvent {}

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

#[test(tokio::test)]
async fn test_actor() {
    let harness = TestSystem::start();
    let system = harness.system();

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

    let child_probe = TestProbe::<ChildEvent>::new();
    let sink = child_actor
        .register_sink("child_events", None)
        .expect("valid sink");
    sink.add("sub1", child_probe.clone());

    parent_ref.tell(TestCommand::Increment(10)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(10));

    let first = child_probe
        .expect_msg(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(first.0, 10);
    let response = child_actor.ask(ChildCommand::GetState).await.unwrap();
    assert_eq!(response, ChildResponse::State(10));

    parent_ref.tell(TestCommand::Decrement(2)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(8));

    let second = child_probe
        .expect_msg(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(second.0, 8);
    let response = child_actor.ask(ChildCommand::GetState).await.unwrap();
    assert_eq!(response, ChildResponse::State(8));

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_actor_error() {
    let harness = TestSystem::start();
    let system = harness.system();

    let parent = TestActor { state: 0 };
    let parent_ref = system.create_root_actor("parent", parent).await.unwrap();

    let parent_probe = TestProbe::<TestEvent>::new();
    let sink = parent_ref
        .register_sink("parent_events", None)
        .expect("valid sink");
    sink.add("sub1", parent_probe.clone());

    parent_ref.tell(TestCommand::Increment(50)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(50));

    let evt = parent_probe
        .expect_msg(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(evt.0, 0);

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_actor_fault() {
    let harness = TestSystem::start();
    let system = harness.system();
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

    let parent_probe = TestProbe::<TestEvent>::new();
    let sink = parent_ref
        .register_sink("parent_events", None)
        .expect("valid sink");
    sink.add("sub1", parent_probe.clone());

    parent_ref.tell(TestCommand::Increment(110)).await.unwrap();
    let response = parent_ref.ask(TestCommand::GetState).await.unwrap();
    assert_eq!(response, TestResponse::State(110));

    let evt = parent_probe
        .expect_msg(Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(evt.0, 100);

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

    harness.shutdown().await;
}
