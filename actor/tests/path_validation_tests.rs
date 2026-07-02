//! Tests for ActorPath validation at actor creation time.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, ActorSystem, Error, Handler,
    Message, NotPersistentActor, Response,
};
use serde::{Deserialize, Serialize};
use test_log::test;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DummyMsg;

impl Message for DummyMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreateChildMsg {
    name: String,
}

impl Message for CreateChildMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreateChildResponse {
    success: bool,
}

impl Response for CreateChildResponse {}

#[derive(Clone)]
struct DummyActor;

impl NotPersistentActor for DummyActor {}

#[async_trait]
impl Actor for DummyActor {
    type Message = DummyMsg;
    type Response = ();
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("DummyActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for DummyActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: DummyMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Clone)]
struct ParentActor;

impl NotPersistentActor for ParentActor {}

#[async_trait]
impl Actor for ParentActor {
    type Message = CreateChildMsg;
    type Response = CreateChildResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ParentActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for ParentActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: CreateChildMsg,
        ctx: &mut ActorContext<Self>,
    ) -> Result<CreateChildResponse, Error> {
        let result: Result<ActorRef<DummyActor>, Error> =
            ctx.create_child(&msg.name, DummyActor).await;
        Ok(CreateChildResponse {
            success: result.is_ok(),
        })
    }
}

#[test(tokio::test)]
async fn test_create_root_actor_rejects_invalid_name() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let result = system
        .create_root_actor::<DummyActor, _>("bad name", DummyActor)
        .await;
    assert!(
        result.is_err(),
        "root actor name with spaces should be rejected"
    );

    system.stop_system();
    Ok(())
}

#[test(tokio::test)]
async fn test_create_root_actor_accepts_valid_name() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor_ref = system
        .create_root_actor::<DummyActor, _>("valid-name_2", DummyActor)
        .await?;
    actor_ref.tell(DummyMsg).await?;

    system.stop_system();
    Ok(())
}

#[test(tokio::test)]
async fn test_create_child_rejects_invalid_name() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let parent_ref = system
        .create_root_actor::<ParentActor, _>("parent", ParentActor)
        .await?;
    let resp = parent_ref
        .ask(CreateChildMsg {
            name: "bad/name".to_owned(),
        })
        .await?;
    assert!(!resp.success, "child name with slash should be rejected");

    system.stop_system();
    Ok(())
}

#[test(tokio::test)]
async fn test_create_child_accepts_valid_name() -> Result<(), Error> {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let parent_ref = system
        .create_root_actor::<ParentActor, _>("parent2", ParentActor)
        .await?;
    let resp = parent_ref
        .ask(CreateChildMsg {
            name: "valid_child-1".to_owned(),
        })
        .await?;
    assert!(resp.success, "valid child name should be accepted");

    system.stop_system();
    Ok(())
}
