//! Tests for the named registry (`register_name`) and `ActorSelection`.

mod helpers;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSelection, Error, Handler, Message,
    NotPersistentActor, Response, TestSystem,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio::sync::Mutex;
use tracing::info_span;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SelMsg {
    Ping,
    Count,
}

impl Message for SelMsg {}

#[derive(Debug, Clone, PartialEq)]
enum SelResp {
    Pong,
    Hits(u32),
}

impl Response for SelResp {}

#[derive(Clone)]
struct SelActor {
    hits: Arc<Mutex<u32>>,
}

impl NotPersistentActor for SelActor {}

#[async_trait]
impl Actor for SelActor {
    type Message = SelMsg;
    type Response = SelResp;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("SelActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for SelActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: SelMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<SelResp, Error> {
        match msg {
            SelMsg::Ping => {
                *self.hits.lock().await += 1;
                Ok(SelResp::Pong)
            }
            SelMsg::Count => Ok(SelResp::Hits(*self.hits.lock().await)),
        }
    }
}

#[test(tokio::test)]
async fn test_register_resolve_unregister() {
    let harness = TestSystem::start();
    let system = harness.system();

    let actor_ref = system
        .create_root_actor(
            "a",
            SelActor {
                hits: Arc::new(Mutex::new(0)),
            },
        )
        .await
        .unwrap();

    // Unknown names resolve to nothing.
    let sel: ActorSelection = system.select_name("pagos");
    assert!(sel.resolve::<SelActor>().await.is_empty());

    system.register_name("pagos", actor_ref.path()).unwrap();
    let resolved = sel.resolve::<SelActor>().await;
    assert_eq!(resolved.len(), 1);

    // Duplicate registration is rejected, not hijacked.
    assert!(system.register_name("pagos", actor_ref.path()).is_err());

    // Invalid names are rejected.
    assert!(
        system
            .register_name("no/slashes", actor_ref.path())
            .is_err()
    );
    assert!(system.register_name("", actor_ref.path()).is_err());
    assert!(
        system
            .register_name("x".repeat(300).as_str(), actor_ref.path())
            .is_err()
    );

    system.unregister_name("pagos");
    assert!(sel.resolve::<SelActor>().await.is_empty());

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_pattern_broadcast_and_ask() {
    let harness = TestSystem::start();
    let system = harness.system();

    for name in ["w-1", "w-2"] {
        system
            .create_root_actor(
                name,
                SelActor {
                    hits: Arc::new(Mutex::new(0)),
                },
            )
            .await
            .unwrap();
    }
    system
        .create_root_actor(
            "other",
            SelActor {
                hits: Arc::new(Mutex::new(0)),
            },
        )
        .await
        .unwrap();

    let sel = system.select_pattern("/user/w-*");
    assert_eq!(sel.resolve::<SelActor>().await.len(), 2);

    sel.tell::<SelActor>(SelMsg::Ping).await.unwrap();

    // Both workers saw exactly one ping; `other` saw none.
    for name in ["w-1", "w-2"] {
        let worker: ave_actors_actor::ActorRef<SelActor> = system
            .get_actor(&ActorPath::from(format!("/user/{name}").as_str()))
            .await
            .unwrap();
        assert_eq!(worker.ask(SelMsg::Count).await.unwrap(), SelResp::Hits(1));
    }
    let other: ave_actors_actor::ActorRef<SelActor> = system
        .get_actor(&ActorPath::from("/user/other"))
        .await
        .unwrap();
    assert_eq!(other.ask(SelMsg::Count).await.unwrap(), SelResp::Hits(0));

    // `ask` on a multi-match is ambiguous by design.
    let result = sel.ask::<SelActor>(SelMsg::Ping).await;
    assert!(
        matches!(result, Err(Error::Functional { .. })),
        "ambiguous ask must fail, got {result:?}"
    );

    // `ask` on an empty selection is NotFound.
    let empty = system.select_pattern("/user/nothing-*");
    assert!(matches!(
        empty.ask::<SelActor>(SelMsg::Ping).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        empty.tell::<SelActor>(SelMsg::Ping).await,
        Err(Error::NotFound { .. })
    ));

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_concurrent_register_name_has_single_winner() {
    let harness = TestSystem::start();
    let system = harness.system();

    let actor_ref = system
        .create_root_actor(
            "racer",
            SelActor {
                hits: Arc::new(Mutex::new(0)),
            },
        )
        .await
        .unwrap();

    // Ten tasks racing the same name: exactly one registration wins, the
    // rest get `Exists` — the name is never duplicated nor hijacked.
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..10 {
        let system = system.clone();
        let path = actor_ref.path();
        set.spawn(async move { system.register_name("race", path).is_ok() });
    }
    let mut wins = 0;
    while let Some(outcome) = set.join_next().await {
        if outcome.expect("registration task panicked") {
            wins += 1;
        }
    }
    assert_eq!(wins, 1, "exactly one concurrent registration must win");

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_dead_names_prune_and_reregister() {
    let harness = TestSystem::start();
    let system = harness.system();

    let actor_ref = system
        .create_root_actor(
            "ephemeral",
            SelActor {
                hits: Arc::new(Mutex::new(0)),
            },
        )
        .await
        .unwrap();
    system.register_name("temp", actor_ref.path()).unwrap();
    assert_eq!(
        system.select_name("temp").resolve::<SelActor>().await.len(),
        1
    );

    actor_ref.ask_stop().await.unwrap();

    // The dead entry prunes lazily on resolve (ask_stop already confirmed
    // termination, so no sleep is needed)...
    helpers::assert_eventually(
        "dead name prunes on resolve",
        std::time::Duration::from_secs(2),
        || async {
            if system
                .select_name("temp")
                .resolve::<SelActor>()
                .await
                .is_empty()
            {
                Some(())
            } else {
                None
            }
        },
    )
    .await;

    // ...so the name is free to re-register after a restart.
    let actor_ref2 = system
        .create_root_actor(
            "ephemeral",
            SelActor {
                hits: Arc::new(Mutex::new(0)),
            },
        )
        .await
        .unwrap();
    system.register_name("temp", actor_ref2.path()).unwrap();
    system
        .select_name("temp")
        .tell::<SelActor>(SelMsg::Ping)
        .await
        .unwrap();
    assert_eq!(
        actor_ref2.ask(SelMsg::Count).await.unwrap(),
        SelResp::Hits(1)
    );

    harness.shutdown().await;
}
