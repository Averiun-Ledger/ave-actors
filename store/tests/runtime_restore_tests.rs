//! Runtime fields must survive snapshot recovery (`restore_runtime`).
//!
//! Mirrors the Node owned/known split: `apply` branches on a
//! runtime-only key that snapshots never carry. Recovery replays
//! uncovered events onto the decoded base, so without the hook the
//! replay decides against placeholder keys and bakes the corruption
//! into the next snapshot. The `Legacy` actor pins that failure mode
//! itself, so the suite goes red — never vacuous — if the hook stops
//! running.

use std::collections::BTreeSet;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, EncryptedKey,
    Error as ActorError, Event, Handler, Message, Response, ShutdownReason,
    SystemRef,
};
use ave_actors_store::{memory::MemoryManager, store::PersistentActor};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use test_log::test;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info_span;

const LIVE_KEY: u64 = 7;

#[derive(Debug, Clone, Default)]
struct SplitState {
    owned: BTreeSet<u64>,
    known: BTreeSet<u64>,
    /// Runtime-only: never serialized, like a node key.
    runtime_key: u64,
}

impl BorshSerialize for SplitState {
    fn serialize<W: std::io::Write>(
        &self,
        writer: &mut W,
    ) -> std::io::Result<()> {
        BorshSerialize::serialize(&self.owned, writer)?;
        BorshSerialize::serialize(&self.known, writer)?;
        Ok(())
    }
}

impl BorshDeserialize for SplitState {
    fn deserialize_reader<R: std::io::Read>(
        reader: &mut R,
    ) -> std::io::Result<Self> {
        let owned = BTreeSet::<u64>::deserialize_reader(reader)?;
        let known = BTreeSet::<u64>::deserialize_reader(reader)?;
        Ok(Self {
            owned,
            known,
            runtime_key: 0,
        })
    }
}

#[derive(
    Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
struct Register {
    id: u64,
    owner: u64,
}

impl Event for Register {}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum SplitMsg {
    Register { id: u64, owner: u64 },
    GetSplit,
}

impl Message for SplitMsg {}

#[derive(Debug, Clone, PartialEq)]
enum SplitResp {
    Ok,
    Split {
        owned: Vec<u64>,
        known: Vec<u64>,
        key: u64,
    },
}

impl Response for SplitResp {}

fn restore_fixed(base: Arc<SplitState>, live: &SplitState) -> Arc<SplitState> {
    let mut out = (*base).clone();
    out.runtime_key = live.runtime_key;
    Arc::new(out)
}

const fn restore_legacy(
    base: Arc<SplitState>,
    _live: &SplitState,
) -> Arc<SplitState> {
    base
}

macro_rules! split_actor {
    ($name:ident, $restore:path) => {
        #[derive(Debug)]
        struct $name {
            state: Arc<SplitState>,
        }

        #[async_trait]
        impl Actor for $name {
            type Message = SplitMsg;
            type Response = SplitResp;
            type Event = Register;
            type SinkEvent = Self::Event;
            type ChildError = ActorError;
            type ChildFault = ActorError;

            fn get_span(
                id: &str,
                _parent_span: Option<tracing::Span>,
            ) -> tracing::Span {
                info_span!("split", actor = stringify!($name), id = %id)
            }

            async fn pre_start(
                &mut self,
                ctx: &mut ActorContext<Self>,
            ) -> Result<(), ActorError> {
                let db: MemoryManager = ctx
                    .system()
                    .get_helper("db")
                    .expect("db helper should be installed");
                let key: Option<EncryptedKey> =
                    ctx.system().get_helper("key");
                self.start_store("store", None, ctx, db, key).await
            }
        }

        #[async_trait]
        impl PersistentActor for $name {
            type InitParams = u64;
            type State = SplitState;

            fn prune_events_on_snapshot() -> bool {
                true
            }

            fn snapshot_every() -> Option<u64> {
                Some(2)
            }

            fn create_initial(key: Self::InitParams) -> Self {
                let mut state = SplitState::default();
                state.runtime_key = key;
                Self {
                    state: Arc::new(state),
                }
            }

            fn apply(
                state: Arc<Self::State>,
                event: &Self::Event,
            ) -> Result<Arc<Self::State>, ActorError> {
                let mut new_state = Arc::clone(&state);
                let inner = Arc::make_mut(&mut new_state);
                if inner.runtime_key == event.owner {
                    inner.owned.insert(event.id);
                } else {
                    inner.known.insert(event.id);
                }
                Ok(new_state)
            }

            fn restore_runtime(
                base: Arc<Self::State>,
                live: &Self::State,
            ) -> Arc<Self::State> {
                $restore(base, live)
            }

            fn state(&self) -> Arc<Self::State> {
                Arc::clone(&self.state)
            }

            fn set_state(&mut self, state: Arc<Self::State>) {
                // Runtime fields stay live, like Node: only maps cross.
                self.state = Arc::new(SplitState {
                    owned: state.owned.clone(),
                    known: state.known.clone(),
                    runtime_key: self.state.runtime_key,
                });
            }
        }

        #[async_trait]
        impl Handler<Self> for $name {
            async fn handle_message(
                &mut self,
                _sender: ActorPath,
                msg: SplitMsg,
                ctx: &mut ActorContext<Self>,
            ) -> Result<SplitResp, ActorError> {
                match msg {
                    SplitMsg::Register { id, owner } => {
                        self.persist(Register { id, owner }, ctx).await?;
                        Ok(SplitResp::Ok)
                    }
                    SplitMsg::GetSplit => Ok(SplitResp::Split {
                        owned: self.state.owned.iter().cloned().collect(),
                        known: self.state.known.iter().cloned().collect(),
                        key: self.state.runtime_key,
                    }),
                }
            }
        }
    };
}

split_actor!(SplitActor, restore_fixed);
split_actor!(LegacySplitActor, restore_legacy);

fn boot(manager: MemoryManager) -> (SystemRef, JoinHandle<ShutdownReason>) {
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    system.add_helper("db", manager);
    let handle = tokio::spawn(async move { runner.run().await });
    (system, handle)
}

async fn expect_split(resp: SplitResp, owned: &[u64], known: &[u64]) {
    match resp {
        SplitResp::Split {
            owned: o,
            known: k,
            key,
        } => {
            assert_eq!(key, LIVE_KEY, "runtime key must survive reboot");
            assert_eq!(o, owned, "owned split diverged after recovery");
            assert_eq!(k, known, "known split diverged after recovery");
        }
        other => panic!("expected Split, got {other:?}"),
    }
}

#[test(tokio::test)]
async fn tail_replay_restores_runtime_fields() {
    let manager = MemoryManager::default();
    let (system, runner) = boot(manager.clone());
    let actor = system
        .create_root_actor("split", SplitActor::initial(LIVE_KEY))
        .await
        .unwrap();
    // Snapshot at 2 covers [0, 2) and prunes them; id 3 stays tail.
    for (id, owner) in [(1, LIVE_KEY), (2, 9), (3, LIVE_KEY)] {
        actor.ask(SplitMsg::Register { id, owner }).await.unwrap();
    }
    drop(actor);
    // Crash: no pre-stop snapshot, the tail must replay on reboot.
    runner.abort();
    drop(system);

    let (system, _runner) = boot(manager);
    let actor = system
        .create_root_actor("split", SplitActor::initial(LIVE_KEY))
        .await
        .unwrap();
    let resp = actor.ask(SplitMsg::GetSplit).await.unwrap();
    expect_split(resp, &[1, 3], &[2]).await;
    system.stop_system();
}

#[test(tokio::test)]
async fn unfixed_tail_replay_misplaces_split() {
    let manager = MemoryManager::default();
    let (system, runner) = boot(manager.clone());
    let actor = system
        .create_root_actor("split", LegacySplitActor::initial(LIVE_KEY))
        .await
        .unwrap();
    for (id, owner) in [(1, LIVE_KEY), (2, 9), (3, LIVE_KEY)] {
        actor.ask(SplitMsg::Register { id, owner }).await.unwrap();
    }
    drop(actor);
    runner.abort();
    drop(system);

    let (system, _runner) = boot(manager);
    let actor = system
        .create_root_actor("split", LegacySplitActor::initial(LIVE_KEY))
        .await
        .unwrap();
    // Pins the historical failure: id 3 replayed against placeholder
    // keys lands in `known`. set_state still preserves the live key.
    let resp = actor.ask(SplitMsg::GetSplit).await.unwrap();
    expect_split(resp, &[1], &[2, 3]).await;
    system.stop_system();
}

#[test(tokio::test)]
async fn prestop_snapshot_keeps_split() {
    let manager = MemoryManager::default();
    let (system, runner) = boot(manager.clone());
    let actor = system
        .create_root_actor("split", SplitActor::initial(LIVE_KEY))
        .await
        .unwrap();
    for (id, owner) in [(1, LIVE_KEY), (2, 9), (3, LIVE_KEY)] {
        actor.ask(SplitMsg::Register { id, owner }).await.unwrap();
    }
    drop(actor);
    // Graceful stop: the pre-stop snapshot absorbs the tail with live
    // keys, so the reboot finds everything up to date.
    system.stop_system();
    runner.await.unwrap();

    let (system, _runner) = boot(manager);
    let actor = system
        .create_root_actor("split", SplitActor::initial(LIVE_KEY))
        .await
        .unwrap();
    let resp = actor.ask(SplitMsg::GetSplit).await.unwrap();
    expect_split(resp, &[1, 3], &[2]).await;
    system.stop_system();
}

#[test(tokio::test)]
async fn unfixed_prestop_snapshot_bakes_corruption() {
    let manager = MemoryManager::default();
    let (system, runner) = boot(manager.clone());
    let actor = system
        .create_root_actor("split", LegacySplitActor::initial(LIVE_KEY))
        .await
        .unwrap();
    for (id, owner) in [(1, LIVE_KEY), (2, 9), (3, LIVE_KEY)] {
        actor.ask(SplitMsg::Register { id, owner }).await.unwrap();
    }
    drop(actor);
    system.stop_system();
    runner.await.unwrap();

    let (system, _runner) = boot(manager);
    let actor = system
        .create_root_actor("split", LegacySplitActor::initial(LIVE_KEY))
        .await
        .unwrap();
    // The pre-stop snapshot itself absorbed the tail with placeholder
    // keys: the corruption is now durable, reboot only replays it.
    let resp = actor.ask(SplitMsg::GetSplit).await.unwrap();
    expect_split(resp, &[1], &[2, 3]).await;
    system.stop_system();
}
