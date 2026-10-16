//! Metadata and middleware tests: automatic ids, correlation
//! chains, `tell_with` / `ask_with`, and interceptor hooks.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorRef, Error, Handler, Intercept,
    Interceptor, Message, MessageMetadata, NotPersistentActor, TestSystem,
};
use serde::{Deserialize, Serialize};
use test_log::test;
use tracing::info_span;

// ---------------------------------------------------------------------------
// Actors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Work;

impl Message for Work {}

/// Records the metadata it observed per handled message.
#[derive(Debug, Clone)]
struct MetaRecorder {
    seen: Arc<Mutex<Vec<MessageMetadata>>>,
    downstream: Option<ActorRef<Self>>,
    /// When set, forwards with the incoming correlation chain.
    propagate: bool,
}

impl NotPersistentActor for MetaRecorder {}

#[async_trait]
impl Actor for MetaRecorder {
    type Message = Work;
    type Response = ();
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("MetaRecorder", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for MetaRecorder {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: Work,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        let metadata = ctx.message_metadata().expect("runner sets metadata");
        self.seen.lock().unwrap().push(metadata);
        if let Some(downstream) = &self.downstream {
            if self.propagate {
                let correlation =
                    ctx.correlation_id().expect("correlation present");
                downstream.tell_with(Work, correlation).await?;
            } else {
                downstream.tell(Work).await?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct RecordingInterceptor {
    before: Arc<Mutex<Vec<(String, String, u64)>>>,
    after: Arc<Mutex<Vec<(String, bool, u64)>>>,
}

impl Interceptor for RecordingInterceptor {
    fn before_handle(&self, ctx: Intercept<'_>) {
        self.before.lock().unwrap().push((
            ctx.path.to_string(),
            ctx.kind.to_owned(),
            ctx.metadata.correlation_id,
        ));
    }

    fn after_handle(
        &self,
        ctx: Intercept<'_>,
        result: &Result<(), Error>,
        elapsed: Duration,
    ) {
        let _ = elapsed;
        self.after.lock().unwrap().push((
            ctx.path.to_string(),
            result.is_ok(),
            ctx.metadata.id,
        ));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test(tokio::test)]
async fn test_messages_carry_unique_ids() {
    let harness = TestSystem::start();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = harness
        .system()
        .create_root_actor(
            "recorder",
            MetaRecorder {
                seen: Arc::clone(&seen),
                downstream: None,
                propagate: false,
            },
        )
        .await
        .unwrap();

    recorder.tell(Work).await.unwrap();
    recorder.tell(Work).await.unwrap();

    let deadline = Duration::from_secs(2);
    let start = std::time::Instant::now();
    loop {
        if seen.lock().unwrap().len() >= 2 {
            break;
        }
        assert!(start.elapsed() < deadline, "messages never handled");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let seen: Vec<_> = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert_ne!(seen[0].id, seen[1].id, "ids must be unique");
    // Fresh tells start their own chain.
    assert_eq!(seen[0].correlation_id, seen[0].id);
    assert_eq!(seen[1].correlation_id, seen[1].id);
    assert!(seen[1].sent_at >= seen[0].sent_at);

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_correlation_propagates_downstream() {
    let harness = TestSystem::start();
    let system = harness.system();
    let tail_seen = Arc::new(Mutex::new(Vec::new()));
    let tail = system
        .create_root_actor(
            "tail",
            MetaRecorder {
                seen: Arc::clone(&tail_seen),
                downstream: None,
                propagate: false,
            },
        )
        .await
        .unwrap();
    let head_seen = Arc::new(Mutex::new(Vec::new()));
    let head = system
        .create_root_actor(
            "head",
            MetaRecorder {
                seen: Arc::clone(&head_seen),
                downstream: Some(tail.clone()),
                propagate: true,
            },
        )
        .await
        .unwrap();

    head.tell(Work).await.unwrap();

    let deadline = Duration::from_secs(2);
    let start = std::time::Instant::now();
    loop {
        if !tail_seen.lock().unwrap().is_empty() {
            break;
        }
        assert!(start.elapsed() < deadline, "chain never completed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let head_meta = head_seen.lock().unwrap()[0];
    let tail_meta = tail_seen.lock().unwrap()[0];
    assert_ne!(head_meta.id, tail_meta.id, "distinct messages");
    assert_eq!(
        head_meta.correlation_id, tail_meta.correlation_id,
        "correlation must travel downstream"
    );

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_tell_without_propagation_starts_new_chain() {
    let harness = TestSystem::start();
    let system = harness.system();
    let tail_seen = Arc::new(Mutex::new(Vec::new()));
    let tail = system
        .create_root_actor(
            "tail",
            MetaRecorder {
                seen: Arc::clone(&tail_seen),
                downstream: None,
                propagate: false,
            },
        )
        .await
        .unwrap();
    let head = system
        .create_root_actor(
            "head",
            MetaRecorder {
                seen: Arc::new(Mutex::new(Vec::new())),
                downstream: Some(tail.clone()),
                propagate: false,
            },
        )
        .await
        .unwrap();

    head.tell(Work).await.unwrap();

    let deadline = Duration::from_secs(2);
    let start = std::time::Instant::now();
    loop {
        if !tail_seen.lock().unwrap().is_empty() {
            break;
        }
        assert!(start.elapsed() < deadline, "chain never completed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let tail_meta = tail_seen.lock().unwrap()[0];
    assert_eq!(
        tail_meta.correlation_id, tail_meta.id,
        "plain tells start a fresh chain"
    );
    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_interceptor_observes_messages() {
    let harness = TestSystem::start();
    let interceptor = RecordingInterceptor::default();
    harness.system().add_interceptor(interceptor.clone());

    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = harness
        .system()
        .create_root_actor(
            "recorder",
            MetaRecorder {
                seen: Arc::clone(&seen),
                downstream: None,
                propagate: false,
            },
        )
        .await
        .unwrap();
    recorder.tell(Work).await.unwrap();
    recorder.ask(Work).await.unwrap();

    let deadline = Duration::from_secs(2);
    let start = std::time::Instant::now();
    loop {
        if interceptor.after.lock().unwrap().len() >= 2 {
            break;
        }
        assert!(start.elapsed() < deadline, "hooks never ran");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let before = interceptor.before.lock().unwrap().clone();
    let after = interceptor.after.lock().unwrap().clone();
    assert_eq!(before.len(), 2);
    assert_eq!(after.len(), 2);
    assert_eq!(before[0].0, "/user/recorder");
    assert_eq!(before[0].1, "tell");
    assert_eq!(before[1].1, "ask");
    assert!(after.iter().all(|(_, ok, _)| *ok));
    // Two independent tells start two independent chains.
    assert_ne!(
        before[0].2, before[1].2,
        "each fresh tell starts its own correlation chain"
    );
    assert_ne!(after[0].2, after[1].2, "message ids are unique");

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_interceptor_registered_late_does_not_apply() {
    let harness = TestSystem::start();
    // Spawned BEFORE any interceptor exists: snapshots an empty registry.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorder = harness
        .system()
        .create_root_actor(
            "recorder",
            MetaRecorder {
                seen: Arc::clone(&seen),
                downstream: None,
                propagate: false,
            },
        )
        .await
        .unwrap();

    let interceptor = RecordingInterceptor::default();
    harness.system().add_interceptor(interceptor.clone());

    recorder.tell(Work).await.unwrap();
    let deadline = Duration::from_secs(2);
    let start = std::time::Instant::now();
    loop {
        if !seen.lock().unwrap().is_empty() {
            break;
        }
        assert!(start.elapsed() < deadline, "message never handled");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Give a would-be hook every chance to fire.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        interceptor.before.lock().unwrap().is_empty(),
        "late interceptors must not observe pre-existing actors"
    );

    harness.shutdown().await;
}
