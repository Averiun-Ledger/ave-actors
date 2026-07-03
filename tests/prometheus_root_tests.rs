#[cfg(feature = "prometheus")]
mod tests {
    use async_trait::async_trait;
    use ave_actors::prometheus::{
        Registry, create_system_with_registry, encode_registry,
    };
    use ave_actors::{
        Actor, ActorContext, ActorError, ActorPath, ActorRef, Handler,
        NotPersistentActor,
    };
    use ave_actors_store::metrics::{STORE_METRICS_HELPER, StoreMetrics};
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use tracing::Span;

    struct Dummy;

    impl NotPersistentActor for Dummy {}

    #[async_trait]
    impl Handler<Dummy> for Dummy {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            _msg: (),
            _ctx: &mut ActorContext<Dummy>,
        ) -> Result<(), ActorError> {
            Ok(())
        }
    }

    #[async_trait]
    impl Actor for Dummy {
        type Message = ();
        type Response = ();
        type Event = ();
        type SinkEvent = ();
        type ChildError = ActorError;
        type ChildFault = ActorError;

        fn get_span(_id: &str, _parent: Option<Span>) -> Span {
            Span::current()
        }
    }

    #[tokio::test]
    async fn create_system_with_registry_registers_actor_and_store_metrics() {
        let mut registry = Registry::default();
        let graceful = CancellationToken::new();
        let crash = CancellationToken::new();
        let (system, mut runner) = create_system_with_registry(
            graceful.clone(),
            crash.clone(),
            &mut registry,
        );

        let run_handle = tokio::spawn(async move { runner.run().await });

        let actor_ref: ActorRef<Dummy> = system
            .create_root_actor("dummy", Dummy)
            .await
            .expect("dummy actor should start");

        actor_ref.ask(()).await.expect("ask should succeed");

        let store_metrics: Arc<StoreMetrics> = system
            .get_helper(STORE_METRICS_HELPER)
            .expect("store metrics helper should be installed");
        store_metrics.inc_errors(&Arc::from("/test"), "Recover");

        let body = encode_registry(&registry)
            .expect("registry should encode successfully");
        assert!(body.contains("ave_actors_actor_messages_processed_total"));
        assert!(body.contains("ave_actors_store_errors_total"));

        system.stop_system();
        let _ = run_handle.await.expect("runner task should complete");
    }
}
