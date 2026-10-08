//! Comprehensive tests for Sink and Handler modules to increase coverage

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, Error, Event, Handler, Message, Response,
    Subscriber, TestProbe, TestSystem,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use test_log::test;
use tracing::info_span;

// Test structures for sink and handler testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkTestEvent {
    pub id: u32,
    pub data: String,
}

impl Event for SinkTestEvent {}
impl Message for SinkTestEvent {}

#[derive(Debug, Clone)]
pub struct TestActor {
    pub counter: u32,
}

impl ave_actors_actor::NotPersistentActor for TestActor {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TestMessage {
    Emit(u32, String),
    GetCounter,
}

impl Message for TestMessage {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestResponse {
    pub value: u32,
}

impl Response for TestResponse {}

#[async_trait]
impl Actor for TestActor {
    type Message = TestMessage;
    type Response = TestResponse;
    type Event = SinkTestEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TestActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for TestActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: TestMessage,
        ctx: &mut ActorContext<Self>,
    ) -> Result<TestResponse, Error> {
        match msg {
            TestMessage::Emit(id, data) => {
                self.counter += 1;
                ctx.publish_all(SinkTestEvent { id, data });
                Ok(TestResponse {
                    value: self.counter,
                })
            }
            TestMessage::GetCounter => Ok(TestResponse {
                value: self.counter,
            }),
        }
    }
}

// Failing subscriber for the error-isolation test (not a pure
// collector, so it stays hand-rolled).
#[derive(Clone)]
pub struct FailingSubscriber;

#[async_trait]
impl Subscriber<SinkTestEvent> for FailingSubscriber {
    async fn notify(&self, _event: Arc<SinkTestEvent>) -> Result<(), Error> {
        Err(Error::Functional {
            description: "Subscriber intentionally failed".to_owned(),
        })
    }
}

// Tests for Sink functionality

#[test(tokio::test)]
async fn test_sink_basic_functionality() {
    let harness = TestSystem::start();
    let system = harness.system();

    let actor = TestActor { counter: 0 };
    let actor_ref = system.create_root_actor("sink_test", actor).await.unwrap();

    let probe = TestProbe::<SinkTestEvent>::new();

    // Register sink on the actor
    let sink = actor_ref
        .register_sink("test_sink", None)
        .expect("valid sink");
    sink.add("sub1", probe.clone());

    // Emit some events (sink registration needs no warm-up sleep: sends queue
    // in the sink buffer regardless).
    actor_ref
        .tell(TestMessage::Emit(1, "test1".to_string()))
        .await
        .unwrap();
    actor_ref
        .tell(TestMessage::Emit(2, "test2".to_string()))
        .await
        .unwrap();
    actor_ref
        .tell(TestMessage::Emit(3, "test3".to_string()))
        .await
        .unwrap();

    let events = probe.expect_count(3, Duration::from_secs(2)).await.unwrap();

    // Verify events were collected
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].id, 1);
    assert_eq!(events[0].data, "test1");
    assert_eq!(events[1].id, 2);
    assert_eq!(events[1].data, "test2");
    assert_eq!(events[2].id, 3);
    assert_eq!(events[2].data, "test3");

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_sink_with_failing_subscriber() {
    let harness = TestSystem::start();
    let system = harness.system();

    let actor = TestActor { counter: 0 };
    let actor_ref = system
        .create_root_actor("failing_sink_test", actor)
        .await
        .unwrap();

    // Register sink with failing subscriber
    let sink = actor_ref
        .register_sink("failing_sink", None)
        .expect("valid sink");
    sink.add("sub1", FailingSubscriber);

    // Emit event - this should not crash the system even though
    // subscriber fails
    actor_ref
        .tell(TestMessage::Emit(1, "test".to_string()))
        .await
        .unwrap();

    // The following `ask` synchronizes with message processing, so no sleep
    // is needed before asserting the actor is still alive.
    let response = actor_ref.ask(TestMessage::GetCounter).await.unwrap();
    assert_eq!(response.value, 1);

    harness.shutdown().await;
}

// Tests for Handler functionality and error scenarios

// Actor that can fail in different ways
#[derive(Debug, Clone)]
pub struct FailingHandlerActor {
    pub fail_on_message: bool,
    pub fail_with_timeout: bool,
}

impl ave_actors_actor::NotPersistentActor for FailingHandlerActor {}

#[async_trait]
impl Actor for FailingHandlerActor {
    type Message = TestMessage;
    type Response = TestResponse;
    type Event = SinkTestEvent;
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("FailingHandlerActor", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for FailingHandlerActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: TestMessage,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<TestResponse, Error> {
        if self.fail_on_message {
            return Err(Error::Functional {
                description: "Handler intentionally failed".to_string(),
            });
        }

        if self.fail_with_timeout {
            // Simulate a very long operation
            // timing: long handler block so ask_timeout tests exercise the
            // timeout path deterministically.
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
        }

        match msg {
            TestMessage::GetCounter => Ok(TestResponse { value: 42 }),
            _ => Ok(TestResponse { value: 0 }),
        }
    }
}

#[test(tokio::test)]
async fn test_handler_error_scenarios() {
    let harness = TestSystem::start();
    let system = harness.system();

    let actor = FailingHandlerActor {
        fail_on_message: true,
        fail_with_timeout: false,
    };

    let actor_ref = system
        .create_root_actor("failing_handler", actor)
        .await
        .unwrap();

    // This should return an error
    let result = actor_ref.ask(TestMessage::GetCounter).await;
    assert!(result.is_err());

    match result {
        Err(Error::Functional { description }) => {
            assert_eq!(description, "Handler intentionally failed");
        }
        _ => panic!("Expected functional error"),
    }

    harness.shutdown().await;
}

#[test(tokio::test)]
async fn test_message_serialization_edge_cases() {
    // Test with complex message structures
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ComplexMessage {
        pub nested: Vec<std::collections::HashMap<String, i32>>,
        pub optional: Option<String>,
        pub tuple: (i32, String, bool),
    }

    impl Message for ComplexMessage {}

    #[derive(Debug, Clone)]
    pub struct ComplexHandlerActor;

    impl ave_actors_actor::NotPersistentActor for ComplexHandlerActor {}

    #[async_trait]
    impl Actor for ComplexHandlerActor {
        type Message = ComplexMessage;
        type Response = TestResponse;
        type Event = SinkTestEvent;
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("ComplexHandlerActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for ComplexHandlerActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: ComplexMessage,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<TestResponse, Error> {
            // Verify message was properly deserialized
            assert!(!msg.nested.is_empty());
            assert!(msg.optional.is_some());
            assert_eq!(msg.tuple.0, 42);

            Ok(TestResponse {
                value: msg.nested.len() as u32,
            })
        }
    }

    let harness = TestSystem::start();
    let system = harness.system();

    let actor = ComplexHandlerActor;
    let actor_ref = system
        .create_root_actor("complex_handler", actor)
        .await
        .unwrap();

    let mut nested = std::collections::HashMap::new();
    nested.insert("key1".to_string(), 100);
    nested.insert("key2".to_string(), 200);

    let complex_msg = ComplexMessage {
        nested: vec![nested],
        optional: Some("test".to_string()),
        tuple: (42, "tuple_test".to_string(), true),
    };

    let result = actor_ref.ask(complex_msg).await.unwrap();
    assert_eq!(result.value, 1);

    harness.shutdown().await;
}

// Test mailbox behavior and message ordering
#[test(tokio::test)]
async fn test_message_ordering_and_mailbox() {
    #[derive(Debug, Clone)]
    pub struct OrderingActor {
        pub received_order: Vec<u32>,
    }

    impl ave_actors_actor::NotPersistentActor for OrderingActor {}

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct OrderedMessage {
        pub sequence: u32,
    }

    impl Message for OrderedMessage {}

    #[async_trait]
    impl Actor for OrderingActor {
        type Message = OrderedMessage;
        type Response = TestResponse;
        type Event = SinkTestEvent;
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("OrderingActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for OrderingActor {
        async fn handle_message(
            &mut self,
            _sender: ActorPath,
            msg: OrderedMessage,
            _ctx: &mut ActorContext<Self>,
        ) -> Result<TestResponse, Error> {
            self.received_order.push(msg.sequence);
            Ok(TestResponse {
                value: self.received_order.len() as u32,
            })
        }
    }

    let harness = TestSystem::start();
    let system = harness.system();

    let actor = OrderingActor {
        received_order: Vec::new(),
    };
    let actor_ref = system
        .create_root_actor("ordering_actor", actor)
        .await
        .unwrap();

    // Send messages in sequence
    for i in 1..=5 {
        actor_ref
            .tell(OrderedMessage { sequence: i })
            .await
            .unwrap();
    }

    // The final `ask` is queued after all tells (FIFO mailbox), so it
    // synchronizes with their processing: no sleep needed.

    // Verify final count
    let result = actor_ref.ask(OrderedMessage { sequence: 0 }).await.unwrap();
    assert_eq!(result.value, 6); // 5 tells + 1 ask

    harness.shutdown().await;
}

// Test for handler with context operations
#[test(tokio::test)]
async fn test_handler_context_operations() {
    #[derive(Debug, Clone)]
    pub struct ContextActor {
        pub path_checked: bool,
        pub system_accessed: bool,
    }

    impl ave_actors_actor::NotPersistentActor for ContextActor {}

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum ContextMessage {
        CheckPath,
        AccessSystem,
        GetState,
    }

    impl Message for ContextMessage {}

    #[async_trait]
    impl Actor for ContextActor {
        type Message = ContextMessage;
        type Response = TestResponse;
        type Event = SinkTestEvent;
        type SinkEvent = Self::Event;
        type ChildError = Error;
        type ChildFault = Error;

        fn get_span(
            id: &str,
            _parent_span: Option<tracing::Span>,
        ) -> tracing::Span {
            info_span!("ContextActor", id = %id)
        }
    }

    #[async_trait]
    impl Handler<Self> for ContextActor {
        async fn handle_message(
            &mut self,
            sender: ActorPath,
            msg: ContextMessage,
            ctx: &mut ActorContext<Self>,
        ) -> Result<TestResponse, Error> {
            match msg {
                ContextMessage::CheckPath => {
                    let my_path = ctx.path();
                    assert_eq!(my_path.to_string(), "/user/context_actor");
                    assert!(!sender.is_empty()); // Should have a sender path
                    self.path_checked = true;
                    Ok(TestResponse { value: 1 })
                }
                ContextMessage::AccessSystem => {
                    let _system = ctx.system();
                    // Verify we can access system without panicking
                    self.system_accessed = true;
                    Ok(TestResponse { value: 2 })
                }
                ContextMessage::GetState => {
                    let state = (self.path_checked as u32)
                        + (self.system_accessed as u32);
                    Ok(TestResponse { value: state })
                }
            }
        }
    }

    let harness = TestSystem::start();
    let system = harness.system();

    let actor = ContextActor {
        path_checked: false,
        system_accessed: false,
    };
    let actor_ref = system
        .create_root_actor("context_actor", actor)
        .await
        .unwrap();

    // Test path checking
    let result = actor_ref.ask(ContextMessage::CheckPath).await.unwrap();
    assert_eq!(result.value, 1);

    // Test system access
    let result = actor_ref.ask(ContextMessage::AccessSystem).await.unwrap();
    assert_eq!(result.value, 2);

    // Verify both operations completed
    let result = actor_ref.ask(ContextMessage::GetState).await.unwrap();
    assert_eq!(result.value, 2); // Both flags should be true

    harness.shutdown().await;
}
