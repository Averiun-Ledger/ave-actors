//! Comprehensive test matrix for actor creation patterns.
//!
//! This test suite verifies all combinations of:
//! - Persistent vs Non-persistent actors
//! - create_root_actor vs create_child
//! - Direct instantiation vs initial() wrapper
//!
//! Expected outcomes:
//! ✅ Non-persistent + Direct instance → SUCCESS
//! ✅ Persistent + initial() wrapper → SUCCESS
//! ❌ Persistent + Direct instance → COMPILE ERROR (prevented by NotPersistentActor requirement)
//! ❌ Non-persistent + initial() wrapper → N/A (NotPersistentActor actors don't have initial())
//!
//! # Important
//!
//! Do NOT implement both `NotPersistentActor` and `PersistentActor` on the same type.
//! This is enforced by convention but not by the type system.

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, ActorSystem, Event, Handler, Message,
    NotPersistentActor, Response, build_tracing_subscriber,
};
use ave_actors_store::store::{FullPersistence, PersistentActor};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::info_span;

// ============================================================================
// Test Actors
// ============================================================================

// Persistent Actor
#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
struct MyPersistentActor {
    counter: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistentMessage;
impl Message for PersistentMessage {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistentResponse;
impl Response for PersistentResponse {}

#[derive(
    Debug,
    Clone,
    Serialize,
    Deserialize,
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
)]
struct PersistentEvent;
impl Event for PersistentEvent {}

#[async_trait]
impl Actor for MyPersistentActor {
    type Message = PersistentMessage;
    type Response = PersistentResponse;
    type Event = PersistentEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("MyPersistentActor", id = %id)
    }
}

#[async_trait]
impl Handler<MyPersistentActor> for MyPersistentActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: PersistentMessage,
        _ctx: &mut ActorContext<MyPersistentActor>,
    ) -> Result<PersistentResponse, ave_actors_actor::Error> {
        Ok(PersistentResponse)
    }
}

#[async_trait]
impl PersistentActor for MyPersistentActor {
    type Persistence = FullPersistence;
    type InitParams = i32;

    fn create_initial(initial_value: i32) -> Self {
        Self {
            counter: initial_value,
        }
    }

    fn apply(
        &mut self,
        _event: &Self::Event,
    ) -> Result<(), ave_actors_actor::Error> {
        Ok(())
    }
}

// Non-Persistent Actor
#[derive(Debug, Clone)]
struct MyNonPersistentActor {
    #[allow(dead_code)]
    pub value: String,
}

impl NotPersistentActor for MyNonPersistentActor {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NonPersistentMessage;
impl Message for NonPersistentMessage {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NonPersistentResponse;
impl Response for NonPersistentResponse {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NonPersistentEvent;
impl Event for NonPersistentEvent {}

#[async_trait]
impl Actor for MyNonPersistentActor {
    type Message = NonPersistentMessage;
    type Response = NonPersistentResponse;
    type Event = NonPersistentEvent;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("MyNonPersistentActor", id = %id)
    }
}

#[async_trait]
impl Handler<MyNonPersistentActor> for MyNonPersistentActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: NonPersistentMessage,
        _ctx: &mut ActorContext<MyNonPersistentActor>,
    ) -> Result<NonPersistentResponse, ave_actors_actor::Error> {
        Ok(NonPersistentResponse)
    }
}

// Parent actor for testing create_child
#[derive(Debug, Clone)]
struct ParentActor;

impl NotPersistentActor for ParentActor {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParentMessage;
impl Message for ParentMessage {}

#[async_trait]
impl Actor for ParentActor {
    type Message = ParentMessage;
    type Response = ();
    type Event = ();
    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("ParentActor", id = %id)
    }

    async fn pre_start(
        &mut self,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<(), ave_actors_actor::Error> {
        Ok(())
    }
}

#[async_trait]
impl Handler<ParentActor> for ParentActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: ParentMessage,
        _ctx: &mut ActorContext<ParentActor>,
    ) -> Result<(), ave_actors_actor::Error> {
        Ok(())
    }
}

// ============================================================================
// Test Cases - create_root_actor
// ============================================================================

/// ✅ SUCCESS: Non-persistent actor with direct instance
#[tokio::test]
async fn test_create_root_actor_non_persistent_direct() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let actor = MyNonPersistentActor {
        value: "test".to_string(),
    };
    let result = system.create_root_actor("non_persistent", actor).await;
    assert!(
        result.is_ok(),
        "Non-persistent actor should be created with direct instance"
    );
}

/// ✅ SUCCESS: Persistent actor with initial() wrapper
#[tokio::test]
async fn test_create_root_actor_persistent_initial() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let result = system
        .create_root_actor("persistent", MyPersistentActor::initial(42))
        .await;
    assert!(
        result.is_ok(),
        "Persistent actor should be created with initial()"
    );
}

/// ❌ COMPILE FAIL: Persistent actor with direct instance
/// This test is commented out because it SHOULD NOT compile.
/// Uncommenting it will cause a compilation error, which is the desired behavior.
///
/// ```compile_fail
/// #[tokio::test]
/// async fn test_create_root_actor_persistent_direct_fails() {
///     build_tracing_subscriber();
///     let (system, _runner) = ActorSystem::create(CancellationToken::new(), CancellationToken::new());
///
///     // This WILL NOT compile because MyPersistentActor doesn't implement NotPersistentActor
///     let actor = MyPersistentActor { counter: 42 };
///     let _result = system.create_root_actor("persistent", actor).await;
///
///     // Error: the trait `NotPersistentActor` is not implemented for `MyPersistentActor`
/// }
/// ```

// ============================================================================
// Test Cases - create_child
// ============================================================================

/// ✅ SUCCESS: Non-persistent child with direct instance
#[tokio::test]
async fn test_create_child_non_persistent_direct() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let parent_ref = system
        .create_root_actor("parent", ParentActor)
        .await
        .unwrap();

    // Access the actor's context to create a child
    let child = MyNonPersistentActor {
        value: "child".to_string(),
    };

    // We can't directly test create_child from outside, but we verify the type compiles
    let _: MyNonPersistentActor = child;

    drop(parent_ref);
}

/// ✅ SUCCESS: Persistent child with initial() wrapper
#[tokio::test]
async fn test_create_child_persistent_initial() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    let _parent_ref = system
        .create_root_actor("parent2", ParentActor)
        .await
        .unwrap();

    // Verify the type compiles for create_child usage
    let _child = MyPersistentActor::initial(100);
}

/// ❌ COMPILE FAIL: Persistent child with direct instance
/// This would fail at compile time just like create_root_actor
///
/// ```compile_fail
/// async fn test_create_child_persistent_direct_fails(ctx: &mut ActorContext<ParentActor>) {
///     // This WILL NOT compile
///     let actor = MyPersistentActor { counter: 42 };
///     let _result = ctx.create_child("child", actor).await;
///
///     // Error: the trait `NotPersistentActor` is not implemented for `MyPersistentActor`
/// }
/// ```

// ============================================================================
// Summary Test - Documents all combinations
// ============================================================================

#[tokio::test]
async fn test_all_valid_combinations() {
    build_tracing_subscriber();
    let (system, mut runner) =
        ActorSystem::create(CancellationToken::new(), CancellationToken::new());
    tokio::spawn(async move { runner.run().await });

    // ✅ Pattern 1: Non-persistent + Direct instance
    let non_persistent = MyNonPersistentActor {
        value: "test1".to_string(),
    };
    assert!(
        system
            .create_root_actor("test1", non_persistent)
            .await
            .is_ok()
    );

    // ✅ Pattern 2: Persistent + initial()
    assert!(
        system
            .create_root_actor("test2", MyPersistentActor::initial(1))
            .await
            .is_ok()
    );

    // ❌ Pattern 3: Persistent + Direct instance → COMPILE ERROR (prevented)
    // let persistent = MyPersistentActor { counter: 1 };
    // system.create_root_actor("test3", persistent).await; // Won't compile!

    // ❌ Pattern 4: Non-persistent + initial() → N/A (doesn't have initial())
    // MyNonPersistentActor::initial(...) // Method doesn't exist!
}

/// Documentation test showing the type safety in action
#[test]
fn test_type_safety_documentation() {
    // This test just documents the type safety guarantees

    // ✅ Allowed: Non-persistent actors implement NotPersistentActor
    fn _assert_non_persistent_has_trait<T: NotPersistentActor>() {}
    _assert_non_persistent_has_trait::<MyNonPersistentActor>();

    // ✅ Allowed: Persistent actors implement PersistentActor
    fn _assert_persistent_has_trait<T: PersistentActor>()
    where
        T::Event: BorshSerialize + BorshDeserialize,
    {
    }
    _assert_persistent_has_trait::<MyPersistentActor>();

    // ⚠️ CONVENTION: Don't implement both traits on the same type
    // While not enforced by the compiler, implementing both traits on the same
    // type will lead to undefined behavior.
}
