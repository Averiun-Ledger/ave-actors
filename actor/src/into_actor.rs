//! Trait for converting types into actor instances.
//!
//! This module provides the `IntoActor` trait which allows the actor system
//! to accept both direct actor instances (for non-persistent actors) and
//! `InitializedActor` wrappers (for persistent actors from the store crate).

use crate::actor::{Actor, Handler};

/// Marker trait for actors that do NOT use persistence.
///
/// This trait must be implemented by actors that don't persist their state.
///
/// # Purpose
///
/// This trait enables compile-time enforcement that:
/// - Non-persistent actors can be created directly via instances
/// - Persistent actors MUST use `PersistentActor::initial()` wrapper
///
/// # Implementation
///
/// Simply add this to your non-persistent actor:
///
/// ```rust
/// use ave_actors_actor::NotPersistentActor;
///
/// struct MyActor;
///
/// impl NotPersistentActor for MyActor {}
/// ```
///
/// # Important
///
/// Do NOT implement both `NotPersistentActor` and `PersistentActor` on the same type.
/// This is enforced by convention but not by the type system.
pub trait NotPersistentActor {}

/// Trait for types that can be converted into an actor instance.
///
/// This trait is implemented for:
/// - Direct actor instances (for non-persistent actors that implement `NotPersistentActor`)
/// - `InitializedActor<A>` wrappers (for persistent actors)
///
/// This allows the actor system's `create_root_actor` and `create_child`
/// methods to accept both types seamlessly while enforcing correct usage at compile time.
pub trait IntoActor<A: Actor + Handler<A>> {
    /// Converts this type into an actor instance.
    fn into_actor(self) -> A;
}

/// Direct actor instances can be used as-is, but ONLY for non-persistent actors.
/// Actors must implement `NotPersistentActor` to use this path.
impl<A: Actor + Handler<A> + NotPersistentActor> IntoActor<A> for A {
    fn into_actor(self) -> A {
        self
    }
}
