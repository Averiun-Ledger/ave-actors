//! Traits for converting values into actor instances, with compile-time enforcement of persistence rules.

use crate::actor::{Actor, Handler};

/// Marker trait for actors that do not use persistence.
///
/// Implement this on your actor to allow passing it directly to
/// `create_root_actor` or `create_child`. Persistent actors use
/// `PersistentActor::initial()` instead and must NOT also implement this trait.
pub trait NotPersistentActor {}

/// Converts a value into an actor instance ready for the actor system.
///
/// Implemented automatically for [`NotPersistentActor`] types (identity conversion),
/// and by the store crate for `InitializedActor<A>` (persistent actors).
pub trait IntoActor<A: Actor + Handler<A>> {
    fn into_actor(self) -> A;
}

impl<A: Actor + Handler<A> + NotPersistentActor> IntoActor<A> for A {
    fn into_actor(self) -> A {
        self
    }
}
