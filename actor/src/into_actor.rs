//! Compile-time enforcement of persistence rules via the [`IntoActor`] and [`NotPersistentActor`] traits.

use crate::actor::{Actor, Handler};

/// Marker trait for actors that do not use persistence.
///
/// Implement this to allow passing the actor directly to `create_root_actor` / `create_child`.
/// Persistent actors must use `PersistentActor::initial()` from the store crate instead.
pub trait NotPersistentActor {}

/// Converts a value into an actor instance.
///
/// Implemented automatically for [`NotPersistentActor`] types, and by the store
/// crate for `InitializedActor<A>` (persistent actors).
pub trait IntoActor<A: Actor + Handler<A>> {
    fn into_actor(self) -> A;
}

impl<A: Actor + Handler<A> + NotPersistentActor> IntoActor<A> for A {
    fn into_actor(self) -> A {
        self
    }
}
