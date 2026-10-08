//! [`pipe_to`]: deliver a future's output to an actor as a message.
//!
//! An actor that awaits a slow future inside
//! [`handle_message`](crate::Handler::handle_message) is deaf until it
//! resolves. `pipe_to` moves the wait out of the actor: the future runs
//! as a detached task and its output is `tell`ed to the target, where
//! it lands in the mailbox like any other message.
//!
//! If the target stopped meanwhile, the output is dropped silently:
//! piping to a dead actor is a normal race, not an error.

use std::future::Future;

use crate::{Actor, ActorRef, Handler};

/// Runs `future` to completion, then `tell`s its output to `target`.
///
/// Returns the task handle: drop it to detach (delivery still
/// happens), or `abort` it to cancel a pending delivery.
///
/// # Panics
///
/// Panics when called outside a Tokio runtime (it spawns a task).
/// Calling it from inside a message handler always qualifies.
///
/// # Example
///
/// ```rust,no_run
/// use ave_actors_actor::{TestProbe, TestSystem, pipe_to};
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), ave_actors_actor::Error> {
/// let harness = TestSystem::start();
/// let probe = TestProbe::<()>::new();
/// let probe_ref = probe.spawn(harness.system(), "probe").await?;
/// // Adapt any future to the target's message type with `async move`.
/// pipe_to(
///     async {
///         tokio::time::sleep(Duration::from_millis(50)).await;
///     },
///     probe_ref,
/// );
/// harness.shutdown().await;
/// # Ok(())
/// # }
/// ```
pub fn pipe_to<T, F>(
    future: F,
    target: ActorRef<T>,
) -> tokio::task::JoinHandle<()>
where
    T: Actor + Handler<T>,
    F: Future<Output = T::Message> + Send + 'static,
{
    tokio::spawn(async move {
        let message = future.await;
        // The target may be long gone; that is fine.
        let _ = target.tell(message).await;
    })
}
