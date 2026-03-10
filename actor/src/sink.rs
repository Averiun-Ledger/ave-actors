//! Event sink and subscriber pattern for forwarding actor broadcast events to user-defined callbacks.

use crate::Event;

use async_trait::async_trait;
use tokio::sync::broadcast::{Receiver as EventReceiver, error::RecvError};

use tracing::{debug, warn};

/// Receives actor events from a broadcast channel and forwards them to a [`Subscriber`].
///
/// Create a `Sink` with [`Sink::new`] and run it in a background task via
/// [`SystemRef::run_sink`]. The sink loop exits automatically when the actor's
/// event channel closes.
pub struct Sink<E: Event> {
    /// The subscriber that will be notified of events.
    subscriber: Box<dyn Subscriber<E>>,
    /// The broadcast receiver for actor events.
    event_receiver: EventReceiver<E>,
}

impl<E: Event> Sink<E> {
    /// Creates a sink that will forward events from `event_receiver` to `subscriber`.
    pub fn new(
        event_receiver: EventReceiver<E>,
        subscriber: impl Subscriber<E>,
    ) -> Self {
        Self {
            subscriber: Box::new(subscriber),
            event_receiver,
        }
    }

    /// Runs the event forwarding loop until the channel closes.
    ///
    /// Calls [`Subscriber::notify`] for each event. If the receiver lags behind
    /// (slow consumer), skipped events are logged as warnings and the loop continues.
    /// Use [`SystemRef::run_sink`] to spawn this in a background task.
    pub async fn run(&mut self) {
        loop {
            match self.event_receiver.recv().await {
                Ok(event) => {
                    self.subscriber.notify(event).await;
                }
                Err(error) => match error {
                    RecvError::Closed => {
                        debug!("Event channel closed, sink stopping");
                        break;
                    }
                    RecvError::Lagged(skipped) => {
                        warn!(skipped, "Sink lagged, skipped events");
                        continue;
                    }
                },
            }
        }
    }
}

/// Callback interface invoked by a [`Sink`] for each received event.
///
/// Implement this trait to react to actor events asynchronously. The `notify`
/// method is called once per event; it must be `Send + Sync + 'static` because
/// it runs inside a spawned Tokio task.
#[async_trait]
pub trait Subscriber<E: Event>: Send + Sync + 'static {
    /// Called for each event received from the actor's broadcast channel.
    async fn notify(&self, event: E);
}
