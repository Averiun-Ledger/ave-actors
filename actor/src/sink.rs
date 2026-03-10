//! Event sink and subscriber pattern for processing actor events.

use crate::Event;

use async_trait::async_trait;
use tokio::sync::broadcast::{Receiver as EventReceiver, error::RecvError};

use tracing::{debug, warn};

/// Receives actor events from a broadcast channel and forwards them to a [`Subscriber`].
pub struct Sink<E: Event> {
    /// The subscriber that will be notified of events.
    subscriber: Box<dyn Subscriber<E>>,
    /// The broadcast receiver for actor events.
    event_receiver: EventReceiver<E>,
}

impl<E: Event> Sink<E> {
    /// Creates a sink from a broadcast receiver and a subscriber.
    pub fn new(
        event_receiver: EventReceiver<E>,
        subscriber: impl Subscriber<E>,
    ) -> Self {
        Self {
            subscriber: Box::new(subscriber),
            event_receiver,
        }
    }

    /// Runs the event loop until the channel closes. Skips lagged events.
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

/// Callback invoked by a [`Sink`] for each received event.
#[async_trait]
pub trait Subscriber<E: Event>: Send + Sync + 'static {
    /// Called for each event received from the actor's broadcast channel.
    async fn notify(&self, event: E);
}
