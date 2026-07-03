#[cfg(feature = "prometheus")]
use prometheus_client::encoding::EncodeLabelSet;
#[cfg(feature = "prometheus")]
use prometheus_client::metrics::{
    counter::Counter, family::Family, gauge::Gauge, histogram::Histogram,
};
#[cfg(feature = "prometheus")]
use prometheus_client::registry::Registry;
#[cfg(feature = "prometheus")]
use std::sync::Arc;

/// Labels describing an actor failure, including the path and the phase in
/// which it happened.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ActorFailureLabels {
    pub path: String,
    pub actor_type: Arc<str>,
    pub phase: &'static str,
}

/// Labels describing an actor restart.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ActorRestartLabels {
    pub scope: Arc<str>,
    pub actor_type: Arc<str>,
    pub strategy: &'static str,
}

/// Labels attached to the processed-messages counter.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MessageLabels {
    pub scope: Arc<str>,
    pub actor_type: Arc<str>,
    pub kind: &'static str,
    pub result: &'static str,
}

/// Labels attached to the message-processing duration histogram.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MessageDurationLabels {
    pub scope: Arc<str>,
    pub actor_type: Arc<str>,
    pub kind: &'static str,
    pub critical: &'static str,
}

/// Labels attached to the currently-active-actors gauge.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ActorActiveLabels {
    pub scope: Arc<str>,
    pub actor_type: Arc<str>,
}

/// Labels identifying a mailbox by its actor path.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MailboxLabels {
    pub path: String,
}

/// Labels describing why a mailbox message was dropped.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MailboxDropLabels {
    pub path: String,
    pub reason: &'static str,
}

/// Labels identifying an event sink by scope and name.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkLabels {
    pub scope: Arc<str>,
    pub sink_name: String,
}

/// Labels describing why an event sink dropped an event.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkDropLabels {
    pub scope: Arc<str>,
    pub sink_name: String,
    pub reason: &'static str,
}

#[cfg(feature = "prometheus")]
const MESSAGE_DURATION_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Prometheus metrics exported by the actor runtime.
#[cfg(feature = "prometheus")]
pub struct ActorMetrics {
    pub(crate) actor_failed_total: Family<ActorFailureLabels, Counter>,
    pub(crate) actor_restarted_total: Family<ActorRestartLabels, Counter>,
    pub(crate) actor_messages_processed_total: Family<MessageLabels, Counter>,
    pub(crate) actor_message_duration_seconds:
        Family<MessageDurationLabels, Histogram>,
    pub(crate) actor_message_wait_seconds:
        Family<MessageDurationLabels, Histogram>,
    pub(crate) actor_active: Family<ActorActiveLabels, Gauge>,
    pub(crate) actor_mailbox_full_total: Family<MailboxLabels, Counter>,
    pub(crate) actor_mailbox_dropped_total: Family<MailboxDropLabels, Counter>,
    pub(crate) sink_events_dropped_total: Family<SinkDropLabels, Counter>,
    pub(crate) sink_delivery_failures_total: Family<SinkLabels, Counter>,
}

#[cfg(feature = "prometheus")]
impl ActorMetrics {
    /// Creates a new, unregistered metrics collection.
    pub fn new() -> Self {
        Self {
            actor_failed_total: Family::new_with_constructor(Counter::default),
            actor_restarted_total: Family::new_with_constructor(
                Counter::default,
            ),
            actor_messages_processed_total: Family::new_with_constructor(
                Counter::default,
            ),
            actor_message_duration_seconds: Family::new_with_constructor(
                || Histogram::new(MESSAGE_DURATION_BUCKETS),
            ),
            actor_message_wait_seconds: Family::new_with_constructor(|| {
                Histogram::new(MESSAGE_DURATION_BUCKETS)
            }),
            actor_active: Family::new_with_constructor(Gauge::default),
            actor_mailbox_full_total: Family::new_with_constructor(
                Counter::default,
            ),
            actor_mailbox_dropped_total: Family::new_with_constructor(
                Counter::default,
            ),
            sink_events_dropped_total: Family::new_with_constructor(
                Counter::default,
            ),
            sink_delivery_failures_total: Family::new_with_constructor(
                Counter::default,
            ),
        }
    }

    /// Registers all metrics into the supplied Prometheus registry.
    pub fn register_into(&self, registry: &mut Registry) {
        registry.register(
            "ave_actors_actor_failed_total",
            "Total number of actor failures",
            self.actor_failed_total.clone(),
        );
        registry.register(
            "ave_actors_actor_restarted_total",
            "Total number of actor restarts",
            self.actor_restarted_total.clone(),
        );
        registry.register(
            "ave_actors_actor_messages_processed_total",
            "Total number of messages processed by actors",
            self.actor_messages_processed_total.clone(),
        );
        registry.register(
            "ave_actors_actor_message_duration_seconds",
            "Message processing duration in seconds",
            self.actor_message_duration_seconds.clone(),
        );
        registry.register(
            "ave_actors_actor_message_wait_seconds",
            "Time from enqueue to start of message handling in seconds",
            self.actor_message_wait_seconds.clone(),
        );
        registry.register(
            "ave_actors_actor_active",
            "Number of actors currently running",
            self.actor_active.clone(),
        );
        registry.register(
            "ave_actors_actor_mailbox_full_total",
            "Total number of mailbox-full events",
            self.actor_mailbox_full_total.clone(),
        );
        registry.register(
            "ave_actors_actor_mailbox_dropped_total",
            "Total number of messages dropped from mailboxes",
            self.actor_mailbox_dropped_total.clone(),
        );
        registry.register(
            "ave_actors_sink_events_dropped_total",
            "Total number of sink events dropped",
            self.sink_events_dropped_total.clone(),
        );
        registry.register(
            "ave_actors_sink_delivery_failures_total",
            "Total number of sink delivery failures",
            self.sink_delivery_failures_total.clone(),
        );
    }

    /// Records an actor failure in the given phase.
    pub fn inc_actor_failed(
        &self,
        path: &crate::ActorPath,
        actor_type: impl Into<Arc<str>>,
        phase: &'static str,
    ) {
        self.actor_failed_total
            .get_or_create(&ActorFailureLabels {
                path: path.to_string(),
                actor_type: actor_type.into(),
                phase,
            })
            .inc();
    }

    /// Records that an actor was restarted with the given strategy.
    pub fn inc_actor_restarted(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
        strategy: &'static str,
    ) {
        self.actor_restarted_total
            .get_or_create(&ActorRestartLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
                strategy,
            })
            .inc();
    }

    /// Records the processing result of a message.
    pub fn inc_messages_processed(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
        kind: &'static str,
        result: &'static str,
    ) {
        self.actor_messages_processed_total
            .get_or_create(&MessageLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
                kind,
                result,
            })
            .inc();
    }

    /// Observes the duration of a message handling in seconds.
    pub fn observe_message_duration(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
        kind: &'static str,
        critical: bool,
        seconds: f64,
    ) {
        self.actor_message_duration_seconds
            .get_or_create(&MessageDurationLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
                kind,
                critical: if critical { "true" } else { "false" },
            })
            .observe(seconds);
    }

    /// Observes the time a message waited in the mailbox before handling.
    pub fn observe_message_wait(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
        kind: &'static str,
        critical: bool,
        seconds: f64,
    ) {
        self.actor_message_wait_seconds
            .get_or_create(&MessageDurationLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
                kind,
                critical: if critical { "true" } else { "false" },
            })
            .observe(seconds);
    }

    /// Increments the number of currently active actors.
    pub fn inc_actor_active(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
    ) {
        self.actor_active
            .get_or_create(&ActorActiveLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
            })
            .inc();
    }

    /// Decrements the number of currently active actors.
    pub fn dec_actor_active(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
    ) {
        self.actor_active
            .get_or_create(&ActorActiveLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
            })
            .dec();
    }

    /// Records a mailbox-full event for the actor at `path`.
    pub fn inc_mailbox_full(&self, path: &crate::ActorPath) {
        self.actor_mailbox_full_total
            .get_or_create(&MailboxLabels {
                path: path.to_string(),
            })
            .inc();
    }

    /// Records that a mailbox message was dropped for the given reason.
    pub fn inc_mailbox_dropped(
        &self,
        path: &crate::ActorPath,
        reason: &'static str,
    ) {
        self.actor_mailbox_dropped_total
            .get_or_create(&MailboxDropLabels {
                path: path.to_string(),
                reason,
            })
            .inc();
    }
}

#[cfg(feature = "prometheus")]
impl Default for ActorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, feature = "prometheus"))]
impl ActorMetrics {
    /// Returns the current value of the mailbox-full counter for `path`.
    pub fn mailbox_full_count(&self, path: &crate::ActorPath) -> u64 {
        self.actor_mailbox_full_total
            .get_or_create(&MailboxLabels {
                path: path.to_string(),
            })
            .get()
    }

    /// Returns the current value of the mailbox-dropped counter for `path`
    /// and `reason`.
    pub fn mailbox_dropped_count(
        &self,
        path: &crate::ActorPath,
        reason: &'static str,
    ) -> u64 {
        self.actor_mailbox_dropped_total
            .get_or_create(&MailboxDropLabels {
                path: path.to_string(),
                reason,
            })
            .get()
    }
}

#[cfg(all(test, feature = "prometheus"))]
mod tests {
    use super::*;

    #[test]
    fn test_actor_metrics_register_and_increment() {
        let mut registry = Registry::default();
        let metrics = ActorMetrics::new();
        metrics.register_into(&mut registry);
        metrics.inc_actor_failed(
            &crate::ActorPath::from("/user/order"),
            "OrderActor",
            "pre_start",
        );
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
        assert!(buf.contains("ave_actors_actor_failed_total"));
        assert!(buf.contains("OrderActor"));
    }
}
