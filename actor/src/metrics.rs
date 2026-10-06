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

/// Labels describing an actor failure, aggregated by scope and type.
///
/// Per-actor paths are deliberately absent: paths embed runtime IDs and
/// would create unbounded series. See `*_detail` metrics for the opt-in
/// per-actor view (`Actor::detailed_metrics`).
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ActorFailureLabels {
    pub scope: Arc<str>,
    pub actor_type: Arc<str>,
    pub phase: &'static str,
}

/// Opt-in per-actor failure labels (`Actor::detailed_metrics`).
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ActorFailureDetailLabels {
    pub path: String,
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

/// Labels identifying a mailbox by scope and actor type (bounded).
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MailboxLabels {
    pub scope: Arc<str>,
    pub actor_type: Arc<str>,
}

/// Opt-in per-actor mailbox labels (`Actor::detailed_metrics`).
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MailboxDetailLabels {
    pub path: String,
}

/// Labels describing why mailbox messages were dropped (bounded).
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MailboxDropLabels {
    pub scope: Arc<str>,
    pub actor_type: Arc<str>,
    pub reason: &'static str,
}

/// Opt-in per-actor mailbox-drop labels (`Actor::detailed_metrics`).
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct MailboxDropDetailLabels {
    pub path: String,
    pub reason: &'static str,
}

/// Labels identifying an event sink by scope, actor type and name.
///
/// All three dimensions are developer-chosen constants (not runtime IDs),
/// so this stays bounded as long as sinks are registered with static
/// names. Do not register sinks with per-instance dynamic names.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkLabels {
    pub scope: Arc<str>,
    pub actor_type: Arc<str>,
    pub sink_name: String,
}

/// Labels describing why an event sink dropped an event.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkDropLabels {
    pub scope: Arc<str>,
    pub actor_type: Arc<str>,
    pub sink_name: String,
    pub reason: &'static str,
}

/// Opt-in per-actor sink-drop labels (`Actor::detailed_metrics`).
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkDropDetailLabels {
    pub path: String,
    pub sink_name: String,
    pub reason: &'static str,
}

/// Opt-in per-actor delivery-failure labels (`Actor::detailed_metrics`).
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SinkDetailLabels {
    pub path: String,
    pub sink_name: String,
}

#[cfg(feature = "prometheus")]
const MESSAGE_DURATION_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Prometheus metrics exported by the actor runtime.
///
/// Aggregate families are keyed by bounded dimensions (`scope`,
/// `actor_type`, static reasons) and are always recorded. `*_detail`
/// families add the actor `path` and are recorded only for actors opting
/// in via [`Actor::detailed_metrics`](crate::Actor::detailed_metrics).
#[cfg(feature = "prometheus")]
pub struct ActorMetrics {
    pub(crate) actor_failed_total: Family<ActorFailureLabels, Counter>,
    pub(crate) actor_failed_detail_total:
        Family<ActorFailureDetailLabels, Counter>,
    pub(crate) actor_restarted_total: Family<ActorRestartLabels, Counter>,
    pub(crate) actor_messages_processed_total: Family<MessageLabels, Counter>,
    pub(crate) actor_message_duration_seconds:
        Family<MessageDurationLabels, Histogram>,
    pub(crate) actor_message_wait_seconds:
        Family<MessageDurationLabels, Histogram>,
    pub(crate) actor_active: Family<ActorActiveLabels, Gauge>,
    pub(crate) actor_mailbox_full_total: Family<MailboxLabels, Counter>,
    pub(crate) actor_mailbox_full_detail_total:
        Family<MailboxDetailLabels, Counter>,
    pub(crate) actor_mailbox_dropped_total: Family<MailboxDropLabels, Counter>,
    pub(crate) actor_mailbox_dropped_detail_total:
        Family<MailboxDropDetailLabels, Counter>,
    pub(crate) sink_events_dropped_total: Family<SinkDropLabels, Counter>,
    pub(crate) sink_delivery_failures_total: Family<SinkLabels, Counter>,
    pub(crate) sink_events_dropped_detail_total:
        Family<SinkDropDetailLabels, Counter>,
    pub(crate) sink_delivery_failures_detail_total:
        Family<SinkDetailLabels, Counter>,
}

#[cfg(feature = "prometheus")]
impl ActorMetrics {
    /// Creates a new, unregistered metrics collection.
    pub fn new() -> Self {
        Self {
            actor_failed_total: Family::new_with_constructor(Counter::default),
            actor_failed_detail_total: Family::new_with_constructor(
                Counter::default,
            ),
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
            actor_mailbox_full_detail_total: Family::new_with_constructor(
                Counter::default,
            ),
            actor_mailbox_dropped_total: Family::new_with_constructor(
                Counter::default,
            ),
            actor_mailbox_dropped_detail_total: Family::new_with_constructor(
                Counter::default,
            ),
            sink_events_dropped_total: Family::new_with_constructor(
                Counter::default,
            ),
            sink_delivery_failures_total: Family::new_with_constructor(
                Counter::default,
            ),
            sink_events_dropped_detail_total: Family::new_with_constructor(
                Counter::default,
            ),
            sink_delivery_failures_detail_total: Family::new_with_constructor(
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
            "ave_actors_actor_failed_detail_total",
            "Per-actor failures, recorded only for actors opting in via detailed_metrics",
            self.actor_failed_detail_total.clone(),
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
            "ave_actors_actor_mailbox_full_detail_total",
            "Per-actor mailbox-full events, opt-in via detailed_metrics",
            self.actor_mailbox_full_detail_total.clone(),
        );
        registry.register(
            "ave_actors_actor_mailbox_dropped_total",
            "Total number of messages dropped from mailboxes",
            self.actor_mailbox_dropped_total.clone(),
        );
        registry.register(
            "ave_actors_actor_mailbox_dropped_detail_total",
            "Per-actor mailbox drops, opt-in via detailed_metrics",
            self.actor_mailbox_dropped_detail_total.clone(),
        );
        registry.register(
            "ave_actors_sink_events_dropped_total",
            "Total number of sink events dropped",
            self.sink_events_dropped_total.clone(),
        );
        registry.register(
            "ave_actors_sink_events_dropped_detail_total",
            "Per-actor sink drops, opt-in via detailed_metrics",
            self.sink_events_dropped_detail_total.clone(),
        );
        registry.register(
            "ave_actors_sink_delivery_failures_total",
            "Total number of sink delivery failures",
            self.sink_delivery_failures_total.clone(),
        );
        registry.register(
            "ave_actors_sink_delivery_failures_detail_total",
            "Per-actor delivery failures, opt-in via detailed_metrics",
            self.sink_delivery_failures_detail_total.clone(),
        );
    }

    /// Records an actor failure in the given phase.
    pub fn inc_actor_failed(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
        phase: &'static str,
    ) {
        self.actor_failed_total
            .get_or_create(&ActorFailureLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
                phase,
            })
            .inc();
    }

    /// Records an actor failure with its path. Call only for actors opting
    /// in via `detailed_metrics`; the aggregate above always fires.
    pub fn inc_actor_failed_detailed(
        &self,
        path: &crate::ActorPath,
        phase: &'static str,
    ) {
        self.actor_failed_detail_total
            .get_or_create(&ActorFailureDetailLabels {
                path: path.to_string(),
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

    /// Records a mailbox-full event.
    pub fn inc_mailbox_full(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
    ) {
        self.actor_mailbox_full_total
            .get_or_create(&MailboxLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
            })
            .inc();
    }

    /// Records a mailbox-full event with its path (opt-in only).
    pub fn inc_mailbox_full_detailed(&self, path: &crate::ActorPath) {
        self.actor_mailbox_full_detail_total
            .get_or_create(&MailboxDetailLabels {
                path: path.to_string(),
            })
            .inc();
    }

    /// Records that a mailbox message was dropped for the given reason.
    pub fn inc_mailbox_dropped(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
        reason: &'static str,
    ) {
        self.actor_mailbox_dropped_total
            .get_or_create(&MailboxDropLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
                reason,
            })
            .inc();
    }

    /// Records a mailbox drop with its path (opt-in only).
    pub fn inc_mailbox_dropped_detailed(
        &self,
        path: &crate::ActorPath,
        reason: &'static str,
    ) {
        self.actor_mailbox_dropped_detail_total
            .get_or_create(&MailboxDropDetailLabels {
                path: path.to_string(),
                reason,
            })
            .inc();
    }
}

/// Pre-created per-message metric handles for one actor.
///
/// `Family::get_or_create` costs a hash + lock per message; pre-creating
/// the 12 combinations once (2 kinds × 2 results × durations/waits by
/// criticality) reduces the hot path to array indexing. Owned by the
/// runner, which processes messages sequentially.
#[cfg(feature = "prometheus")]
pub(crate) struct MessageMetricHandles {
    processed: [[Counter; 2]; 2],
    duration: [[Histogram; 2]; 2],
    wait: [[Histogram; 2]; 2],
}

#[cfg(feature = "prometheus")]
impl MessageMetricHandles {
    /// Index rows: `kind` is always `"tell"` or `"ask"` (set by the
    /// runner), anything else counts as `ask`.
    const fn kind_index(kind: &'static str) -> usize {
        if kind.as_bytes()[0] == b't' { 0 } else { 1 }
    }

    pub(crate) fn new(
        metrics: &ActorMetrics,
        scope: &Arc<str>,
        actor_type: &Arc<str>,
    ) -> Self {
        let counter = |result: &'static str, kind: &'static str| {
            metrics
                .actor_messages_processed_total
                .get_or_create(&MessageLabels {
                    scope: Arc::clone(scope),
                    actor_type: Arc::clone(actor_type),
                    kind,
                    result,
                })
                .clone()
        };
        let histogram = |kind: &'static str, critical: bool| {
            let labels = MessageDurationLabels {
                scope: Arc::clone(scope),
                actor_type: Arc::clone(actor_type),
                kind,
                critical: if critical { "true" } else { "false" },
            };
            (
                metrics
                    .actor_message_duration_seconds
                    .get_or_create(&labels)
                    .clone(),
                metrics
                    .actor_message_wait_seconds
                    .get_or_create(&labels)
                    .clone(),
            )
        };
        let kinds = ["tell", "ask"];
        let processed = [
            [counter("ok", kinds[0]), counter("err", kinds[0])],
            [counter("ok", kinds[1]), counter("err", kinds[1])],
        ];
        let mut duration = std::array::from_fn(|_| {
            [Histogram::new([1.0]), Histogram::new([1.0])]
        });
        let mut wait = duration.clone();
        for (ki, kind) in kinds.iter().enumerate() {
            for (ci, critical) in [false, true].iter().enumerate() {
                let (d, w) = histogram(kind, *critical);
                duration[ki][ci] = d;
                wait[ki][ci] = w;
            }
        }
        Self {
            processed,
            duration,
            wait,
        }
    }

    pub(crate) const fn processed(
        &self,
        kind: &'static str,
        ok: bool,
    ) -> &Counter {
        let result = if ok { 0 } else { 1 };
        &self.processed[Self::kind_index(kind)][result]
    }

    pub(crate) const fn duration(
        &self,
        kind: &'static str,
        critical: bool,
    ) -> &Histogram {
        &self.duration[Self::kind_index(kind)][critical as usize]
    }

    pub(crate) const fn wait(
        &self,
        kind: &'static str,
        critical: bool,
    ) -> &Histogram {
        &self.wait[Self::kind_index(kind)][critical as usize]
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
    /// Returns the current value of the mailbox-full counter.
    pub fn mailbox_full_count(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
    ) -> u64 {
        self.actor_mailbox_full_total
            .get_or_create(&MailboxLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
            })
            .get()
    }

    /// Returns the current value of the mailbox-dropped counter.
    pub fn mailbox_dropped_count(
        &self,
        scope: impl Into<Arc<str>>,
        actor_type: impl Into<Arc<str>>,
        reason: &'static str,
    ) -> u64 {
        self.actor_mailbox_dropped_total
            .get_or_create(&MailboxDropLabels {
                scope: scope.into(),
                actor_type: actor_type.into(),
                reason,
            })
            .get()
    }

    /// Returns the per-actor mailbox-full detail counter.
    pub fn mailbox_full_detail_count(&self, path: &crate::ActorPath) -> u64 {
        self.actor_mailbox_full_detail_total
            .get_or_create(&MailboxDetailLabels {
                path: path.to_string(),
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
        metrics.inc_actor_failed("user", "OrderActor", "pre_start");
        metrics.inc_actor_failed_detailed(
            &crate::ActorPath::from("/user/order"),
            "pre_start",
        );
        let mut buf = String::new();
        prometheus_client::encoding::text::encode(&mut buf, &registry).unwrap();
        assert!(buf.contains("ave_actors_actor_failed_total"));
        assert!(buf.contains("ave_actors_actor_failed_detail_total"));
        assert!(buf.contains("OrderActor"));
        assert!(buf.contains("/user/order"));
    }
}
