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

/// Name used to register/retrieve the shared [`StoreMetrics`] helper in an
/// [`ActorSystem`](ave_actors_actor::ActorSystem).
pub const STORE_METRICS_HELPER: &str = "ave_actors_store_metrics";

/// Labels identifying the actor path a store metric belongs to.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StorePathLabels {
    pub path: Arc<str>,
}

/// Labels attached to the errors counter, including the operation that failed.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StoreErrorLabels {
    pub path: Arc<str>,
    pub operation: &'static str,
}

/// Labels attached to the operation-duration histogram.
#[cfg(feature = "prometheus")]
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StoreDurationLabels {
    pub path: Arc<str>,
    pub operation: &'static str,
}

#[cfg(feature = "prometheus")]
const STORE_DURATION_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Prometheus metrics exported by the persistence layer.
#[cfg(feature = "prometheus")]
pub struct StoreMetrics {
    pub(crate) store_errors_total: Family<StoreErrorLabels, Counter>,
    pub(crate) store_operation_duration_seconds:
        Family<StoreDurationLabels, Histogram>,
    pub(crate) store_pending_events: Family<StorePathLabels, Gauge>,
}

#[cfg(feature = "prometheus")]
impl StoreMetrics {
    /// Creates a new, unregistered metrics collection.
    pub fn new() -> Self {
        Self {
            store_errors_total: Family::new_with_constructor(Counter::default),
            store_operation_duration_seconds: Family::new_with_constructor(
                || Histogram::new(STORE_DURATION_BUCKETS),
            ),
            store_pending_events: Family::new_with_constructor(Gauge::default),
        }
    }

    /// Registers all metrics into the supplied Prometheus registry.
    pub fn register_into(&self, registry: &mut Registry) {
        registry.register(
            "ave_actors_store_errors_total",
            "Total number of store errors",
            self.store_errors_total.clone(),
        );
        registry.register(
            "ave_actors_store_operation_duration_seconds",
            "Store operation duration in seconds",
            self.store_operation_duration_seconds.clone(),
        );
        registry.register(
            "ave_actors_store_pending_events",
            "Number of events persisted since the last snapshot",
            self.store_pending_events.clone(),
        );
    }

    /// Records that a store operation failed.
    pub fn inc_errors(&self, path: &Arc<str>, operation: &'static str) {
        self.store_errors_total
            .get_or_create(&StoreErrorLabels {
                path: Arc::clone(path),
                operation,
            })
            .inc();
    }

    /// Observes the duration of a store operation in seconds.
    pub fn observe_operation_duration(
        &self,
        path: &Arc<str>,
        operation: &'static str,
        seconds: f64,
    ) {
        self.store_operation_duration_seconds
            .get_or_create(&StoreDurationLabels {
                path: Arc::clone(path),
                operation,
            })
            .observe(seconds);
    }

    /// Sets the number of events persisted since the last snapshot.
    fn set_pending_events(&self, path: &Arc<str>, count: i64) {
        self.store_pending_events
            .get_or_create(&StorePathLabels {
                path: Arc::clone(path),
            })
            .set(count);
    }

    /// Sets the number of events persisted since the last snapshot from a
    /// `u64` value, saturating at `i64::MAX` to avoid overflow when the store
    /// contains an extremely large event log.
    pub fn set_pending_events_u64(&self, path: &Arc<str>, count: u64) {
        let count = count.min(i64::MAX as u64) as i64;
        self.set_pending_events(path, count);
    }
}

#[cfg(feature = "prometheus")]
impl Default for StoreMetrics {
    fn default() -> Self {
        Self::new()
    }
}
