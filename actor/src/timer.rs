//! Timers scheduled by an actor.
//!
//! Each actor has a single `TimerScheduler` task that manages a heap of
//! deadlines. This keeps the cost of many timers per actor low compared to
//! spawning one task per timer.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::{Actor, ActorPath, Handler, SystemRef};

/// Opaque identifier returned by `ActorContext::schedule_once` and
/// `ActorContext::schedule`. It can be passed to
/// `ActorContext::cancel_timer` to abort a pending timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerKey(u64);

impl TimerKey {
    pub(crate) const fn new(id: u64) -> Self {
        Self(id)
    }
}

struct TimerEntry<A: Actor + Handler<A>> {
    deadline: Instant,
    key: TimerKey,
    msg: A::Message,
    period: Option<Duration>,
    /// Generation used to discard timers created before a `cancel_all` call.
    epoch: u64,
}

impl<A: Actor + Handler<A>> PartialEq for TimerEntry<A> {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}

impl<A: Actor + Handler<A>> Eq for TimerEntry<A> {}

impl<A: Actor + Handler<A>> PartialOrd for TimerEntry<A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A: Actor + Handler<A>> Ord for TimerEntry<A> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse so the earliest deadline becomes the highest priority in a
        // `BinaryHeap` (which is a max-heap by default).
        other.deadline.cmp(&self.deadline)
    }
}

/// Scheduler for an actor's timers.
///
/// A single background task sleeps until the nearest deadline and fires all
/// expired timers. The scheduler is cloneable so it can be shared between the
/// `ActorRunner` and the `ActorContext`.
pub struct TimerScheduler<A: Actor + Handler<A>> {
    system: SystemRef,
    path: ActorPath,
    running: Arc<AtomicBool>,
    accepting: Arc<AtomicBool>,
    epoch: Arc<AtomicU64>,
    next_id: Arc<AtomicU64>,
    max_timers: usize,
    has_task: Arc<AtomicBool>,
    heap: Arc<Mutex<BinaryHeap<TimerEntry<A>>>>,
    cancelled: Arc<Mutex<HashSet<TimerKey>>>,
    notify: Arc<Notify>,
}

impl<A: Actor + Handler<A>> Clone for TimerScheduler<A> {
    fn clone(&self) -> Self {
        Self {
            system: self.system.clone(),
            path: self.path.clone(),
            running: Arc::clone(&self.running),
            accepting: Arc::clone(&self.accepting),
            epoch: Arc::clone(&self.epoch),
            next_id: Arc::clone(&self.next_id),
            max_timers: self.max_timers,
            has_task: Arc::clone(&self.has_task),
            heap: Arc::clone(&self.heap),
            cancelled: Arc::clone(&self.cancelled),
            notify: Arc::clone(&self.notify),
        }
    }
}

impl<A: Actor + Handler<A>> TimerScheduler<A> {
    /// Creates a new scheduler. The background task is started lazily on the
    /// first scheduled timer, so actors that never use timers do not pay the
    /// cost of an extra Tokio task.
    pub fn new(system: SystemRef, path: ActorPath, max_timers: usize) -> Self {
        let running = Arc::new(AtomicBool::new(true));
        let accepting = Arc::new(AtomicBool::new(false));
        let epoch = Arc::new(AtomicU64::new(0));
        let next_id = Arc::new(AtomicU64::new(1));
        let has_task = Arc::new(AtomicBool::new(false));
        let heap = Arc::new(Mutex::new(BinaryHeap::new()));
        let cancelled = Arc::new(Mutex::new(HashSet::new()));
        let notify = Arc::new(Notify::new());

        Self {
            system,
            path,
            running,
            accepting,
            epoch,
            next_id,
            max_timers,
            has_task,
            heap,
            cancelled,
            notify,
        }
    }

    fn next_key(&self) -> TimerKey {
        TimerKey::new(self.next_id.fetch_add(1, AtomicOrdering::SeqCst))
    }

    fn lock_heap(&self) -> MutexGuard<'_, BinaryHeap<TimerEntry<A>>> {
        self.heap
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_cancelled(&self) -> MutexGuard<'_, HashSet<TimerKey>> {
        self.cancelled
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Starts the background timer task if it has not been started yet and the
    /// scheduler has not been shut down.
    fn ensure_task_running(&self) {
        if !self.running.load(AtomicOrdering::SeqCst) {
            return;
        }
        if self
            .has_task
            .compare_exchange(
                false,
                true,
                AtomicOrdering::SeqCst,
                AtomicOrdering::SeqCst,
            )
            .is_ok()
        {
            tokio::spawn(timer_loop(TimerLoopState {
                system: self.system.clone(),
                path: self.path.clone(),
                running: self.running.clone(),
                epoch: self.epoch.clone(),
                heap: self.heap.clone(),
                cancelled: self.cancelled.clone(),
                notify: self.notify.clone(),
            }));
        }
    }

    /// Allows or forbids the creation of new timers. Used by the runner to
    /// block timers while the actor is stopping.
    pub(crate) fn set_accepting(&self, value: bool) {
        self.accepting.store(value, AtomicOrdering::SeqCst);
        if value {
            self.notify.notify_one();
        }
    }

    /// Schedules a single message to be delivered after `delay`.
    pub(crate) fn schedule_once(
        &self,
        delay: Duration,
        msg: A::Message,
    ) -> TimerKey {
        let key = self.next_key();
        if !self.accepting.load(AtomicOrdering::SeqCst) {
            return key;
        }

        let deadline = Instant::now() + delay;
        let epoch = self.epoch.load(AtomicOrdering::SeqCst);
        let entry = TimerEntry {
            deadline,
            key,
            msg,
            period: None,
            epoch,
        };

        let mut heap = self.lock_heap();
        if heap.len() >= self.max_timers {
            warn!(
                max_timers = self.max_timers,
                "Actor has reached its timer limit; new timer ignored"
            );
            return key;
        }
        heap.push(entry);
        drop(heap);
        self.ensure_task_running();
        self.notify.notify_one();
        key
    }

    /// Schedules a message to be delivered every `period`.
    pub(crate) fn schedule(&self, period: Duration, msg: A::Message) -> TimerKey
    where
        A::Message: Clone,
    {
        let key = self.next_key();
        if !self.accepting.load(AtomicOrdering::SeqCst) {
            return key;
        }

        let deadline = Instant::now() + period;
        let epoch = self.epoch.load(AtomicOrdering::SeqCst);
        let entry = TimerEntry {
            deadline,
            key,
            msg,
            period: Some(period),
            epoch,
        };

        let mut heap = self.lock_heap();
        if heap.len() >= self.max_timers {
            warn!(
                max_timers = self.max_timers,
                "Actor has reached its timer limit; new timer ignored"
            );
            return key;
        }
        heap.push(entry);
        drop(heap);
        self.ensure_task_running();
        self.notify.notify_one();
        key
    }

    /// Marks a timer as cancelled. The entry will be discarded when it reaches
    /// the top of the heap.
    pub(crate) fn cancel(&self, key: TimerKey) {
        self.lock_cancelled().insert(key);
        self.notify.notify_one();
    }

    /// Cancels every pending timer, clears the heap and prevents new timers
    /// from being scheduled. The background task remains alive so timers can
    /// be created again after a restart.
    pub(crate) fn cancel_all(&self) {
        self.accepting.store(false, AtomicOrdering::SeqCst);
        self.epoch.fetch_add(1, AtomicOrdering::SeqCst);
        self.lock_heap().clear();
        self.lock_cancelled().clear();
        self.notify.notify_one();
    }

    /// Stops the background task permanently.
    pub(crate) fn shutdown(&self) {
        self.running.store(false, AtomicOrdering::SeqCst);
        self.accepting.store(false, AtomicOrdering::SeqCst);
        self.notify.notify_one();
    }
}

/// State passed to the single background task that manages an actor's timers.
struct TimerLoopState<A: Actor + Handler<A>> {
    system: SystemRef,
    path: ActorPath,
    running: Arc<AtomicBool>,
    epoch: Arc<AtomicU64>,
    heap: Arc<Mutex<BinaryHeap<TimerEntry<A>>>>,
    cancelled: Arc<Mutex<HashSet<TimerKey>>>,
    notify: Arc<Notify>,
}

async fn timer_loop<A: Actor + Handler<A>>(state: TimerLoopState<A>) {
    while state.running.load(AtomicOrdering::SeqCst) {
        let next_deadline = state
            .heap
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .peek()
            .map(|entry| entry.deadline);

        match next_deadline {
            Some(deadline) => {
                let sleep = tokio::time::sleep_until(deadline);
                tokio::select! {
                    _ = sleep => {
                        let current_epoch =
                            state.epoch.load(AtomicOrdering::SeqCst);
                        fire_expired_timers(
                            &state.system,
                            &state.path,
                            current_epoch,
                            &state.heap,
                            &state.cancelled,
                        )
                        .await;
                    }
                    _ = state.notify.notified() => {}
                }
            }
            None => {
                state.notify.notified().await;
            }
        }
    }
}

async fn fire_expired_timers<A: Actor + Handler<A>>(
    system: &SystemRef,
    path: &ActorPath,
    current_epoch: u64,
    heap: &Arc<Mutex<BinaryHeap<TimerEntry<A>>>>,
    cancelled: &Arc<Mutex<HashSet<TimerKey>>>,
) {
    let now = Instant::now();
    let mut to_fire = Vec::new();

    {
        let mut heap =
            heap.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        while let Some(entry) = heap.pop() {
            if entry.deadline > now {
                // The earliest deadline is in the future, so all remaining
                // entries are also in the future. Put it back and stop.
                heap.push(entry);
                break;
            }

            let was_cancelled = cancelled
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&entry.key);

            let active = !was_cancelled && entry.epoch == current_epoch;

            // Re-insert periodic timers with their next deadline. Periodic
            // timers created in an older epoch are discarded on this firing.
            if let Some(period) = entry.period
                && active
            {
                heap.push(TimerEntry {
                    deadline: entry.deadline + period,
                    key: entry.key,
                    msg: entry.msg.clone(),
                    period: entry.period,
                    epoch: entry.epoch,
                });
            }

            if active {
                to_fire.push(entry);
            }
        }

        drop(heap);
    }

    for entry in to_fire {
        match system.get_actor::<A>(path).await {
            Ok(actor_ref) => {
                if actor_ref.tell(entry.msg).await.is_err() {
                    debug!(
                        path = %path,
                        "Timer message could not be delivered; actor stopped"
                    );
                }
            }
            Err(_) => {
                debug!(
                    path = %path,
                    "Timer target actor not found; discarding timer message"
                );
            }
        }
    }
}
