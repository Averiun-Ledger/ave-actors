//! Retry and supervision strategies for actor startup failures.

use std::{collections::VecDeque, fmt::Debug, time::Duration};

/// Defines how many times and how quickly a failing actor is restarted.
///
/// Implement this to create a custom backoff policy. The actor system calls
/// `max_retries` to determine the attempt budget and `next_backoff` before
/// each retry to get the delay (or `None` for immediate retry).
pub trait RetryStrategy: Debug + Send + Sync {
    /// Returns the maximum number of restart attempts before the actor is permanently stopped.
    fn max_retries(&self) -> usize;

    /// Returns the delay before the next restart attempt, or `None` to retry immediately.
    fn next_backoff(&mut self) -> Option<Duration>;
}

/// Determines what happens when an actor fails during startup.
///
/// Pass this when creating an actor to control whether the system stops
/// on first failure or retries with a configurable back-off strategy.
#[derive(Debug, Clone)]
pub enum SupervisionStrategy {
    /// Stop the actor permanently on the first startup error.
    Stop,
    /// Retry startup using the given [`Strategy`].
    Retry(Strategy),
}

/// Concrete retry strategy implementations. Choose `NoInterval`, `FixedInterval`, or `CustomIntervalStrategy`.
#[derive(Debug, Clone)]
pub enum Strategy {
    /// Retry immediately with no delay between attempts.
    NoInterval(NoIntervalStrategy),
    /// Retry with a fixed delay between attempts.
    FixedInterval(FixedIntervalStrategy),
    /// Retry with custom-defined delays for each attempt.
    CustomIntervalStrategy(CustomIntervalStrategy),
}

impl RetryStrategy for Strategy {
    fn max_retries(&self) -> usize {
        match self {
            Self::NoInterval(strategy) => strategy.max_retries(),
            Self::FixedInterval(strategy) => strategy.max_retries(),
            Self::CustomIntervalStrategy(strategy) => strategy.max_retries(),
        }
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        match self {
            Self::NoInterval(strategy) => strategy.next_backoff(),
            Self::FixedInterval(strategy) => strategy.next_backoff(),
            Self::CustomIntervalStrategy(strategy) => strategy.next_backoff(),
        }
    }
}

impl Default for Strategy {
    fn default() -> Self {
        Self::NoInterval(NoIntervalStrategy::default())
    }
}

/// Retries startup immediately with no delay between attempts, up to `max_retries` times.
#[derive(Debug, Default, Clone)]
pub struct NoIntervalStrategy {
    /// Maximum number of retry attempts.
    max_retries: usize,
}

impl NoIntervalStrategy {
    /// Creates the strategy with up to `max_retries` immediate restart attempts.
    pub const fn new(max_retries: usize) -> Self {
        Self { max_retries }
    }
}

impl RetryStrategy for NoIntervalStrategy {
    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        None
    }
}

/// Retries startup after a fixed delay between each attempt, up to `max_retries` times.
#[derive(Debug, Default, Clone)]
pub struct FixedIntervalStrategy {
    /// Maximum number of retries before permanently failing an actor.
    max_retries: usize,
    /// Fixed wait duration before each retry attempt.
    duration: Duration,
}

impl FixedIntervalStrategy {
    /// Creates the strategy with up to `max_retries` attempts and `duration` wait between each.
    pub const fn new(max_retries: usize, duration: Duration) -> Self {
        Self {
            max_retries,
            duration,
        }
    }
}

impl RetryStrategy for FixedIntervalStrategy {
    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        Some(self.duration)
    }
}

/// Retries startup with a per-attempt delay sequence defined by a `VecDeque<Duration>`.
///
/// The number of durations provided sets the retry budget: each call to
/// `next_backoff` pops one duration from the front until the queue is empty.
#[derive(Debug, Default, Clone)]
pub struct CustomIntervalStrategy {
    /// Queue of delay durations for each retry attempt.
    /// Each call to next_backoff() pops one duration from the front.
    durations: VecDeque<Duration>,
    /// Maximum number of retries (equal to the number of durations provided).
    max_retries: usize,
}

impl CustomIntervalStrategy {
    /// Creates the strategy from `durations`; `max_retries` is set to `durations.len()`.
    pub fn new(durations: VecDeque<Duration>) -> Self {
        let max_retries = durations.len();
        Self {
            durations,
            max_retries,
        }
    }
}

impl RetryStrategy for CustomIntervalStrategy {
    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        self.durations.pop_front()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_no_interval_strategy() {
        let mut strategy = NoIntervalStrategy::new(3);
        assert_eq!(strategy.max_retries(), 3);
        assert_eq!(strategy.next_backoff(), None);
    }

    #[test]
    fn test_fixed_interval_strategy() {
        let mut strategy =
            FixedIntervalStrategy::new(3, Duration::from_secs(1));
        assert_eq!(strategy.max_retries(), 3);
        assert_eq!(strategy.next_backoff(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn test_exponential_custom_strategy() {
        let mut strategy = CustomIntervalStrategy::new(VecDeque::from([
            Duration::from_secs(1),
            Duration::from_secs(2),
            Duration::from_secs(3),
        ]));
        assert_eq!(strategy.max_retries(), 3);
        assert!(strategy.next_backoff().is_some());
        assert!(strategy.next_backoff().is_some());
        assert!(strategy.next_backoff().is_some());
        assert!(strategy.next_backoff().is_none());
    }
}
