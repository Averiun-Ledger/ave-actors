//! Retry and supervision strategies for actor startup failures.

use crate::Error;

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

impl SupervisionStrategy {
    /// Validates the strategy parameters when the strategy is not `Stop`.
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Stop => Ok(()),
            Self::Retry(strategy) => strategy.validate(),
        }
    }
}

/// Concrete retry strategy implementations.
#[derive(Debug, Clone)]
pub enum Strategy {
    /// Retry immediately with no delay between attempts.
    NoInterval(NoIntervalStrategy),
    /// Retry with a uniform delay between attempts, optionally with jitter.
    Interval(IntervalStrategy),
    /// Retry with exponential backoff, optionally with jitter.
    Exponential(ExponentialBackoffStrategy),
    /// Retry with custom-defined delays for each attempt.
    CustomIntervalStrategy(CustomIntervalStrategy),
}

impl RetryStrategy for Strategy {
    fn max_retries(&self) -> usize {
        match self {
            Self::NoInterval(strategy) => strategy.max_retries(),
            Self::Interval(strategy) => strategy.max_retries(),
            Self::Exponential(strategy) => strategy.max_retries(),
            Self::CustomIntervalStrategy(strategy) => strategy.max_retries(),
        }
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        match self {
            Self::NoInterval(strategy) => strategy.next_backoff(),
            Self::Interval(strategy) => strategy.next_backoff(),
            Self::Exponential(strategy) => strategy.next_backoff(),
            Self::CustomIntervalStrategy(strategy) => strategy.next_backoff(),
        }
    }
}

/// Maximum number of retries allowed in any built-in strategy.
const MAX_RETRIES: usize = 1000;

/// Minimum delay between retry attempts (must be strictly positive).
///
/// One microsecond is the smallest practical value; anything shorter would
/// effectively busy-loop the executor on retries.
const MIN_RETRY_DELAY: Duration = Duration::from_micros(1);

/// Maximum delay between retry attempts.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(3600);

impl Strategy {
    /// Validates that the strategy parameters are within safe ranges.
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::NoInterval(strategy) => strategy.validate(),
            Self::Interval(strategy) => strategy.validate(),
            Self::Exponential(strategy) => strategy.validate(),
            Self::CustomIntervalStrategy(strategy) => strategy.validate(),
        }
    }
}

fn validate_retry_delay(delay: Duration) -> Result<(), Error> {
    if delay < MIN_RETRY_DELAY {
        return Err(Error::InvalidConfiguration {
            component: "supervision strategy".to_owned(),
            reason: format!(
                "retry delay {:?} is below the minimum {:?}",
                delay, MIN_RETRY_DELAY
            ),
        });
    }
    if delay > MAX_RETRY_DELAY {
        return Err(Error::InvalidConfiguration {
            component: "supervision strategy".to_owned(),
            reason: format!(
                "retry delay {:?} exceeds the maximum {:?}",
                delay, MAX_RETRY_DELAY
            ),
        });
    }
    Ok(())
}

impl Default for Strategy {
    fn default() -> Self {
        Self::NoInterval(NoIntervalStrategy::default())
    }
}

/// Applies ±25% jitter to a duration.
fn apply_jitter(duration: Duration) -> Duration {
    let base_ms = duration.as_millis() as u64;
    let jitter_range = base_ms / 4;
    let jitter = fastrand::u64(0..=jitter_range * 2) as i64;
    let offset = jitter - jitter_range as i64;
    let result_ms = if offset >= 0 {
        base_ms.saturating_add(offset as u64)
    } else {
        base_ms.saturating_sub((-offset) as u64)
    };
    Duration::from_millis(result_ms)
}

/// Retries startup immediately with no delay between attempts, up to `max_retries` times.
#[derive(Debug, Default, Clone)]
pub struct NoIntervalStrategy {
    /// Maximum number of retry attempts.
    max_retries: usize,
}

impl NoIntervalStrategy {
    /// Creates the strategy with up to `max_retries` immediate restart attempts.
    ///
    /// The returned strategy is **not** validated. Use [`NoIntervalStrategy::try_new`]
    /// or wrap it in [`Strategy`] and call [`Strategy::validate`] to check limits.
    pub const fn new(max_retries: usize) -> Self {
        Self { max_retries }
    }

    /// Creates the strategy, validating that `max_retries` is within the
    /// allowed range.
    pub fn try_new(max_retries: usize) -> Result<Self, Error> {
        let strategy = Self::new(max_retries);
        strategy.validate()?;
        Ok(strategy)
    }

    /// Validates that `max_retries` is within the allowed range.
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_retries > MAX_RETRIES {
            return Err(Error::InvalidConfiguration {
                component: "supervision strategy".to_owned(),
                reason: format!(
                    "max_retries {} exceeds the maximum {}",
                    self.max_retries, MAX_RETRIES
                ),
            });
        }
        Ok(())
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

/// Retries startup after a uniform delay between each attempt, up to `max_retries` times.
///
/// Jitter can be enabled to add random noise of ±25% to each delay,
/// preventing thundering herd when many actors retry simultaneously.
#[derive(Debug, Default, Clone)]
pub struct IntervalStrategy {
    /// Maximum number of retries before permanently failing an actor.
    max_retries: usize,
    /// Base wait duration before each retry attempt.
    duration: Duration,
    /// Whether to apply ±25% jitter to each delay.
    jitter: bool,
}

impl IntervalStrategy {
    /// Creates the strategy with up to `max_retries` attempts and `duration` wait between each.
    ///
    /// The returned strategy is **not** validated. Use [`IntervalStrategy::try_new`]
    /// or wrap it in [`Strategy`] and call [`Strategy::validate`] to check limits.
    pub const fn new(max_retries: usize, duration: Duration) -> Self {
        Self {
            max_retries,
            duration,
            jitter: false,
        }
    }

    /// Creates the strategy, validating `max_retries` and `duration`.
    pub fn try_new(
        max_retries: usize,
        duration: Duration,
    ) -> Result<Self, Error> {
        let strategy = Self::new(max_retries, duration);
        strategy.validate()?;
        Ok(strategy)
    }

    /// Creates the strategy with jitter enabled.
    ///
    /// The returned strategy is **not** validated. Use
    /// [`IntervalStrategy::try_with_jitter`] or wrap it in [`Strategy`] and
    /// call [`Strategy::validate`] to check limits.
    pub const fn with_jitter(max_retries: usize, duration: Duration) -> Self {
        Self {
            max_retries,
            duration,
            jitter: true,
        }
    }

    /// Creates the strategy with jitter enabled, validating `max_retries` and
    /// `duration`.
    pub fn try_with_jitter(
        max_retries: usize,
        duration: Duration,
    ) -> Result<Self, Error> {
        let strategy = Self::with_jitter(max_retries, duration);
        strategy.validate()?;
        Ok(strategy)
    }

    /// Validates that `max_retries` and `duration` are within allowed ranges.
    pub fn validate(&self) -> Result<(), Error> {
        validate_retry_delay(self.duration)?;
        if self.max_retries > MAX_RETRIES {
            return Err(Error::InvalidConfiguration {
                component: "supervision strategy".to_owned(),
                reason: format!(
                    "max_retries {} exceeds the maximum {}",
                    self.max_retries, MAX_RETRIES
                ),
            });
        }
        Ok(())
    }
}

impl RetryStrategy for IntervalStrategy {
    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        if self.jitter {
            Some(apply_jitter(self.duration))
        } else {
            Some(self.duration)
        }
    }
}

/// Retries startup with exponential backoff between attempts.
///
/// Delay formula: `min(base * multiplier^attempt, max)`.
/// Jitter can be enabled to add random noise of ±25% to each delay.
#[derive(Debug, Clone)]
pub struct ExponentialBackoffStrategy {
    /// Initial delay for the first retry attempt.
    base: Duration,
    /// Maximum delay cap.
    max: Duration,
    /// Multiplier applied to the delay on each attempt (typically 2).
    multiplier: u32,
    /// Whether to apply ±25% jitter to each delay.
    jitter: bool,
    /// Current attempt counter (increments on each `next_backoff` call).
    attempt: u32,
    /// Maximum number of retries before permanently failing an actor.
    max_retries: usize,
}

impl ExponentialBackoffStrategy {
    /// Creates the strategy with exponential backoff.
    ///
    /// The returned strategy is **not** validated. Use
    /// [`ExponentialBackoffStrategy::try_new`] or wrap it in [`Strategy`] and
    /// call [`Strategy::validate`] to check limits.
    pub const fn new(
        max_retries: usize,
        base: Duration,
        max: Duration,
        multiplier: u32,
    ) -> Self {
        Self {
            base,
            max,
            multiplier,
            jitter: false,
            attempt: 0,
            max_retries,
        }
    }

    /// Creates the strategy, validating all parameters.
    pub fn try_new(
        max_retries: usize,
        base: Duration,
        max: Duration,
        multiplier: u32,
    ) -> Result<Self, Error> {
        let strategy = Self::new(max_retries, base, max, multiplier);
        strategy.validate()?;
        Ok(strategy)
    }

    /// Creates the strategy with jitter enabled.
    ///
    /// The returned strategy is **not** validated. Use
    /// [`ExponentialBackoffStrategy::try_with_jitter`] or wrap it in
    /// [`Strategy`] and call [`Strategy::validate`] to check limits.
    pub const fn with_jitter(
        max_retries: usize,
        base: Duration,
        max: Duration,
        multiplier: u32,
    ) -> Self {
        Self {
            base,
            max,
            multiplier,
            jitter: true,
            attempt: 0,
            max_retries,
        }
    }

    /// Creates the strategy with jitter enabled, validating all parameters.
    pub fn try_with_jitter(
        max_retries: usize,
        base: Duration,
        max: Duration,
        multiplier: u32,
    ) -> Result<Self, Error> {
        let strategy = Self::with_jitter(max_retries, base, max, multiplier);
        strategy.validate()?;
        Ok(strategy)
    }

    /// Validates that all parameters are within allowed ranges.
    pub fn validate(&self) -> Result<(), Error> {
        validate_retry_delay(self.base)?;
        validate_retry_delay(self.max)?;
        if self.base > self.max {
            return Err(Error::InvalidConfiguration {
                component: "supervision strategy".to_owned(),
                reason: format!(
                    "exponential backoff base {:?} is greater than max {:?}",
                    self.base, self.max
                ),
            });
        }
        if self.multiplier < 2 {
            return Err(Error::InvalidConfiguration {
                component: "supervision strategy".to_owned(),
                reason: format!(
                    "exponential backoff multiplier {} must be at least 2",
                    self.multiplier
                ),
            });
        }
        if self.max_retries > MAX_RETRIES {
            return Err(Error::InvalidConfiguration {
                component: "supervision strategy".to_owned(),
                reason: format!(
                    "max_retries {} exceeds the maximum {}",
                    self.max_retries, MAX_RETRIES
                ),
            });
        }
        Ok(())
    }
}

impl RetryStrategy for ExponentialBackoffStrategy {
    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn next_backoff(&mut self) -> Option<Duration> {
        let base_ms = self.base.as_millis() as u64;
        let multiplier = self.multiplier as u64;
        let delay_ms =
            base_ms.saturating_mul(multiplier.saturating_pow(self.attempt));
        let delay = Duration::from_millis(delay_ms).min(self.max);
        self.attempt += 1;
        if self.jitter {
            Some(apply_jitter(delay))
        } else {
            Some(delay)
        }
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
    ///
    /// The returned strategy is **not** validated. Use
    /// [`CustomIntervalStrategy::try_new`] or wrap it in [`Strategy`] and call
    /// [`Strategy::validate`] to check limits.
    pub fn new(durations: VecDeque<Duration>) -> Self {
        let max_retries = durations.len();
        Self {
            durations,
            max_retries,
        }
    }

    /// Creates the strategy, validating the number of durations and each delay.
    pub fn try_new(durations: VecDeque<Duration>) -> Result<Self, Error> {
        let strategy = Self::new(durations);
        strategy.validate()?;
        Ok(strategy)
    }

    /// Validates that the number of durations and each delay are within
    /// allowed ranges.
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_retries > MAX_RETRIES {
            return Err(Error::InvalidConfiguration {
                component: "supervision strategy".to_owned(),
                reason: format!(
                    "custom interval count {} exceeds the maximum {}",
                    self.max_retries, MAX_RETRIES
                ),
            });
        }
        for delay in &self.durations {
            validate_retry_delay(*delay)?;
        }
        Ok(())
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
    fn test_interval_strategy() {
        let mut strategy = IntervalStrategy::new(3, Duration::from_secs(1));
        assert_eq!(strategy.max_retries(), 3);
        assert_eq!(strategy.next_backoff(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn test_interval_strategy_with_jitter() {
        let mut strategy =
            IntervalStrategy::with_jitter(3, Duration::from_secs(1));
        assert_eq!(strategy.max_retries(), 3);
        let Some(delay) = strategy.next_backoff() else {
            panic!("jittered delay should be present");
        };
        let expected_range =
            Duration::from_millis(750)..=Duration::from_millis(1250);
        assert!(
            expected_range.contains(&delay),
            "jittered delay {:?} should be within ±25% of 1s",
            delay
        );
    }

    #[test]
    fn test_exponential_backoff_strategy() {
        let mut strategy = ExponentialBackoffStrategy::new(
            5,
            Duration::from_millis(100),
            Duration::from_secs(1),
            2,
        );
        assert_eq!(strategy.max_retries(), 5);
        assert_eq!(strategy.next_backoff(), Some(Duration::from_millis(100)));
        assert_eq!(strategy.next_backoff(), Some(Duration::from_millis(200)));
        assert_eq!(strategy.next_backoff(), Some(Duration::from_millis(400)));
        assert_eq!(strategy.next_backoff(), Some(Duration::from_millis(800)));
        // Should cap at max
        assert_eq!(strategy.next_backoff(), Some(Duration::from_secs(1)));
    }

    #[test]
    fn test_exponential_backoff_with_jitter() {
        let mut strategy = ExponentialBackoffStrategy::with_jitter(
            3,
            Duration::from_secs(1),
            Duration::from_secs(10),
            2,
        );
        let Some(delay) = strategy.next_backoff() else {
            panic!("jittered delay should be present");
        };
        let expected_range =
            Duration::from_millis(750)..=Duration::from_millis(1250);
        assert!(
            expected_range.contains(&delay),
            "jittered delay {:?} should be within ±25% of 1s",
            delay
        );
    }

    #[test]
    fn test_custom_interval_strategy() {
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

    #[test]
    fn test_custom_interval_try_new_valid() {
        let strategy = CustomIntervalStrategy::try_new(VecDeque::from([
            Duration::from_secs(1),
            Duration::from_secs(2),
        ]));
        let Ok(strategy) = strategy else {
            panic!("valid custom interval strategy should be created");
        };
        assert_eq!(strategy.max_retries(), 2);
    }

    #[test]
    fn test_custom_interval_try_new_invalid_zero_delay() {
        let result =
            CustomIntervalStrategy::try_new(VecDeque::from([Duration::ZERO]));
        assert!(result.is_err());
    }

    #[test]
    fn test_custom_interval_try_new_too_many_durations() {
        let durations: VecDeque<Duration> =
            (0..=MAX_RETRIES).map(|_| Duration::from_secs(1)).collect();
        let result = CustomIntervalStrategy::try_new(durations);
        assert!(result.is_err());
    }
}
