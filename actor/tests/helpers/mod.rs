//! Shared helpers for actor integration tests.

//! Polls `check` until it returns `Some(value)`, failing the test after
//! `timeout` instead of sleeping a fixed amount and hoping it is enough.
//!
//! Prefer this over `tokio::time::sleep` followed by a single assert:
//! fixed sleeps go flaky under loaded CI, while polling adapts both ways
//! (fast when ready early, patient when slow).
pub async fn assert_eventually<T, Fut, F>(
    what: &str,
    timeout: std::time::Duration,
    mut check: F,
) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
