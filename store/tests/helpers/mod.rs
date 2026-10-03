//! Shared helpers for store integration tests.

// (The `store_new!` macro below is exported at the crate root.)

/// Polls `check` until it returns `Some(value)`, failing the test after
/// `timeout` instead of sleeping a fixed amount and hoping it is enough.
///
/// Prefer this over `tokio::time::sleep` followed by a single assert: fixed
/// sleeps go flaky under loaded CI, while polling adapts both ways (fast
/// when ready early, patient when slow).
#[allow(dead_code)]
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

/// Creates a [`Store`](ave_actors_store::store::Store) with a fixed test actor
/// path and no metrics.
///
/// This macro abstracts over the `prometheus` feature so integration tests can
/// construct a store without caring whether the optional metrics argument is
/// present.
#[macro_export]
macro_rules! store_new {
    ($type:ty, $($arg:expr),* $(,)?) => {
        {
            #[cfg(feature = "prometheus")]
            {
                ::ave_actors_store::store::Store::<$type>::new(
                    $($arg),*,
                    ::std::option::Option::None,
                    ::std::sync::Arc::from("/test"),
                )
            }
            #[cfg(not(feature = "prometheus"))]
            {
                ::ave_actors_store::store::Store::<$type>::new(
                    $($arg),*
                )
            }
        }
    };
}
