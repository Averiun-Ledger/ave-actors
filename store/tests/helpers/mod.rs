//! Shared helpers for store integration tests.

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
