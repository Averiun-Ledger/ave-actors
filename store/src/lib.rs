#![doc = include_str!("../README.md")]

pub mod config;
pub mod database;
pub mod error;
pub mod memory;
pub mod store;

#[cfg(feature = "prometheus")]
pub mod metrics;

pub use error::{Error, StoreOperation};
pub use store::InitializedActor;

#[cfg(feature = "prometheus")]
pub use metrics::StoreMetrics;
