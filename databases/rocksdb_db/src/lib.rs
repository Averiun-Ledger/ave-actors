//! RocksDB database module.
//!
//! This module contains the RocksDB database implementation.
//!
#[cfg(feature = "rocksdb")]
pub mod db;

#[cfg(feature = "rocksdb")]
pub use db::{RocksDbManager, RocksDbStore};
#[cfg(feature = "export-rocksdb")]
pub use rocksdb;
