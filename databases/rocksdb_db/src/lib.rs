#![doc = include_str!("../README.md")]

#[cfg(feature = "rocksdb")]
pub mod db;

#[cfg(feature = "rocksdb")]
pub use db::{RocksDbManager, RocksDbStore};
#[cfg(feature = "export-rocksdb")]
pub use rocksdb;
