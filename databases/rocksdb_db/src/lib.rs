#![doc = include_str!("../README.md")]

#[cfg(feature = "rocksdb")]
mod db;

#[cfg(feature = "rocksdb")]
pub use db::{RocksDbManager, RocksDbStore};
#[cfg(feature = "export-rocksdb")]
pub use rocksdb;
