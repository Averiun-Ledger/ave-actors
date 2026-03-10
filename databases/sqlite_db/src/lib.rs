#![doc = include_str!("../README.md")]

#[cfg(feature = "sqlite")]
mod db;

#[cfg(feature = "sqlite")]
pub use db::{SqliteCollection, SqliteManager};
#[cfg(feature = "export-sqlite")]
pub use rusqlite;
