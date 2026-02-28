//! # SQLite database module.
//!
//! This module contains the SQLite database backend implementation.
//!

//pub mod sqlite;
#[cfg(feature = "sqlite")]
mod db;

#[cfg(feature = "sqlite")]
pub use db::{SqliteCollection, SqliteManager};
#[cfg(feature = "export-sqlite")]
pub use rusqlite;
