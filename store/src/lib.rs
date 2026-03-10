#![doc = include_str!("../README.md")]

pub mod config;
pub mod database;
pub mod error;
pub mod memory;
pub mod store;

pub use error::{Error, StoreOperation};
pub use store::InitializedActor;
