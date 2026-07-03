#![doc = include_str!("../README.md")]

mod actor;
mod error;
mod handler;
mod helpers;
mod into_actor;
mod parent_ref;
mod path;
mod retries;
mod runner;
mod sink;
mod supervision;
mod system;
mod timer;

pub use actor::{
    Actor, ActorContext, ActorRef, ChildAction, Event, Handler, Message,
    OverflowStrategy, Response,
};
pub use error::Error;
pub use into_actor::{IntoActor, NotPersistentActor};
pub use parent_ref::ParentRef;
pub use path::ActorPath;

pub use helpers::encrypted_key::EncryptedKey;
pub use sink::{RetryPolicy, Sink, SinkEntry, Subscriber};

pub use retries::{RetryActor, RetryMessage};
pub use supervision::{
    CustomIntervalStrategy, ExponentialBackoffStrategy, IntervalStrategy,
    NoIntervalStrategy, RetryStrategy, Strategy, SupervisionStrategy,
};
pub use system::{
    ActorSystem, ActorSystemConfig, ShutdownReason, SystemEvent, SystemRef,
    SystemRunner,
};
pub use timer::TimerKey;

#[cfg(feature = "prometheus")]
pub mod metrics;
#[cfg(feature = "prometheus")]
pub use metrics::ActorMetrics;
