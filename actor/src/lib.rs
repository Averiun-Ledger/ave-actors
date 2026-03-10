#![doc = include_str!("../README.md")]

mod actor;
mod error;
mod handler;
mod helpers;
mod into_actor;
mod path;
mod retries;
mod runner;
mod sink;
mod supervision;
mod system;

pub use actor::{
    Actor, ActorContext, ActorRef, ChildAction, Event, Handler, Message,
    Response,
};
pub use error::Error;
pub use into_actor::{IntoActor, NotPersistentActor};
pub use path::ActorPath;

pub use helpers::encrypted_key::EncryptedKey;
pub use sink::{Sink, Subscriber};

pub use retries::{RetryActor, RetryMessage};
pub use supervision::{
    CustomIntervalStrategy, FixedIntervalStrategy, NoIntervalStrategy,
    RetryStrategy, Strategy, SupervisionStrategy,
};
pub use system::{
    ActorSystem, ShutdownReason, SystemEvent, SystemRef, SystemRunner,
};
