#![doc = include_str!("../README.md")]

mod actor;
mod error;
mod handler;
mod helpers;
mod into_actor;
mod middleware;
mod parent_ref;
mod path;
mod pipe;
mod retries;
mod router;
mod runner;
mod selection;
mod sink;
mod supervision;
mod system;
mod testkit;
mod timer;

pub use actor::{
    Actor, ActorContext, ActorRef, ChildAction, Event, Handler, Message,
    OverflowStrategy, Response,
};
pub use error::Error;
pub use into_actor::{IntoActor, NotPersistentActor};
pub use middleware::{Intercept, Interceptor, MessageMetadata};
pub use parent_ref::ParentRef;
pub use path::ActorPath;
pub use pipe::pipe_to;

pub use helpers::encrypted_key::EncryptedKey;
pub use sink::{RetryPolicy, Sink, SinkEntry, Subscriber};

pub use retries::{RetryActor, RetryMessage};
pub use router::{Router, RouterMsg, RouterResponse, RoutingStrategy};
pub use selection::ActorSelection;
pub use supervision::{
    CustomIntervalStrategy, ExponentialBackoffStrategy, IntervalStrategy,
    NoIntervalStrategy, RetryStrategy, Strategy, SupervisionStrategy,
};
pub use system::{
    ActorSystem, ActorSystemConfig, ShutdownReason, SystemEvent, SystemRef,
    SystemRunner,
};
pub use testkit::{ProbeActor, TestProbe, TestSystem};
pub use timer::TimerKey;

#[cfg(feature = "prometheus")]
pub mod metrics;
#[cfg(feature = "prometheus")]
pub use metrics::ActorMetrics;
