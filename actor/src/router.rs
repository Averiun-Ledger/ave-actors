//! Work-sharing routers: distribute messages over a pool of workers.
//!
//! A [`Router`] fronts any set of same-type workers and forwards each
//! message according to its [`RoutingStrategy`]. Workers can be
//! attached from already-running actors ([`Router::new`]) or spawned
//! as the router's children from templates ([`Router::pooled`]).
//! Faulted pool children restart in place, so the pool size is stable.
//!
//! Control messages ([`RouterMsg`]) also manage membership (`AddWorker`
//! / `RemoveWorker` / `WorkerCount`) and the strategy at runtime.

use async_trait::async_trait;
use tracing::info_span;

use crate::into_actor::NotPersistentActor;
use crate::{
    Actor, ActorContext, ActorPath, ActorRef, ChildAction, Error, Handler,
    Message, Response,
};

// ---------------------------------------------------------------------------
// Strategy
// ---------------------------------------------------------------------------

/// How a [`Router`] picks workers for each message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingStrategy {
    /// Cycle through workers in order.
    RoundRobin,
    /// Pick a uniformly random worker per message.
    Random,
    /// Send to every worker; an `ask` resolves with the first
    /// successful response (late responses are dropped).
    Broadcast,
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// Messages accepted by [`Router`].
#[derive(Debug)]
pub enum RouterMsg<W: Actor> {
    /// Forward the inner message according to the strategy.
    Route(W::Message),
    /// Attach a running worker (ignored when already present).
    AddWorker(ActorRef<W>),
    /// Detach the worker at `path` (no-op when absent).
    RemoveWorker(ActorPath),
    /// Report the current worker count.
    WorkerCount,
    /// Switch strategy for subsequent messages.
    SetStrategy(RoutingStrategy),
}

impl<W: Actor> Clone for RouterMsg<W> {
    fn clone(&self) -> Self {
        match self {
            Self::Route(message) => Self::Route(message.clone()),
            Self::AddWorker(worker) => Self::AddWorker(worker.clone()),
            Self::RemoveWorker(path) => Self::RemoveWorker(path.clone()),
            Self::WorkerCount => Self::WorkerCount,
            Self::SetStrategy(strategy) => Self::SetStrategy(*strategy),
        }
    }
}

impl<W: Actor> Message for RouterMsg<W> {}

/// Responses returned by [`Router`].
#[derive(Debug)]
pub enum RouterResponse<W: Actor> {
    /// The selected worker's response.
    Routed(W::Response),
    /// Current worker count.
    Count(usize),
    /// Membership or strategy changed.
    Updated,
}

impl<W: Actor> Response for RouterResponse<W> {}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Distributes messages over `W` workers.
///
/// See the [module](self) documentation for the routing contract.
#[derive(Debug)]
pub struct Router<W: Actor + Handler<W> + NotPersistentActor> {
    workers: Vec<ActorRef<W>>,
    pending: Vec<W>,
    strategy: RoutingStrategy,
    next: usize,
}

impl<W: Actor + Handler<W> + NotPersistentActor> Router<W> {
    /// Routes over already-running workers.
    pub const fn new(
        workers: Vec<ActorRef<W>>,
        strategy: RoutingStrategy,
    ) -> Self {
        Self {
            workers,
            pending: Vec::new(),
            strategy,
            next: 0,
        }
    }

    /// Spawns one child per template in [`pre_start`](Actor::pre_start)
    /// (`pool-0`, `pool-1`, ...) and routes over them. Extra workers
    /// can join later via [`RouterMsg::AddWorker`].
    pub const fn pooled(templates: Vec<W>, strategy: RoutingStrategy) -> Self {
        Self {
            workers: Vec::new(),
            pending: templates,
            strategy,
            next: 0,
        }
    }

    /// Current worker count.
    pub const fn len(&self) -> usize {
        self.workers.len()
    }

    /// Returns `true` when no worker is attached.
    pub const fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    fn pick(&mut self) -> Option<ActorRef<W>> {
        if self.workers.is_empty() {
            return None;
        }
        let worker = match self.strategy {
            RoutingStrategy::RoundRobin => {
                let at = self.next % self.workers.len();
                self.next = self.next.wrapping_add(1);
                at
            }
            RoutingStrategy::Random => fastrand::usize(..self.workers.len()),
            RoutingStrategy::Broadcast => {
                // Broadcast fans out to all workers; no single pick.
                return None;
            }
        };
        Some(self.workers[worker].clone())
    }

    fn no_workers<T>() -> Result<T, Error> {
        Err(Error::Functional {
            description: "router has no workers".to_owned(),
        })
    }
}

impl<W: Actor + Handler<W> + NotPersistentActor> NotPersistentActor
    for Router<W>
{
}

#[async_trait]
impl<W: Actor + Handler<W> + NotPersistentActor> Actor for Router<W> {
    type Message = RouterMsg<W>;
    type Response = RouterResponse<W>;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("Router", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        for (i, template) in self.pending.drain(..).enumerate() {
            let child =
                ctx.create_child(&format!("pool-{i}"), template).await?;
            self.workers.push(child);
        }
        Ok(())
    }
}

#[async_trait]
impl<W: Actor + Handler<W> + NotPersistentActor> Handler<Self> for Router<W> {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        msg: RouterMsg<W>,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<RouterResponse<W>, Error> {
        match msg {
            RouterMsg::Route(inner) => {
                if self.strategy == RoutingStrategy::Broadcast {
                    return self.broadcast(inner).await;
                }
                let Some(worker) = self.pick() else {
                    return Self::no_workers();
                };
                let response = worker.ask(inner).await?;
                Ok(RouterResponse::Routed(response))
            }
            RouterMsg::AddWorker(worker) => {
                if !self.workers.iter().any(|w| w.path() == worker.path()) {
                    self.workers.push(worker);
                }
                Ok(RouterResponse::Updated)
            }
            RouterMsg::RemoveWorker(path) => {
                self.workers.retain(|w| w.path() != path);
                Ok(RouterResponse::Updated)
            }
            RouterMsg::WorkerCount => {
                Ok(RouterResponse::Count(self.workers.len()))
            }
            RouterMsg::SetStrategy(strategy) => {
                self.strategy = strategy;
                Ok(RouterResponse::Updated)
            }
        }
    }

    async fn on_child_fault(
        &mut self,
        _error: Error,
        _ctx: &mut ActorContext<Self>,
    ) -> ChildAction {
        // Pool children restart in place: the pool size never shrinks
        // on transient worker faults.
        ChildAction::Restart
    }
}

impl<W: Actor + Handler<W> + NotPersistentActor> Router<W> {
    async fn broadcast(
        &self,
        message: W::Message,
    ) -> Result<RouterResponse<W>, Error> {
        if self.workers.is_empty() {
            return Self::no_workers();
        }
        let mut set = tokio::task::JoinSet::new();
        for worker in &self.workers {
            let worker = worker.clone();
            let message = message.clone();
            set.spawn(async move { worker.ask(message).await });
        }
        let mut last_error = None;
        while let Some(outcome) = set.join_next().await {
            match outcome {
                Ok(Ok(response)) => {
                    set.abort_all();
                    return Ok(RouterResponse::Routed(response));
                }
                Ok(Err(error)) => last_error = Some(error),
                Err(join_error) => {
                    last_error = Some(Error::Functional {
                        description: format!(
                            "broadcast worker panicked: {join_error}"
                        ),
                    });
                }
            }
        }
        Err(last_error.unwrap_or_else(|| Error::Functional {
            description: "broadcast reached no worker".to_owned(),
        }))
    }
}
