# ave-actors-actor

Async actor model for Rust built on [Tokio](https://tokio.rs). Actors communicate exclusively through message passing, run concurrently in async tasks, and support supervision trees, lifecycle hooks, persistence, and event broadcasting.

This crate is part of the [ave-actors](https://github.com/Averiun-Ledger/ave-actors) workspace.

---

## Core concepts

| Concept | Description |
|---|---|
| `Actor` | Trait that defines behavior, lifecycle hooks, and supervision strategy |
| `Handler<M>` | Trait for processing a message type `M` |
| `ActorRef<A>` | Typed handle to send messages to an actor |
| `ActorContext<A>` | Provided to the actor during execution; used to spawn children, emit errors, publish events |
| `ActorSystem` | Creates and manages actors; holds a registry keyed by `ActorPath` |
| `ActorPath` | Hierarchical address, e.g. `/user/parent/child` |

---

## Quick start

```rust
use ave_actors_actor::{Actor, ActorContext, ActorPath, ActorRef, Handler, Message, Response, Event};
use ave_actors_actor::{ActorSystem, NotPersistentActor};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

// --- Messages ---

#[derive(Clone)]
struct Ping;

#[derive(Clone)]
struct Pong;

impl Message for Ping {}
impl Response for Pong {}

// --- Actor ---

struct MyActor;

impl NotPersistentActor for MyActor {}

#[async_trait]
impl Actor for MyActor {
    type Message = Ping;
    type Event   = ();
    type Response = Pong;

    fn get_span(id: &str, _parent: Option<tracing::Span>) -> tracing::Span {
        tracing::info_span!("MyActor", id)
    }
}

#[async_trait]
impl Handler<MyActor> for MyActor {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: Ping,
        _ctx: &mut ActorContext<MyActor>,
    ) -> Result<Pong, ave_actors_actor::Error> {
        Ok(Pong)
    }
}

// --- Main ---

#[tokio::main]
async fn main() {
    let graceful = CancellationToken::new();
    let crash    = CancellationToken::new();

    let (system, mut runner) = ActorSystem::create(graceful, crash);

    let actor_ref: ActorRef<MyActor> =
        system.create_root_actor("my-actor", MyActor).await.unwrap();

    let _pong = actor_ref.ask(Ping).await.unwrap();

    system.stop_system();
    runner.run().await;
}
```

---

## Lifecycle

```
pre_start → [message loop] → pre_stop → post_stop
                ↑ (on failure + Retry strategy)
            pre_restart
```

Override any hook in the `Actor` trait:

```rust
async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Error> { .. }
async fn pre_restart(&mut self, ctx: &mut ActorContext<Self>, err: Option<&Error>) -> Result<(), Error> { .. }
async fn pre_stop(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Error> { .. }
async fn post_stop(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Error> { .. }
```

---

## Supervision

Root actors can restart automatically on failure:

```rust
use ave_actors_actor::{SupervisionStrategy, Strategy, FixedIntervalStrategy};
use std::time::Duration;

fn supervision_strategy(&self) -> SupervisionStrategy {
    SupervisionStrategy::Retry(Strategy::FixedInterval(
        FixedIntervalStrategy::new(5, Duration::from_secs(1)),
    ))
}
```

Child faults are escalated to the parent via `Handler::on_child_fault`, which returns a `ChildAction` (`Stop`, `Restart`, or `Delegate`).

---

## Message passing

```rust
// Fire-and-forget
actor_ref.tell(MyMsg).await?;

// Request-response (waits for reply)
let response = actor_ref.ask(MyMsg).await?;

// With custom timeout
let response = actor_ref.ask_timeout(MyMsg, Duration::from_secs(5)).await?;

// Graceful stop, waits for actor to finish
actor_ref.ask_stop().await?;
```

---

## Events

Actors broadcast typed events to subscribers:

```rust
// Inside the actor:
ctx.publish_event(MyEvent::SomethingHappened).await;

// Subscriber outside:
let mut rx = actor_ref.subscribe();
while let Ok(event) = rx.recv().await { .. }

// Or run a sink task:
use ave_actors_actor::Sink;
system.run_sink(Sink::new(actor_ref.subscribe(), my_subscriber)).await;
```

`Subscriber<E>` is a trait with a single `notify(&self, event: E)` method.

---

## Actor paths

Paths are hierarchical strings following a `/user/<name>` convention.

```rust
let path = ActorPath::from("/user/parent");
let child_path = path / "child"; // "/user/parent/child"

path.is_parent_of(&child_path); // true
path.level();                   // 2
path.key();                     // "parent"
```
