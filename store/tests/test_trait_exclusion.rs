// This test file verifies that PersistentActor and NotPersistentActor are mutually exclusive
// UNCOMMENT the code below to verify the compile error

/*
use actor::{Actor, Handler, Message, Response, Event, ActorContext, ActorPath, NotPersistentActor};
use store::store::{PersistentActor, FullPersistence};
use serde::{Serialize, Deserialize};
use async_trait::async_trait;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConflictingActor {
    value: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Msg;
impl Message for Msg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Evt;
impl Event for Evt {}

#[async_trait]
impl Actor for ConflictingActor {
    type Message = Msg;
    type Response = ();
    type Event = Evt;
}

#[async_trait]
impl Handler<ConflictingActor> for ConflictingActor {
    async fn handle_message(
        &mut self,
        _: ActorPath,
        _: Msg,
        _: &mut ActorContext<ConflictingActor>,
    ) -> Result<(), actor::Error> {
        Ok(())
    }
}

// First implement PersistentActor
#[async_trait]
impl PersistentActor for ConflictingActor {
    type Persistence = FullPersistence;
    type InitParams = i32;

    fn create_initial(value: i32) -> Self {
        Self { value }
    }

    fn apply(&mut self, _: &Self::Event) -> Result<(), actor::Error> {
        Ok(())
    }
}

// Now try to implement NotPersistentActor - this WILL cause a compile error!
// Error: conflicting implementations of trait `actor::into_actor::sealed::Sealed`
impl NotPersistentActor for ConflictingActor {}

#[test]
fn this_should_not_compile() {
    // If you can run this test, the mutual exclusion is broken
}
*/
