//! Tests for `SystemRef::actor_tree` and `format_tree`.

use std::time::Duration;

use async_trait::async_trait;
use ave_actors_actor::{
    Actor, ActorContext, ActorPath, Error, Handler, Message,
    NotPersistentActor, Response, TestSystem, format_tree,
};
use serde::{Deserialize, Serialize};
use test_log::test;
use tracing::info_span;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum TreeMsg {
    Ping,
}

impl Message for TreeMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TreeResponse {
    ok: bool,
}

impl Response for TreeResponse {}

#[derive(Clone)]
struct TreeChild;

impl NotPersistentActor for TreeChild {}

#[async_trait]
impl Actor for TreeChild {
    type Message = TreeMsg;
    type Response = TreeResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TreeChild", id = %id)
    }
}

#[async_trait]
impl Handler<Self> for TreeChild {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: TreeMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<TreeResponse, Error> {
        Ok(TreeResponse { ok: true })
    }
}

#[derive(Clone)]
struct TreeParent;

impl NotPersistentActor for TreeParent {}

#[async_trait]
impl Actor for TreeParent {
    type Message = TreeMsg;
    type Response = TreeResponse;
    type Event = ();
    type SinkEvent = Self::Event;
    type ChildError = Error;
    type ChildFault = Error;

    fn get_span(
        id: &str,
        _parent_span: Option<tracing::Span>,
    ) -> tracing::Span {
        info_span!("TreeParent", id = %id)
    }

    async fn pre_start(
        &mut self,
        ctx: &mut ActorContext<Self>,
    ) -> Result<(), Error> {
        ctx.create_child::<TreeChild, _>("child_a", TreeChild).await?;
        ctx.create_child::<TreeChild, _>("child_b", TreeChild).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<Self> for TreeParent {
    async fn handle_message(
        &mut self,
        _sender: ActorPath,
        _msg: TreeMsg,
        _ctx: &mut ActorContext<Self>,
    ) -> Result<TreeResponse, Error> {
        Ok(TreeResponse { ok: true })
    }
}

async fn wait_for_path(
    system: &ave_actors_actor::SystemRef,
    path: &str,
    present: bool,
) -> Result<(), Error> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let target = ActorPath::from(path);
    loop {
        let found = system
            .actor_tree()
            .iter()
            .flat_map(subtree_paths)
            .any(|p| p == target);
        if found == present {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            return Err(Error::Functional {
                description: format!(
                    "path {path} presence did not become {present}"
                ),
            });
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn subtree_paths(
    node: &ave_actors_actor::ActorTreeNode,
) -> Vec<ActorPath> {
    let mut out = vec![ActorPath::from(node.path.as_str())];
    for child in &node.children {
        out.extend(subtree_paths(child));
    }
    out
}

#[test(tokio::test)]
async fn empty_system_has_no_tree() -> Result<(), Error> {
    let harness = TestSystem::start();
    assert!(harness.system().actor_tree().is_empty());
    harness.shutdown().await;
    Ok(())
}

#[test(tokio::test)]
async fn tree_reports_roots_children_and_types() -> Result<(), Error> {
    let harness = TestSystem::start();
    let system = harness.system();

    system.create_root_actor("tree_parent", TreeParent).await?;
    system.create_root_actor("tree_lonely", TreeChild).await?;
    wait_for_path(system, "/user/tree_parent/child_b", true).await?;

    let tree = system.actor_tree();
    assert_eq!(tree.len(), 2);
    assert_eq!(tree[0].path, "/user/tree_lonely");
    assert_eq!(tree[1].path, "/user/tree_parent");
    assert!(tree[0].actor_type.ends_with("TreeChild"));
    assert!(tree[1].actor_type.ends_with("TreeParent"));

    let kids = &tree[1].children;
    assert_eq!(kids.len(), 2);
    assert_eq!(kids[0].path, "/user/tree_parent/child_a");
    assert_eq!(kids[1].path, "/user/tree_parent/child_b");
    assert!(kids.iter().all(|k| k.actor_type.ends_with("TreeChild")));
    assert!(kids.iter().all(|k| k.children.is_empty()));

    let sizes: usize = tree.iter().map(|n| n.size()).sum();
    assert_eq!(sizes, 4);

    harness.shutdown().await;
    Ok(())
}

#[test(tokio::test)]
async fn stopped_actors_leave_the_tree() -> Result<(), Error> {
    let harness = TestSystem::start();
    let system = harness.system();

    system.create_root_actor("prune_parent", TreeParent).await?;
    wait_for_path(system, "/user/prune_parent/child_a", true).await?;

    let child: ave_actors_actor::ActorRef<TreeChild> = system
        .get_actor(&ActorPath::from("/user/prune_parent/child_a"))
        .await?;
    child.ask_stop().await?;
    wait_for_path(system, "/user/prune_parent/child_a", false).await?;

    let tree = system.actor_tree();
    assert_eq!(tree.len(), 1);
    assert_eq!(tree[0].children.len(), 1);
    assert_eq!(tree[0].children[0].path, "/user/prune_parent/child_b");

    harness.shutdown().await;
    Ok(())
}

#[test(tokio::test)]
async fn format_renders_indented_paths() -> Result<(), Error> {
    let harness = TestSystem::start();
    let system = harness.system();

    system.create_root_actor("fmt_parent", TreeParent).await?;
    wait_for_path(system, "/user/fmt_parent/child_a", true).await?;

    let rendered = format_tree(&system.actor_tree());
    let parent_type = system
        .actor_type_of(&ActorPath::from("/user/fmt_parent"))
        .unwrap_or("unknown");
    assert!(rendered.contains(&format!("/user/fmt_parent ({parent_type})")));
    assert!(rendered.contains("/user/fmt_parent/child_a ("));
    assert!(rendered.contains("/user/fmt_parent/child_b ("));
    // Children render one level deeper than their parent.
    for line in rendered.lines() {
        if line.contains("child_a") || line.contains("child_b") {
            assert!(line.starts_with("  /user/"), "line: {line}");
        }
    }

    harness.shutdown().await;
    Ok(())
}
