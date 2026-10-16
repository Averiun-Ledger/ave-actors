//! Printable supervision-tree snapshot for debugging.
//!
//! [`ActorTreeNode`] is a point-in-time view of the live actor
//! hierarchy: each node carries its path, the concrete actor type
//! recorded at spawn, and its children. Use
//! [`SystemRef::actor_tree`](crate::SystemRef::actor_tree) to take the
//! snapshot and [`format_tree`] to render it for logs or consoles.

use serde::Serialize;

/// One live actor and its children at snapshot time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ActorTreeNode {
    /// Full path (`/user/node/subject-42`).
    pub path: String,
    /// Concrete type recorded at spawn (`my_crate::NodeActor`).
    /// `"unknown"` when the type entry is missing.
    pub actor_type: String,
    /// Direct children, sorted by path.
    pub children: Vec<Self>,
}

impl ActorTreeNode {
    /// Total nodes in this subtree, including `self`.
    pub fn size(&self) -> usize {
        1 + self.children.iter().map(Self::size).sum::<usize>()
    }
}

/// Rendered tree, one `path (type)` line per node indented by depth.
pub fn format_tree(roots: &[ActorTreeNode]) -> String {
    let mut out = String::new();
    for root in roots {
        render_node(root, 0, &mut out);
    }
    out
}

fn render_node(node: &ActorTreeNode, depth: usize, out: &mut String) {
    out.push_str(&"  ".repeat(depth));
    out.push_str(&node.path);
    out.push_str(" (");
    out.push_str(&node.actor_type);
    out.push_str(")\n");
    for child in &node.children {
        render_node(child, depth + 1, out);
    }
}
