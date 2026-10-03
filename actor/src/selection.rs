//! Named registry lookups and path-pattern actor selection.
//!
//! [`SystemRef::register_name`] binds an operator-chosen name to a live
//! actor path; [`ActorSelection`] resolves names or path patterns lazily on
//! every send, so callers never hold stale [`ActorRef`]s across restarts.
//! Re-register the same name in `pre_start` and selections keep working.

use crate::{Actor, ActorPath, ActorRef, Error, Handler, SystemRef};

/// Matches one path segment against a glob-lite pattern.
///
/// `*` matches any (possibly empty) substring: `"pagos-*"` matches
/// `"pagos-1"` but not `"x-pagos-1"`. A bare `"*"` matches everything,
/// including empty segments.
fn segment_matches(pattern: &str, value: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == value;
    }
    let mut parts = pattern.split('*').peekable();
    let mut rest = value;
    if !pattern.starts_with('*') {
        let first = parts.next().unwrap_or("");
        match rest.strip_prefix(first) {
            Some(remaining) => rest = remaining,
            None => return false,
        }
    }
    let mut middle: Vec<&str> = parts.filter(|part| !part.is_empty()).collect();
    // With an anchored end the final part must reach the end of the value,
    // so it is matched with `ends_with` instead of `find`.
    let last = if pattern.ends_with('*') {
        None
    } else {
        middle.pop()
    };
    for part in &middle {
        match rest.find(part) {
            Some(pos) => rest = &rest[pos + part.len()..],
            None => return false,
        }
    }
    match last {
        None => true,
        Some(suffix) => rest.ends_with(suffix),
    }
}

/// Matches an actor path against a `/`-separated pattern with the same
/// segment count, where each segment follows [`segment_matches`].
pub(crate) fn path_matches(pattern: &str, path: &ActorPath) -> bool {
    let pattern_segments: Vec<&str> =
        pattern.split('/').filter(|s| !s.is_empty()).collect();
    let value_segments = path.segments();
    if pattern_segments.len() != value_segments.len() {
        return false;
    }
    pattern_segments
        .iter()
        .zip(value_segments.iter())
        .all(|(pat, val)| segment_matches(pat, val))
}

/// A lazily-resolved set of actors: either one registry name or all paths
/// matching a pattern.
///
/// Resolution happens on every send through the live registry, so restarts
/// (which re-register names) are transparent to callers.
#[derive(Clone)]
pub struct ActorSelection {
    system: SystemRef,
    target: SelectionTarget,
}

#[derive(Clone, Debug)]
enum SelectionTarget {
    Name(String),
    Pattern(String),
}

impl ActorSelection {
    pub(crate) fn by_name(system: SystemRef, name: String) -> Self {
        Self {
            system,
            target: SelectionTarget::Name(name),
        }
    }

    pub(crate) fn by_pattern(system: SystemRef, pattern: String) -> Self {
        Self {
            system,
            target: SelectionTarget::Pattern(pattern),
        }
    }

    /// Resolves the selection to live typed actor references right now.
    ///
    /// Stale registry entries are pruned; actors of another type are
    /// skipped silently.
    pub async fn resolve<A>(&self) -> Vec<ActorRef<A>>
    where
        A: Actor + Handler<A>,
    {
        let paths = match &self.target {
            SelectionTarget::Name(name) => {
                self.system.resolve_name(name).into_iter().collect()
            }
            SelectionTarget::Pattern(pattern) => {
                self.system.matching_paths(pattern)
            }
        };
        let mut refs = Vec::new();
        for path in paths {
            if let Ok(actor_ref) = self.system.get_actor::<A>(&path).await {
                refs.push(actor_ref);
            }
        }
        refs
    }

    /// Sends `message` to every resolved actor (fire-and-forget).
    ///
    /// Returns [`Error::NotFound`] when nothing matches: unlike a direct
    /// `tell`, silence here would mean the message went nowhere.
    pub async fn tell<A>(&self, message: A::Message) -> Result<(), Error>
    where
        A: Actor + Handler<A>,
    {
        let refs = self.resolve::<A>().await;
        if refs.is_empty() {
            return Err(Error::NotFound {
                path: ActorPath::from("/"),
            });
        }
        for actor_ref in refs {
            actor_ref.tell(message.clone()).await?;
        }
        Ok(())
    }

    /// Sends `message` and waits for a response. Requires exactly one
    /// match: zero matches is [`Error::NotFound`], several is a
    /// [`Error::Functional`] ambiguity error since replies cannot be
    /// attributed.
    pub async fn ask<A>(
        &self,
        message: A::Message,
    ) -> Result<A::Response, Error>
    where
        A: Actor + Handler<A>,
    {
        let mut refs = self.resolve::<A>().await;
        if refs.is_empty() {
            return Err(Error::NotFound {
                path: ActorPath::from("/"),
            });
        }
        if refs.len() > 1 {
            return Err(Error::Functional {
                description: "multiple actors matched selection".to_owned(),
            });
        }
        refs.pop().expect("exactly one match").ask(message).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_matches_exact() {
        assert!(segment_matches("pagos", "pagos"));
        assert!(!segment_matches("pagos", "pagos-1"));
        assert!(!segment_matches("pagos-1", "pagos"));
    }

    #[test]
    fn test_segment_matches_wildcards() {
        assert!(segment_matches("*", "anything"));
        assert!(segment_matches("*", ""));
        assert!(segment_matches("pagos-*", "pagos-1"));
        assert!(!segment_matches("pagos-*", "x-pagos-1"));
        assert!(!segment_matches("pagos-*", "pagos"));
        assert!(segment_matches("*-worker", "a-worker"));
        assert!(!segment_matches("*-worker", "a-worker-x"));
        assert!(segment_matches("*mid*", "amidb"));
        assert!(!segment_matches("*mid*", "adb"));
        assert!(segment_matches("a*b*c", "axbyc"));
        assert!(!segment_matches("a*b*c", "axby"));
    }

    #[test]
    fn test_path_matches_segments() {
        assert!(path_matches(
            "/user/pagos-*",
            &ActorPath::from("/user/pagos-1")
        ));
        assert!(!path_matches(
            "/user/pagos-*",
            &ActorPath::from("/user/other/pagos-1")
        ));
        assert!(!path_matches("/user/*", &ActorPath::from("/user")));
        assert!(path_matches("/*/*", &ActorPath::from("/user/a")));
    }
}
