//! Hierarchical path addressing for actors (e.g. `/user/parent/child`).

use crate::Error;
use serde::{Deserialize, Serialize};

use std::cmp::Ordering;
use std::fmt::{Error as FmtError, Formatter};
use std::sync::Arc;

/// Maximum depth allowed for an actor path (number of segments).
const MAX_PATH_DEPTH: usize = 255;

/// Maximum length of a single path segment.
const MAX_SEGMENT_LENGTH: usize = 256;

/// Returns `true` if `c` is allowed inside an `ActorPath` segment.
fn is_valid_segment_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// A slash-separated path that uniquely identifies an actor in the system (e.g. `/user/orders/order-42`).
///
/// Paths are built by appending segments with the `/` operator. The first
/// segment is conventionally the root scope (`user`, `system`, etc.).
#[derive(Clone, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub struct ActorPath(Arc<[String]>);

impl Serialize for ActorPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_ref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ActorPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let vec = Vec::<String>::deserialize(deserializer)?;
        Ok(Self(Arc::from(vec)))
    }
}

impl ActorPath {
    /// Returns a path containing only the first segment (the root scope).
    pub fn root(&self) -> Self {
        self.0.first().map_or_else(
            || Self(Arc::from([])),
            |first| Self(Arc::from([first.clone()])),
        )
    }

    /// Returns this path with its last segment removed, or an empty path if already at root.
    pub fn parent(&self) -> Self {
        if self.0.len() > 1 {
            Self(Arc::from(&self.0[..self.0.len() - 1]))
        } else {
            Self(Arc::from([]))
        }
    }

    /// Returns the last segment of the path (the actor's local id).
    pub fn key(&self) -> &str {
        self.0.last().map(|s| s.as_str()).unwrap_or("")
    }

    /// Returns the number of segments in this path (its depth).
    pub fn level(&self) -> usize {
        self.0.len()
    }

    /// Returns this path truncated to `level` segments, or `self` unchanged if `level` is out of range.
    pub fn at_level(&self, level: usize) -> Self {
        if level < 1 || level >= self.level() {
            self.clone()
        } else if self.is_top_level() {
            self.root()
        } else if level == self.level() - 1 {
            self.parent()
        } else {
            Self(Arc::from(&self.0[..level]))
        }
    }

    /// Returns `true` if this path has no segments (e.g. parsed from `"/"`).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns `true` if this path is a proper prefix of `other`, meaning this actor is an ancestor of `other`.
    pub fn is_ancestor_of(&self, other: &Self) -> bool {
        self.0.len() < other.0.len() && other.0.starts_with(&self.0)
    }

    /// Returns `true` if `other` is a proper prefix of this path, meaning this actor is a descendant of `other`.
    pub fn is_descendant_of(&self, other: &Self) -> bool {
        other.0.len() < self.0.len() && self.0.starts_with(&other.0)
    }

    /// Returns `true` if this path is the direct parent of `other` (one level above it).
    pub fn is_parent_of(&self, other: &Self) -> bool {
        *self == other.parent()
    }

    /// Returns `true` if `other` is the direct parent of this path (this path is one level below `other`).
    pub fn is_child_of(&self, other: &Self) -> bool {
        self.parent() == *other
    }

    /// Returns `true` if this path has exactly one segment, i.e. it is a direct child of the root scope.
    pub fn is_top_level(&self) -> bool {
        self.0.len() == 1
    }

    /// Validates that `segment` is a legal `ActorPath` component.
    ///
    /// Allowed characters are ASCII alphanumeric, `_` and `-`. The segment
    /// must be non-empty and no longer than [`MAX_SEGMENT_LENGTH`].
    pub fn validate_segment(segment: &str) -> Result<(), Error> {
        if segment.is_empty() {
            return Err(Error::InvalidConfiguration {
                component: "ActorPath".to_owned(),
                reason: "path segment cannot be empty".to_owned(),
            });
        }
        if segment.len() > MAX_SEGMENT_LENGTH {
            return Err(Error::InvalidConfiguration {
                component: "ActorPath".to_owned(),
                reason: format!(
                    "path segment length {} exceeds maximum {}",
                    segment.len(),
                    MAX_SEGMENT_LENGTH
                ),
            });
        }
        if let Some(c) = segment.chars().find(|&c| !is_valid_segment_char(c)) {
            return Err(Error::InvalidConfiguration {
                component: "ActorPath".to_owned(),
                reason: format!(
                    "path segment '{}' contains invalid character '{}'",
                    segment, c
                ),
            });
        }
        Ok(())
    }

    /// Validates the full path: each segment must be legal and the depth must
    /// not exceed [`MAX_PATH_DEPTH`].
    pub fn validate(&self) -> Result<(), Error> {
        if self.0.len() > MAX_PATH_DEPTH {
            return Err(Error::InvalidConfiguration {
                component: "ActorPath".to_owned(),
                reason: format!(
                    "path depth {} exceeds maximum {}",
                    self.0.len(),
                    MAX_PATH_DEPTH
                ),
            });
        }
        for segment in self.0.iter() {
            Self::validate_segment(segment)?;
        }
        Ok(())
    }

    /// Creates a path from a `/`-separated string, validating every segment and
    /// the overall depth.
    ///
    /// Prefer this over [`ActorPath::from`] when the input comes from an
    /// untrusted source.
    pub fn try_from_str(str: &str) -> Result<Self, Error> {
        let path = Self::from(str);
        path.validate()?;
        Ok(path)
    }
}

/// Creates a path from a `/`-separated string such as `"/user/parent/child"`.
impl From<&str> for ActorPath {
    fn from(str: &str) -> Self {
        let tokens: Vec<String> = str
            .split('/')
            .map(|s| s.trim())
            .filter(|x| !x.is_empty())
            .map(|s| s.to_string())
            .collect();
        Self(Arc::from(tokens))
    }
}

/// Creates a path from a string. Equivalent to [`ActorPath::from`] with a `&str`.
impl From<String> for ActorPath {
    fn from(string: String) -> Self {
        Self::from(string.as_str())
    }
}

impl From<&String> for ActorPath {
    fn from(string: &String) -> Self {
        Self::from(string.as_str())
    }
}

/// Appends `segment` as a new path component: `parent_path / "child-id"`.
impl std::ops::Div<&str> for ActorPath {
    type Output = Self;

    fn div(self, rhs: &str) -> Self::Output {
        let mut keys = self.0.to_vec();
        let mut tokens: Vec<String> = rhs
            .split('/')
            .map(|s| s.trim())
            .filter(|x| !x.is_empty())
            .map(|s| s.to_string())
            .collect();

        keys.append(&mut tokens);
        Self(Arc::from(keys))
    }
}

impl std::fmt::Display for ActorPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self.level().cmp(&1) {
            Ordering::Less => write!(f, "/"),
            Ordering::Equal => write!(f, "/{}", self.0[0]),
            Ordering::Greater => write!(f, "/{}", self.0.join("/")),
        }
    }
}

impl std::fmt::Debug for ActorPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        match self.level().cmp(&1) {
            Ordering::Less => write!(f, "/"),
            Ordering::Equal => write!(f, "/{}", self.0[0]),
            Ordering::Greater => write!(f, "/{}", self.0.join("/")),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parse_empty_string() {
        let path = ActorPath::from("");
        assert_eq!(path.0.as_ref(), &Vec::<String>::new());
    }

    #[test]
    fn parse_single_root() {
        let path = ActorPath::from("/acme");
        println!("{:?}", path);
        assert_eq!(path.0.as_ref(), &vec!["acme"]);
    }

    #[test]
    fn parse_two_deep() {
        let path = ActorPath::from("/acme/building");
        println!("{:?}", path);
        assert_eq!(path.0.as_ref(), &vec!["acme", "building"]);
    }

    #[test]
    fn parse_three_deep() {
        let path = ActorPath::from("/acme/building/room");
        println!("{:?}", path);
        assert_eq!(path.0.as_ref(), &vec!["acme", "building", "room"]);
    }

    #[test]
    fn parse_levels() {
        let path = ActorPath::from("/acme/building/room/sensor");
        println!("{:?}", path);
        assert_eq!(path.level(), 4);
    }

    #[test]
    fn test_get_key() {
        let path = ActorPath::from("/acme/building/room/sensor");
        println!("{:?}", path);
        assert_eq!(path.key(), "sensor");
    }

    #[test]
    fn parse_get_parent() {
        let path = ActorPath::from("/acme/building/room/sensor").parent();
        println!("{:?}", path);
        assert_eq!(path.parent().0.as_ref(), &vec!["acme", "building"]);
    }

    #[test]
    fn parse_to_string() {
        let path = ActorPath::from("/acme/building/room/sensor");
        let string = path.to_string();
        println!("{:?}", string);
        assert_eq!(string, "/acme/building/room/sensor");
    }

    #[test]
    fn parse_root_at_root() {
        let path = ActorPath::from("/acme");
        let string = path.root().to_string();
        println!("{:?}", string);
        assert_eq!(string, "/acme");
    }

    #[test]
    fn parse_parent_at_root() {
        let path = ActorPath::from("/acme");
        let string = path.parent().to_string();
        println!("{:?}", string);
        assert_eq!(string, "/");
    }

    #[test]
    fn parse_root_to_string() {
        let path = ActorPath::from("/acme/building/room/sensor");
        let string = path.root().to_string();
        println!("{:?}", string);
        assert_eq!(string, "/acme");
    }

    #[test]
    fn test_if_empty() {
        let path = ActorPath::from("/");
        assert!(path.is_empty());
        let not_empty = ActorPath::from("/not_empty");
        assert!(!not_empty.is_empty());
    }

    #[test]
    fn test_if_parent_child() {
        let path = ActorPath::from("/acme/building/room/sensor");
        let parent = path.parent();
        assert!(parent.is_parent_of(&path));
        assert!(path.is_child_of(&parent));
    }

    #[test]
    fn test_if_descendant() {
        let path = ActorPath::from("/acme/building/room/sensor");
        let parent = path.parent();
        assert!(path.is_descendant_of(&parent));
        assert!(!path.is_descendant_of(&path));
    }

    #[test]
    fn test_if_ancestor() {
        let path = ActorPath::from("/acme/building/room/sensor");
        let parent = path.parent();
        assert!(parent.is_ancestor_of(&path));
        assert!(!path.is_ancestor_of(&path));
    }

    #[test]
    fn test_if_ancestor_descendant() {
        let path = ActorPath::from("/acme/building/room/sensor");
        let root = path.root();
        assert!(root.is_ancestor_of(&path));
        assert!(path.is_descendant_of(&root));
    }

    #[test]
    fn test_root_slash_relationships() {
        let root = ActorPath::from("/");
        let child = ActorPath::from("/acme");
        let grandchild = ActorPath::from("/acme/building");

        assert!(root.is_ancestor_of(&child));
        assert!(root.is_ancestor_of(&grandchild));
        assert!(child.is_descendant_of(&root));
        assert!(grandchild.is_descendant_of(&root));
        assert!(!root.is_ancestor_of(&root));
        assert!(!root.is_descendant_of(&root));
    }

    #[test]
    fn test_if_root() {
        let path = ActorPath::from("/acme/building/room/sensor");
        let root = path.root();
        println!("{:?}", path);
        println!("{:?}", root);
        assert!(root.is_top_level());
        assert!(!path.is_top_level());
    }

    #[test]
    fn test_at_level() {
        let path = ActorPath::from("/acme/building/room/sensor");
        assert_eq!(path.at_level(0), path);
        assert_eq!(path.at_level(1), path.root());
        assert_eq!(path.at_level(2), ActorPath::from("/acme/building"));
        assert_eq!(path.at_level(3), path.parent());
        assert_eq!(path.at_level(4), path);
        assert_eq!(path.at_level(5), path);
    }

    #[test]
    fn test_add_path() {
        let path = ActorPath::from("/acme");
        let child = path.clone() / "child";
        println!("{}", &child);
        assert!(path.is_parent_of(&child))
    }

    #[test]
    fn test_div_with_slashes() {
        let path = ActorPath::from("/acme");
        let child = path / "building/room";
        assert_eq!(child.level(), 3);
        assert_eq!(child.key(), "room");
    }

    #[test]
    fn test_serde_roundtrip() {
        let path = ActorPath::from("/acme/building/room");
        let json = serde_json::to_string(&path).unwrap();
        let decoded: ActorPath = serde_json::from_str(&json).unwrap();
        assert_eq!(path, decoded);
    }

    #[test]
    fn test_validate_segment_accepts_alphanumeric_dash_underscore() {
        assert!(ActorPath::validate_segment("valid").is_ok());
        assert!(ActorPath::validate_segment("valid-2").is_ok());
        assert!(ActorPath::validate_segment("valid_2").is_ok());
    }

    #[test]
    fn test_validate_segment_rejects_empty() {
        assert!(ActorPath::validate_segment("").is_err());
    }

    #[test]
    fn test_validate_segment_rejects_invalid_characters() {
        assert!(ActorPath::validate_segment("with space").is_err());
        assert!(ActorPath::validate_segment("with/slash").is_err());
        assert!(ActorPath::validate_segment("with.dot").is_err());
        assert!(ActorPath::validate_segment("with@symbol").is_err());
    }

    #[test]
    fn test_validate_rejects_excessive_depth() {
        let segments: Vec<String> =
            (0..=255).map(|i| format!("seg{i}")).collect();
        let path = ActorPath::from(segments.join("/").as_str());
        assert!(path.validate().is_err());
    }

    #[test]
    fn test_validate_accepts_max_depth() {
        let segments: Vec<String> =
            (0..255).map(|i| format!("seg{i}")).collect();
        let path = ActorPath::from(segments.join("/").as_str());
        assert!(path.validate().is_ok());
    }

    #[test]
    fn test_try_from_str_validates() {
        assert!(ActorPath::try_from_str("/a/b").is_ok());
        assert!(ActorPath::try_from_str("/a/b c").is_err());
    }
}
