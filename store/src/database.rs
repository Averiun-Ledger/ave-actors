//! Storage backend traits: [`DbManager`], [`Collection`], and [`State`].

use crate::error::Error;

/// Key-value pair yielded by a [`Collection`] iterator.
pub type CollectionEntry = (String, Vec<u8>);

/// Result item yielded by a [`Collection`] iterator.
pub type CollectionEntryResult = Result<CollectionEntry, Error>;

/// Boxed iterator returned by [`Collection::iter`].
pub type CollectionIter<'a> =
    Box<dyn Iterator<Item = CollectionEntryResult> + 'a>;

/// Factory for creating [`Collection`] and [`State`] storage backends.
///
/// Implement this trait to plug in a custom database (SQLite, RocksDB, etc.).
/// The type parameters `C` and `S` are the concrete collection and state types
/// your backend produces.
pub trait DbManager<C, S>: Sync + Send + Clone
where
    C: Collection + 'static,
    S: State + 'static,
{
    /// Creates a new ordered key-value collection, typically used to store events.
    ///
    /// `name` identifies the table or column family; `prefix` scopes all keys so
    /// multiple actors can share the same physical table without key collisions.
    /// Returns an error if the backend cannot allocate the collection.
    fn create_collection(&self, name: &str, prefix: &str) -> Result<C, Error>;

    /// Creates a single-value state store, typically used to store actor snapshots.
    ///
    /// `name` identifies the table or column family; `prefix` scopes the stored
    /// value within it. Returns an error if the backend cannot allocate the storage.
    fn create_state(&self, name: &str, prefix: &str) -> Result<S, Error>;

    /// Optional cleanup hook called when the database manager should shut down.
    ///
    /// Backends that need to flush WAL buffers or close connections should override
    /// this. The default implementation is a no-op and always returns `Ok(())`.
    fn stop(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/// Single-value storage used to persist actor state snapshots.
///
/// Unlike [`Collection`], a `State` holds only the most recent value.
/// Implementations must be `Send + Sync + 'static` to be used across async tasks.
pub trait State: Sync + Send + 'static {
    /// Returns the name identifier of this state storage unit.
    fn name(&self) -> &str;

    /// Returns the currently stored bytes.
    ///
    /// Returns [`Error::EntryNotFound`] if no value has been stored yet.
    fn get(&self) -> Result<Vec<u8>, Error>;

    /// Stores `data` as the current value, replacing any previous one.
    fn put(&mut self, data: &[u8]) -> Result<(), Error>;

    /// Deletes the current value.
    ///
    /// Returns [`Error::EntryNotFound`] if there is nothing to delete.
    fn del(&mut self) -> Result<(), Error>;

    /// Removes all data from this state store. Succeeds silently if the store is already empty.
    fn purge(&mut self) -> Result<(), Error>;
}

/// Ordered key-value storage used to persist event sequences.
///
/// Keys are typically zero-padded sequence numbers (e.g. `"00000000000000000042"`),
/// which makes the last-entry and range queries efficient. Implementations must
/// be `Send + Sync + 'static`.
pub trait Collection: Sync + Send + 'static {
    /// Returns the name identifier of this collection.
    fn name(&self) -> &str;

    /// Returns the value stored under `key`.
    ///
    /// Returns [`Error::EntryNotFound`] if the key does not exist.
    fn get(&self, key: &str) -> Result<Vec<u8>, Error>;

    /// Associates `data` with `key`, inserting or replacing any previous value.
    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), Error>;

    /// Removes the entry for `key`.
    ///
    /// Returns [`Error::EntryNotFound`] if the key does not exist.
    fn del(&mut self, key: &str) -> Result<(), Error>;

    /// Returns the last key-value pair in insertion/sort order, or `None` if the collection is empty.
    fn last(&self) -> Result<Option<(String, Vec<u8>)>, Error>;

    /// Removes all entries from the collection.
    fn purge(&mut self) -> Result<(), Error>;

    /// Returns an iterator over all key-value pairs.
    ///
    /// Pass `reverse = true` to iterate in descending key order.
    /// Returns an error if the backend cannot acquire the necessary locks to start
    /// iteration; individual items in the iterator may also yield errors.
    fn iter<'a>(&'a self, reverse: bool) -> Result<CollectionIter<'a>, Error>;

    /// Returns at most `quantity.abs()` values, optionally starting after `from`.
    ///
    /// If `from` is `Some(key)`, iteration begins at the entry immediately after `key`.
    /// A positive `quantity` iterates forward; negative iterates in reverse.
    /// Returns [`Error::EntryNotFound`] if `from` is provided but does not exist.
    fn get_by_range(
        &self,
        from: Option<String>,
        quantity: isize,
    ) -> Result<Vec<Vec<u8>>, Error> {
        fn convert<'a>(
            iter: impl Iterator<Item = CollectionEntryResult> + 'a,
        ) -> CollectionIter<'a> {
            Box::new(iter)
        }
        let (mut iter, quantity) = match from {
            Some(key) => {
                // Find the key
                let iter = if quantity >= 0 {
                    self.iter(false)?
                } else {
                    self.iter(true)?
                };
                let mut iter = iter.peekable();
                loop {
                    let Some(next_item) = iter.peek() else {
                        return Err(Error::EntryNotFound { key });
                    };
                    let (current_key, _) = match next_item {
                        Ok((current_key, event)) => (current_key, event),
                        Err(error) => return Err(error.clone()),
                    };
                    if current_key == &key {
                        break;
                    }
                    iter.next();
                }
                iter.next(); // Exclusive From
                (convert(iter), quantity.abs())
            }
            None => {
                if quantity >= 0 {
                    (self.iter(false)?, quantity)
                } else {
                    (self.iter(true)?, quantity.abs())
                }
            }
        };
        let mut result = Vec::new();
        let mut counter = 0;
        while counter < quantity {
            let Some(item) = iter.next() else {
                break;
            };
            let (_, event) = item?;
            result.push(event);
            counter += 1;
        }
        Ok(result)
    }
}

#[macro_export]
macro_rules! test_store_trait {
    ($name:ident: $type:ty: $type2:ty) => {
        #[cfg(test)]
        mod $name {
            use super::*;
            use $crate::error::Error;

            #[test]
            fn test_create_collection() {
                let mut manager = <$type>::default();
                let store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                assert_eq!(Collection::name(&store), "test");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_create_state() {
                let mut manager = <$type>::default();
                let store: $type2 =
                    manager.create_state("test", "test").unwrap();
                assert_eq!(State::name(&store), "test");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_put_get_collection() {
                let mut manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key", b"value").unwrap();
                assert_eq!(Collection::get(&store, "key").unwrap(), b"value");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_put_get_state() {
                let mut manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_state("test", "test").unwrap();
                State::put(&mut store, b"value").unwrap();
                assert_eq!(State::get(&store).unwrap(), b"value");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_del_collection() {
                let mut manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key", b"value").unwrap();
                Collection::del(&mut store, "key").unwrap();
                assert_eq!(
                    Collection::get(&store, "key"),
                    Err(Error::EntryNotFound {
                        key: "test.key".to_owned()
                    })
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_del_state() {
                let mut manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_state("test", "test").unwrap();
                State::put(&mut store, b"value").unwrap();
                State::del(&mut store).unwrap();
                assert_eq!(
                    State::get(&store),
                    Err(Error::EntryNotFound {
                        key: "test".to_owned()
                    })
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_iter() {
                let mut manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                let items: Vec<_> = store
                    .iter(false)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(
                    items,
                    vec![
                        ("key1".to_string(), b"value1".to_vec()),
                        ("key2".to_string(), b"value2".to_vec()),
                        ("key3".to_string(), b"value3".to_vec()),
                    ]
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_iter_reverse() {
                let mut manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                let items: Vec<_> = store
                    .iter(true)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(
                    items,
                    vec![
                        ("key3".to_string(), b"value3".to_vec()),
                        ("key2".to_string(), b"value2".to_vec()),
                        ("key1".to_string(), b"value1".to_vec()),
                    ]
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_last() {
                let mut manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                let last = store.last().unwrap();
                assert_eq!(
                    last,
                    Some(("key3".to_string(), b"value3".to_vec()))
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_get_by_range() {
                let mut manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                let result = store.get_by_range(None, 2).unwrap();
                assert_eq!(
                    result,
                    vec![b"value1".to_vec(), b"value2".to_vec()]
                );
                let result =
                    store.get_by_range(Some("key3".to_string()), -2).unwrap();
                assert_eq!(
                    result,
                    vec![b"value2".to_vec(), b"value1".to_vec()]
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_purge_collection() {
                let mut manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                assert_eq!(
                    Collection::get(&store, "key1"),
                    Ok(b"value1".to_vec())
                );
                assert_eq!(
                    Collection::get(&store, "key2"),
                    Ok(b"value2".to_vec())
                );
                assert_eq!(
                    Collection::get(&store, "key3"),
                    Ok(b"value3".to_vec())
                );
                Collection::purge(&mut store).unwrap();
                assert_eq!(
                    Collection::get(&store, "key1"),
                    Err(Error::EntryNotFound {
                        key: "test.key1".to_owned()
                    })
                );
                assert_eq!(
                    Collection::get(&store, "key2"),
                    Err(Error::EntryNotFound {
                        key: "test.key2".to_owned()
                    })
                );
                assert_eq!(
                    Collection::get(&store, "key3"),
                    Err(Error::EntryNotFound {
                        key: "test.key3".to_owned()
                    })
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_purge_state() {
                let mut manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_state("test", "test").unwrap();
                State::put(&mut store, b"value1").unwrap();
                assert_eq!(State::get(&store), Ok(b"value1".to_vec()));
                State::purge(&mut store).unwrap();
                assert_eq!(
                    State::get(&store),
                    Err(Error::EntryNotFound {
                        key: "test".to_owned()
                    })
                );

                State::put(&mut store, b"value2").unwrap();
                assert_eq!(State::get(&store), Ok(b"value2".to_vec()));
                State::purge(&mut store).unwrap();
                assert_eq!(
                    State::get(&store),
                    Err(Error::EntryNotFound {
                        key: "test".to_owned()
                    })
                );
                assert!(manager.stop().is_ok())
            }
        }
    };
}
