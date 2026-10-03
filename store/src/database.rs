//! Storage backend traits: [`DbManager`], [`Collection`], and [`State`].

use crate::error::Error;

/// Key-value pair yielded by a [`Collection`] iterator.
pub type CollectionEntry = (String, Vec<u8>);

/// Result item yielded by a [`Collection`] iterator.
pub type CollectionEntryResult = Result<CollectionEntry, Error>;

/// Boxed iterator returned by [`Collection::iter`].
pub type CollectionIter<'a> =
    Box<dyn Iterator<Item = CollectionEntryResult> + 'a>;

/// Single write operation inside an atomic [`DbManager::write_batch`] call.
///
/// `collection` / `store` are backend collection/state names (e.g.
/// `"store_events"`), `prefix` scoping is supplied separately to
/// `write_batch`. Backends must apply the same key mapping as their
/// [`Collection`] / [`State`] implementations.
#[derive(Debug, Clone, Copy)]
pub enum BatchOp<'a> {
    /// Insert or replace an event under `key` in `collection`.
    PutEvent {
        /// Backend collection name.
        collection: &'a str,
        /// Event key (e.g. zero-padded sequence number).
        key: &'a str,
        /// Encoded event bytes.
        data: &'a [u8],
    },
    /// Replace the single value held by state store `store`.
    PutState {
        /// Backend state name.
        store: &'a str,
        /// Encoded snapshot/metadata bytes.
        data: &'a [u8],
    },
}

/// Factory for creating [`Collection`] and [`State`] storage backends.
///
/// Implement this trait to plug in a custom database (SQLite, RocksDB, etc.).
/// The type parameters `C` and `S` are the concrete collection and state types
/// your backend produces.
pub trait DbManager<C, S>: Sync + Send
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
    fn stop(self) -> Result<(), Error>
    where
        Self: Sized,
    {
        Ok(())
    }

    /// Returns an atomic multi-write handle for this backend, if supported.
    ///
    /// A returned handle guarantees all-or-nothing application of
    /// [`BatchWrite::write_batch`], including across crashes. The default is
    /// `None`; callers then apply multi-write operations sequentially and
    /// compensate on error exactly as with individual writes.
    fn batch_writer(&self) -> Option<Box<dyn BatchWrite>> {
        None
    }
}

/// Atomic multi-write handle for one backend.
///
/// Returned by [`DbManager::batch_writer`]. A `Some` handle is a contract:
/// [`BatchWrite::write_batch`] either applies every op durably or applies
/// none (rolling back on failure), so callers must not compensate after an
/// error beyond reporting it.
pub trait BatchWrite: Sync + Send {
    /// Applies `ops` scoped by `prefix` atomically (all-or-nothing).
    ///
    /// Key mapping must match the backend's [`Collection`] / [`State`]
    /// implementations. Implementations must validate any interpolated
    /// identifiers exactly as their handle constructors do.
    fn write_batch(
        &self,
        prefix: &str,
        ops: &[BatchOp<'_>],
    ) -> Result<(), Error>;
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
    /// Backends implement this as check-then-delete without a transaction
    /// unless documented otherwise: under concurrent writers a delete may
    /// remove a concurrently written value or report success for an
    /// already-deleted one. The [`Store`](crate::store::Store) only deletes
    /// during compensation and purge, never concurrently.
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
    /// Returns [`Error::EntryNotFound`] if the key does not exist. Same
    /// check-then-delete caveat as [`State::del`](State::del) under
    /// concurrent writers.
    fn del(&mut self, key: &str) -> Result<(), Error>;

    /// Returns the last key-value pair in insertion/sort order, or `None` if the collection is empty.
    fn last(&self) -> Result<Option<(String, Vec<u8>)>, Error>;

    /// Removes all entries from the collection.
    fn purge(&mut self) -> Result<(), Error>;

    /// Removes all entries whose keys fall within the inclusive range
    /// `[start, end]`.
    ///
    /// The default implementation iterates over the range (via
    /// [`iter_range`](Collection::iter_range)) and deletes each entry
    /// individually. Backends that support native range deletes should
    /// override it for better performance.
    fn del_range(&mut self, start: &str, end: &str) -> Result<(), Error> {
        let keys: Vec<String> = self
            .iter_range(start, end, false)?
            .map(|item| item.map(|(k, _)| k))
            .collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            match self.del(&key) {
                Ok(()) | Err(Error::EntryNotFound { .. }) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Returns an iterator over all key-value pairs.
    ///
    /// Pass `reverse = true` to iterate in descending key order.
    /// Returns an error if the backend cannot acquire the necessary locks to start
    /// iteration; individual items in the iterator may also yield errors.
    fn iter<'a>(&'a self, reverse: bool) -> Result<CollectionIter<'a>, Error>;

    /// Returns an iterator over key-value pairs within the inclusive range
    /// `[start, end]`.
    ///
    /// Pass `reverse = true` to iterate in descending key order.
    /// The default implementation delegates to [`iter`](Collection::iter) and
    /// filters in-memory, so backends that support native range queries should
    /// override it for better performance.
    fn iter_range<'a>(
        &'a self,
        start: &str,
        end: &str,
        reverse: bool,
    ) -> Result<CollectionIter<'a>, Error> {
        let start = start.to_owned();
        let end = end.to_owned();
        let iter = self.iter(reverse)?;
        Ok(Box::new(iter.filter(move |item| match item {
            Ok((key, _)) => key >= &start && key <= &end,
            Err(_) => true,
        })))
    }

    /// Returns at most `quantity` values in magnitude, optionally starting after `from`.
    ///
    /// If `from` is `Some(key)`, iteration begins at the entry immediately after `key`.
    /// A positive `quantity` iterates forward; negative iterates in reverse.
    /// Returns [`Error::EntryNotFound`] if `from` is provided but does not exist.
    fn get_by_range(
        &self,
        from: Option<&str>,
        quantity: isize,
    ) -> Result<Vec<Vec<u8>>, Error> {
        // `unsigned_abs` (not `abs`): `isize::MIN.abs()` panics in debug
        // and wraps in release.
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
                        return Err(Error::EntryNotFound {
                            key: key.to_string(),
                        });
                    };
                    let (current_key, _) = match next_item {
                        Ok((current_key, event)) => (current_key, event),
                        Err(error) => return Err(error.clone()),
                    };
                    if current_key == key {
                        break;
                    }
                    iter.next();
                }
                iter.next(); // Exclusive From
                (
                    Box::new(iter) as CollectionIter<'_>,
                    quantity.unsigned_abs(),
                )
            }
            None => {
                if quantity >= 0 {
                    (self.iter(false)?, quantity as usize)
                } else {
                    (self.iter(true)?, quantity.unsigned_abs())
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
                let manager = <$type>::default();
                let store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                assert_eq!(Collection::name(&store), "test");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_create_state() {
                let manager = <$type>::default();
                let store: $type2 =
                    manager.create_state("test", "test").unwrap();
                assert_eq!(State::name(&store), "test");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_put_get_collection() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key", b"value").unwrap();
                assert_eq!(Collection::get(&store, "key").unwrap(), b"value");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_put_get_state() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_state("test", "test").unwrap();
                State::put(&mut store, b"value").unwrap();
                assert_eq!(State::get(&store).unwrap(), b"value");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_del_collection() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key", b"value").unwrap();
                Collection::del(&mut store, "key").unwrap();
                assert!(matches!(
                    Collection::get(&store, "key"),
                    Err(Error::EntryNotFound { key }) if key == "test.key"
                ));
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_del_state() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_state("test", "test").unwrap();
                State::put(&mut store, b"value").unwrap();
                State::del(&mut store).unwrap();
                assert!(matches!(
                    State::get(&store),
                    Err(Error::EntryNotFound { key }) if key == "test"
                ));
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_iter() {
                let manager = <$type>::default();
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
                let manager = <$type>::default();
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
            fn test_iter_range() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                let items: Vec<_> = store
                    .iter_range("key2", "key3", false)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(
                    items,
                    vec![
                        ("key2".to_string(), b"value2".to_vec()),
                        ("key3".to_string(), b"value3".to_vec()),
                    ]
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_iter_range_reverse() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                let items: Vec<_> = store
                    .iter_range("key1", "key2", true)
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                assert_eq!(
                    items,
                    vec![
                        ("key2".to_string(), b"value2".to_vec()),
                        ("key1".to_string(), b"value1".to_vec()),
                    ]
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_last() {
                let manager = <$type>::default();
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
                let manager = <$type>::default();
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
                let result = store.get_by_range(Some("key3"), -2).unwrap();
                assert_eq!(
                    result,
                    vec![b"value2".to_vec(), b"value1".to_vec()]
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_del_range() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                Collection::del_range(&mut store, "key1", "key2").unwrap();
                assert!(matches!(
                    Collection::get(&store, "key1"),
                    Err(Error::EntryNotFound { key }) if key == "test.key1"
                ));
                assert!(matches!(
                    Collection::get(&store, "key2"),
                    Err(Error::EntryNotFound { key }) if key == "test.key2"
                ));
                assert_eq!(Collection::get(&store, "key3").unwrap(), b"value3".to_vec());
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_purge_collection() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                assert_eq!(Collection::get(&store, "key1").unwrap(), b"value1".to_vec());
                assert_eq!(Collection::get(&store, "key2").unwrap(), b"value2".to_vec());
                assert_eq!(Collection::get(&store, "key3").unwrap(), b"value3".to_vec());
                Collection::purge(&mut store).unwrap();
                assert!(matches!(
                    Collection::get(&store, "key1"),
                    Err(Error::EntryNotFound { key }) if key == "test.key1"
                ));
                assert!(matches!(
                    Collection::get(&store, "key2"),
                    Err(Error::EntryNotFound { key }) if key == "test.key2"
                ));
                assert!(matches!(
                    Collection::get(&store, "key3"),
                    Err(Error::EntryNotFound { key }) if key == "test.key3"
                ));
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_purge_state() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_state("test", "test").unwrap();
                State::put(&mut store, b"value1").unwrap();
                assert_eq!(State::get(&store).unwrap(), b"value1".to_vec());
                State::purge(&mut store).unwrap();
                assert!(matches!(
                    State::get(&store),
                    Err(Error::EntryNotFound { key }) if key == "test"
                ));

                State::put(&mut store, b"value2").unwrap();
                assert_eq!(State::get(&store).unwrap(), b"value2".to_vec());
                State::purge(&mut store).unwrap();
                assert!(matches!(
                    State::get(&store),
                    Err(Error::EntryNotFound { key }) if key == "test"
                ));
                assert!(manager.stop().is_ok())
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    /// Mock collection where `del` returns `EntryNotFound` for key "b".
    struct DelRangeMock {
        data: Vec<(String, Vec<u8>)>,
    }

    impl Collection for DelRangeMock {
        fn name(&self) -> &str {
            "mock"
        }
        fn get(&self, _key: &str) -> Result<Vec<u8>, Error> {
            unimplemented!()
        }
        fn put(&mut self, _key: &str, _data: &[u8]) -> Result<(), Error> {
            unimplemented!()
        }
        fn del(&mut self, key: &str) -> Result<(), Error> {
            if key == "b" {
                Err(Error::EntryNotFound {
                    key: key.to_string(),
                })
            } else {
                Ok(())
            }
        }
        fn last(&self) -> Result<Option<(String, Vec<u8>)>, Error> {
            unimplemented!()
        }
        fn purge(&mut self) -> Result<(), Error> {
            unimplemented!()
        }
        fn iter(&self, _reverse: bool) -> Result<CollectionIter<'_>, Error> {
            unimplemented!()
        }
        fn iter_range(
            &self,
            _start: &str,
            _end: &str,
            _reverse: bool,
        ) -> Result<CollectionIter<'_>, Error> {
            Ok(Box::new(self.data.clone().into_iter().map(Ok)))
        }
    }

    #[test]
    fn test_del_range_skips_not_found() {
        let mut mock = DelRangeMock {
            data: vec![
                ("a".to_string(), vec![]),
                ("b".to_string(), vec![]),
                ("c".to_string(), vec![]),
            ],
        };
        assert!(mock.del_range("a", "c").is_ok());
    }

    /// Mock collection for testing `get_by_range` edge cases.
    struct GetByRangeMock {
        items: Vec<(String, Vec<u8>)>,
    }

    impl Collection for GetByRangeMock {
        fn name(&self) -> &str {
            "mock"
        }
        fn get(&self, _key: &str) -> Result<Vec<u8>, Error> {
            unimplemented!()
        }
        fn put(&mut self, _key: &str, _data: &[u8]) -> Result<(), Error> {
            unimplemented!()
        }
        fn del(&mut self, _key: &str) -> Result<(), Error> {
            unimplemented!()
        }
        fn last(&self) -> Result<Option<(String, Vec<u8>)>, Error> {
            unimplemented!()
        }
        fn purge(&mut self) -> Result<(), Error> {
            unimplemented!()
        }
        fn iter(&self, reverse: bool) -> Result<CollectionIter<'_>, Error> {
            let items = if reverse {
                self.items.clone().into_iter().rev().collect::<Vec<_>>()
            } else {
                self.items.clone()
            };
            Ok(Box::new(items.into_iter().map(Ok)))
        }
        fn iter_range(
            &self,
            _start: &str,
            _end: &str,
            _reverse: bool,
        ) -> Result<CollectionIter<'_>, Error> {
            unimplemented!()
        }
    }

    #[test]
    fn test_get_by_range_none_negative_quantity() {
        let mock = GetByRangeMock {
            items: vec![
                ("a".to_string(), b"1".to_vec()),
                ("b".to_string(), b"2".to_vec()),
            ],
        };
        let result = mock.get_by_range(None, -2).unwrap();
        assert_eq!(result, vec![b"2".to_vec(), b"1".to_vec()]);
    }

    #[test]
    fn test_get_by_range_from_not_found() {
        let mock = GetByRangeMock {
            items: vec![("a".to_string(), b"1".to_vec())],
        };
        assert!(matches!(
            mock.get_by_range(Some("z"), 1),
            Err(Error::EntryNotFound { key }) if key == "z"
        ));
    }

    #[test]
    fn test_get_by_range_from_found() {
        let mock = GetByRangeMock {
            items: vec![
                ("a".to_string(), b"1".to_vec()),
                ("b".to_string(), b"2".to_vec()),
                ("c".to_string(), b"3".to_vec()),
            ],
        };
        // get_by_range uses an exclusive from, so starting at "b" skips it.
        let result = mock.get_by_range(Some("b"), 2).unwrap();
        assert_eq!(result, vec![b"3".to_vec()]);
    }
}
