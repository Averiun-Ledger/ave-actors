//! In-memory [`DbManager`] backend, intended for tests and ephemeral usage.

use crate::{
    database::{BatchOp, BatchWrite, Collection, DbManager, State},
    error::{Error, StoreOperation},
};

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};

type MemoryData = Arc<
    RwLock<HashMap<(String, String), Arc<RwLock<BTreeMap<String, Vec<u8>>>>>>,
>;

/// In-memory database manager backed by a shared `Arc<RwLock<...>>`.
///
/// All collections and state stores created by the same `MemoryManager` instance
/// share the underlying `HashMap`, so data survives across handle clones just
/// as it would with a real database.
#[derive(Default, Clone)]
pub struct MemoryManager {
    data: MemoryData,
}

impl MemoryManager {
    fn get_or_create_store(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<MemoryStore, Error> {
        let mut data_lock = self.data.write().map_err(|e| Error::Store {
            source: None,
            code: None,
            operation: StoreOperation::LockManagerData,
            reason: e.to_string(),
        })?;
        let data = data_lock
            .entry((name.to_owned(), prefix.to_owned()))
            .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())))
            .clone();
        drop(data_lock);

        Ok(MemoryStore {
            name: name.to_owned(),
            prefix: prefix.to_owned(),
            data,
        })
    }
}

impl DbManager<MemoryStore, MemoryStore> for MemoryManager {
    fn create_state(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<MemoryStore, Error> {
        self.get_or_create_store(name, prefix)
    }

    fn stop(self) -> Result<(), Error> {
        Ok(())
    }

    fn create_collection(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<MemoryStore, Error> {
        self.get_or_create_store(name, prefix)
    }

    fn batch_writer(&self) -> Option<Box<dyn BatchWrite>> {
        Some(Box::new(MemoryBatchWriter {
            manager: self.clone(),
        }))
    }
}

/// Atomic multi-write handle over [`MemoryManager`]'s shared map.
///
/// All ops apply while holding every affected inner store's write lock
/// (acquired in deterministic order), so concurrent handle writers block
/// until the whole batch is applied: all-or-nothing.
#[derive(Clone)]
struct MemoryBatchWriter {
    manager: MemoryManager,
}

impl BatchWrite for MemoryBatchWriter {
    fn write_batch(
        &self,
        prefix: &str,
        ops: &[BatchOp<'_>],
    ) -> Result<(), Error> {
        // Unique (name, prefix) stores touched, in deterministic lock order.
        let mut stores: Vec<(String, String)> = ops
            .iter()
            .map(|op| match op {
                BatchOp::PutEvent { collection, .. } => {
                    ((*collection).to_owned(), prefix.to_owned())
                }
                BatchOp::PutState { store, .. } => {
                    ((*store).to_owned(), prefix.to_owned())
                }
            })
            .collect();
        stores.sort();
        stores.dedup();

        // Owned handles keep the inner maps alive while locked; lock in
        // deterministic order so concurrent batches cannot deadlock.
        let mut owned = Vec::with_capacity(stores.len());
        for (name, store_prefix) in &stores {
            owned.push(self.manager.get_or_create_store(name, store_prefix)?);
        }
        let mut guards = Vec::with_capacity(owned.len());
        for store in &owned {
            guards.push(store.data.write().map_err(|e| Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::LockData,
                reason: e.to_string(),
            })?);
        }

        // Apply with all locks held; no persistent failure mode remains.
        // Index by store name once instead of scanning per op. The map is
        // built from the same ops, so a miss is an internal invariant
        // break: report it instead of panicking.
        let index: std::collections::HashMap<&str, usize> = stores
            .iter()
            .enumerate()
            .map(|(pos, (name, _))| (name.as_str(), pos))
            .collect();
        for op in ops {
            match op {
                BatchOp::PutEvent {
                    collection,
                    key,
                    data,
                } => {
                    let pos =
                        index.get(collection).copied().ok_or_else(|| {
                            Error::Store {
                                source: None,
                                code: None,
                                operation: StoreOperation::LockData,
                                reason:
                                    "batch store missing for collection op \
                                     (internal invariant broken)"
                                        .to_owned(),
                            }
                        })?;
                    guards[pos]
                        .insert(format!("{prefix}.{key}"), (*data).to_vec());
                }
                BatchOp::PutState { store, data } => {
                    let pos = index.get(store).copied().ok_or_else(|| {
                        Error::Store {
                            source: None,
                            code: None,
                            operation: StoreOperation::LockData,
                            reason: "batch store missing for state op \
                                     (internal invariant broken)"
                                .to_owned(),
                        }
                    })?;
                    guards[pos].insert(prefix.to_owned(), (*data).to_vec());
                }
            }
        }
        Ok(())
    }
}

/// In-memory implementation of both [`Collection`] and [`State`].
///
/// Data is stored in a `BTreeMap` behind an `Arc<RwLock<...>>` so it can be
/// shared across clones. All keys in collection mode are prefixed with
/// `"<prefix>."` to avoid collisions with state-mode entries.
#[derive(Default, Clone)]
pub struct MemoryStore {
    name: String,
    prefix: String,
    data: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
}

impl MemoryStore {
    fn collection_prefix(&self) -> String {
        format!("{}.", self.prefix)
    }
}

impl State for MemoryStore {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self) -> Result<Vec<u8>, Error> {
        let lock = self.data.read().map_err(|e| Error::Store {
            source: None,
            code: None,
            operation: StoreOperation::LockData,
            reason: e.to_string(),
        })?;

        lock.get(&self.prefix).map_or_else(
            || {
                Err(Error::EntryNotFound {
                    key: self.prefix.clone(),
                })
            },
            |value| Ok(value.clone()),
        )
    }

    fn put(&mut self, data: &[u8]) -> Result<(), Error> {
        self.data
            .write()
            .map_err(|e| Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::LockData,
                reason: e.to_string(),
            })?
            .insert(self.prefix.clone(), data.to_vec());

        Ok(())
    }

    fn del(&mut self) -> Result<(), Error> {
        let mut lock = self.data.write().map_err(|e| Error::Store {
            source: None,
            code: None,
            operation: StoreOperation::LockData,
            reason: e.to_string(),
        })?;
        match lock.remove(&self.prefix) {
            Some(_) => Ok(()),
            None => Err(Error::EntryNotFound {
                key: self.prefix.clone(),
            }),
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        self.data
            .write()
            .map_err(|e| Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::LockData,
                reason: e.to_string(),
            })?
            .remove(&self.prefix);
        Ok(())
    }
}

impl Collection for MemoryStore {
    fn last(&self) -> Result<Option<(String, Vec<u8>)>, Error> {
        let lock = self.data.read().map_err(|e| Error::Store {
            source: None,
            code: None,
            operation: StoreOperation::LockData,
            reason: e.to_string(),
        })?;
        let collection_prefix = self.collection_prefix();
        let prefix_len = collection_prefix.len();
        // BTreeMap range scan from the end: O(log n + 1) instead of
        // cloning the whole map via `iter(true)`.
        let upper = format!("{}~", collection_prefix);
        Ok(lock
            .range(collection_prefix..upper)
            .next_back()
            .map(|(key, value)| (key[prefix_len..].to_owned(), value.clone())))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        let key = format!("{}.{}", self.prefix, key);
        let lock = self.data.read().map_err(|e| Error::Store {
            source: None,
            code: None,
            operation: StoreOperation::LockData,
            reason: e.to_string(),
        })?;

        lock.get(&key).map_or_else(
            || Err(Error::EntryNotFound { key: key.clone() }),
            |value| Ok(value.clone()),
        )
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), Error> {
        let key = format!("{}.{}", self.prefix, key);
        self.data
            .write()
            .map_err(|e| Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::LockData,
                reason: e.to_string(),
            })?
            .insert(key, data.to_vec());

        Ok(())
    }

    fn del(&mut self, key: &str) -> Result<(), Error> {
        let key = format!("{}.{}", self.prefix, key);
        let mut lock = self.data.write().map_err(|e| Error::Store {
            source: None,
            code: None,
            operation: StoreOperation::LockData,
            reason: e.to_string(),
        })?;
        match lock.remove(&key) {
            Some(_) => Ok(()),
            None => Err(Error::EntryNotFound { key }),
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        let mut lock = self.data.write().map_err(|e| Error::Store {
            source: None,
            code: None,
            operation: StoreOperation::LockData,
            reason: e.to_string(),
        })?;
        let collection_prefix = self.collection_prefix();

        let keys_to_remove: Vec<String> = lock
            .keys()
            .filter(|key| key.starts_with(&collection_prefix))
            .cloned()
            .collect();
        for key in keys_to_remove {
            lock.remove(&key);
        }
        drop(lock);
        Ok(())
    }

    fn iter<'a>(
        &'a self,
        reverse: bool,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(String, Vec<u8>), Error>> + 'a>,
        Error,
    > {
        let lock = self.data.read().map_err(|e| Error::Store {
            source: None,
            code: None,
            operation: StoreOperation::LockData,
            reason: e.to_string(),
        })?;
        let collection_prefix = self.collection_prefix();
        let prefix_len = collection_prefix.len();

        let items: Vec<(String, Vec<u8>)> = if reverse {
            lock.iter()
                .rev()
                .filter(|(key, _)| key.starts_with(&collection_prefix))
                .map(|(key, value)| {
                    let key = &key[prefix_len..];
                    (key.to_owned(), value.clone())
                })
                .collect()
        } else {
            lock.iter()
                .filter(|(key, _)| key.starts_with(&collection_prefix))
                .map(|(key, value)| {
                    let key = &key[prefix_len..];
                    (key.to_owned(), value.clone())
                })
                .collect()
        };

        Ok(Box::new(items.into_iter().map(Ok)))
    }

    fn iter_range<'a>(
        &'a self,
        start: &str,
        end: &str,
        reverse: bool,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(String, Vec<u8>), Error>> + 'a>,
        Error,
    > {
        let lock = self.data.read().map_err(|e| Error::Store {
            source: None,
            code: None,
            operation: StoreOperation::LockData,
            reason: e.to_string(),
        })?;
        let collection_prefix = self.collection_prefix();
        let prefix_len = collection_prefix.len();

        let start_key = format!("{}.{}", self.prefix, start);
        let end_key = format!("{}.{}", self.prefix, end);

        let items: Vec<(String, Vec<u8>)> = if reverse {
            lock.range(start_key..=end_key)
                .rev()
                .map(|(key, value)| {
                    let key = &key[prefix_len..];
                    (key.to_owned(), value.clone())
                })
                .collect()
        } else {
            lock.range(start_key..=end_key)
                .map(|(key, value)| {
                    let key = &key[prefix_len..];
                    (key.to_owned(), value.clone())
                })
                .collect()
        };

        Ok(Box::new(items.into_iter().map(Ok)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{Collection, State};
    use crate::test_store_trait;
    test_store_trait! {
        unit_test_memory_manager:crate::memory::MemoryManager:MemoryStore
    }

    #[test]
    fn test_state_del_not_found() {
        let manager = MemoryManager::default();
        let mut state = manager.create_state("test", "test").unwrap();
        assert!(matches!(
            State::del(&mut state),
            Err(Error::EntryNotFound { key }) if key == "test"
        ));
    }

    #[test]
    fn test_collection_del_not_found() {
        let manager = MemoryManager::default();
        let mut collection = manager.create_collection("test", "test").unwrap();
        assert!(matches!(
            Collection::del(&mut collection, "missing"),
            Err(Error::EntryNotFound { key }) if key == "test.missing"
        ));
    }
}
