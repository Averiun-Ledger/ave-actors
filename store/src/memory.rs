//! Memory store implementation.
//!

use crate::{
    database::{Collection, DbManager, State},
    error::{Error, StoreOperation},
};

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
};

type MemoryData = Arc<
    RwLock<HashMap<(String, String), Arc<RwLock<BTreeMap<String, Vec<u8>>>>>>,
>;

#[derive(Default, Clone)]
pub struct MemoryManager {
    data: MemoryData,
}

impl DbManager<MemoryStore, MemoryStore> for MemoryManager {
    fn create_state(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<MemoryStore, Error> {
        let mut data_lock = self.data.write().map_err(|e| Error::Store {
            operation: StoreOperation::LockManagerData,
            reason: format!("{}", e),
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

    fn stop(&mut self) -> Result<(), Error> {
        Ok(())
    }

    fn create_collection(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<MemoryStore, Error> {
        let mut data_lock = self.data.write().map_err(|e| Error::Store {
            operation: StoreOperation::LockManagerData,
            reason: format!("{}", e),
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

/// A store implementation that stores data in memory.
///
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
            operation: StoreOperation::LockData,
            reason: format!("{}", e),
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
                operation: StoreOperation::LockData,
                reason: format!("{}", e),
            })?
            .insert(self.prefix.clone(), data.to_vec());

        Ok(())
    }

    fn del(&mut self) -> Result<(), Error> {
        let mut lock = self.data.write().map_err(|e| Error::Store {
            operation: StoreOperation::LockData,
            reason: format!("{}", e),
        })?;
        match lock.remove(&self.prefix) {
            Some(_) => Ok(()),
            None => Err(Error::EntryNotFound {
                key: self.prefix.clone(),
            }),
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        let mut lock = self.data.write().map_err(|e| Error::Store {
            operation: StoreOperation::LockData,
            reason: format!("{}", e),
        })?;
        let _ = lock.remove(&self.prefix);
        Ok(())
    }
}

impl Collection for MemoryStore {
    fn last(&self) -> Result<Option<(String, Vec<u8>)>, Error> {
        let mut iter = self.iter(true)?;
        iter.next().transpose()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        let key = format!("{}.{}", self.prefix, key);
        let lock = self.data.read().map_err(|e| Error::Store {
            operation: StoreOperation::LockData,
            reason: format!("{}", e),
        })?;

        lock.get(&key).map_or_else(
            || {
                Err(Error::EntryNotFound {
                    key: key.clone(),
                })
            },
            |value| Ok(value.clone()),
        )
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), Error> {
        let key = format!("{}.{}", self.prefix, key);
        self.data
            .write()
            .map_err(|e| Error::Store {
                operation: StoreOperation::LockData,
                reason: format!("{}", e),
            })?
            .insert(key, data.to_vec());

        Ok(())
    }

    fn del(&mut self, key: &str) -> Result<(), Error> {
        let key = format!("{}.{}", self.prefix, key);
        let mut lock = self.data.write().map_err(|e| Error::Store {
            operation: StoreOperation::LockData,
            reason: format!("{}", e),
        })?;
        match lock.remove(&key) {
            Some(_) => Ok(()),
            None => Err(Error::EntryNotFound {
                key,
            }),
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        let mut lock = self.data.write().map_err(|e| Error::Store {
            operation: StoreOperation::LockData,
            reason: format!("{}", e),
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
    ) -> Result<Box<dyn Iterator<Item = Result<(String, Vec<u8>), Error>> + 'a>, Error> {
        let lock = self.data.read().map_err(|e| Error::Store {
            operation: StoreOperation::LockData,
            reason: format!("{}", e),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_store_trait;
    test_store_trait! {
        unit_test_memory_manager:crate::memory::MemoryManager:MemoryStore
    }
}
