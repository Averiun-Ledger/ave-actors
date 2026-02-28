//! Memory store implementation.
//!

use crate::{
    database::{Collection, DbManager, State},
    error::Error,
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
            operation: "lock_manager_data".to_owned(),
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

    fn stop(self) -> Result<(), Error> {
        Ok(())
    }

    fn create_collection(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<MemoryStore, Error> {
        let mut data_lock = self.data.write().map_err(|e| Error::Store {
            operation: "lock_manager_data".to_owned(),
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

impl State for MemoryStore {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self) -> Result<Vec<u8>, Error> {
        let lock = self.data.read().map_err(|e| Error::Store {
            operation: "lock_data".to_owned(),
            reason: format!("{}", e),
        })?;

        lock.get(&self.prefix).map_or_else(
            || {
                Err(Error::EntryNotFound {
                    key: "Query returned no rows".to_owned(),
                })
            },
            |value| Ok(value.clone()),
        )
    }

    fn put(&mut self, data: &[u8]) -> Result<(), Error> {
        self.data
            .write()
            .map_err(|e| Error::Store {
                operation: "lock_data".to_owned(),
                reason: format!("{}", e),
            })?
            .insert(self.prefix.clone(), data.to_vec());

        Ok(())
    }

    fn del(&mut self) -> Result<(), Error> {
        let mut lock = self.data.write().map_err(|e| Error::Store {
            operation: "lock_data".to_owned(),
            reason: format!("{}", e),
        })?;
        match lock.remove(&self.prefix) {
            Some(_) => Ok(()),
            None => Err(Error::EntryNotFound {
                key: "Query returned no rows".to_owned(),
            }),
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        let mut lock = self.data.write().map_err(|e| Error::Store {
            operation: "lock_data".to_owned(),
            reason: format!("{}", e),
        })?;

        let keys_to_remove: Vec<String> = lock
            .keys()
            .filter(|key| key.starts_with(&self.prefix))
            .cloned()
            .collect();
        for key in keys_to_remove {
            lock.remove(&key);
        }
        drop(lock);
        Ok(())
    }
}

impl Collection for MemoryStore {
    fn last(&self) -> Option<(String, Vec<u8>)> {
        let mut iter = self.iter(true);
        iter.next()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        let key = format!("{}.{}", self.prefix, key);
        let lock = self.data.read().map_err(|e| Error::Store {
            operation: "lock_data".to_owned(),
            reason: format!("{}", e),
        })?;

        lock.get(&key).map_or_else(
            || {
                Err(Error::EntryNotFound {
                    key: "Query returned no rows".to_owned(),
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
                operation: "lock_data".to_owned(),
                reason: format!("{}", e),
            })?
            .insert(key, data.to_vec());

        Ok(())
    }

    fn del(&mut self, key: &str) -> Result<(), Error> {
        let key = format!("{}.{}", self.prefix, key);
        let mut lock = self.data.write().map_err(|e| Error::Store {
            operation: "lock_data".to_owned(),
            reason: format!("{}", e),
        })?;
        match lock.remove(&key) {
            Some(_) => Ok(()),
            None => Err(Error::EntryNotFound {
                key: "Query returned no rows".to_owned(),
            }),
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        let mut lock = self.data.write().map_err(|e| Error::Store {
            operation: "lock_data".to_owned(),
            reason: format!("{}", e),
        })?;

        let keys_to_remove: Vec<String> = lock
            .keys()
            .filter(|key| key.starts_with(&self.prefix))
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
    ) -> Box<dyn Iterator<Item = (String, Vec<u8>)> + 'a> {
        let Ok(lock) = self.data.read() else {
            return Box::new(std::iter::empty());
        };

        let items: Vec<(String, Vec<u8>)> = if reverse {
            lock.iter()
                .rev()
                .filter(|(key, _)| key.starts_with(&self.prefix))
                .map(|(key, value)| {
                    let key = &key[self.prefix.len() + 1..];
                    (key.to_owned(), value.clone())
                })
                .collect()
        } else {
            lock.iter()
                .filter(|(key, _)| key.starts_with(&self.prefix))
                .map(|(key, value)| {
                    let key = &key[self.prefix.len() + 1..];
                    (key.to_owned(), value.clone())
                })
                .collect()
        };

        Box::new(items.into_iter())
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
