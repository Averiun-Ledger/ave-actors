

//! RocksDB store implementation.
//!

use ave_actors_store::{
    Error,
    database::{Collection, DbManager, State},
};

use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, DBCompactionStyle,
    DBCompressionType, DBIteratorWithThreadMode, Direction, IteratorMode,
    LogLevel, Options, WriteOptions,
};
use tracing::{debug, error, info, warn};

use std::{env, fs, path::{Path, PathBuf}, sync::Arc};

#[derive(Clone, Copy, Debug)]
enum MachineProfile {
    Tiny,
    Small,
    Medium,
    Large,
    XLarge,
}

/// RocksDB database manager for persistent actor storage.
/// Manages RocksDB instances and provides factory methods for creating
/// column families for event storage and state snapshots.
///
/// # Storage Model
///
/// - **Collections**: RocksDB column families for event storage
/// - **State**: RocksDB column families for state snapshots
/// - **Connection**: Thread-safe shared DB instance using Arc<DB>
/// - **Column Families**: Separate namespaces for different actors
///
#[derive(Clone)]
pub struct RocksDbManager {
    /// RocksDB configuration options.
    opts: Options,
    /// Thread-safe shared RocksDB instance.
    db: Arc<DB>,
    /// Per-write durability policy.
    strong_durability: bool,
}

impl RocksDbManager {
    /// Creates a new RocksDB database manager.
    /// Opens or creates a RocksDB database at the specified path,
    /// loading all existing column families.
    ///
    /// # Arguments
    ///
    /// * `path` - Directory path where the RocksDB database will be created.
    ///
    /// # Returns
    ///
    /// Returns a new RocksDbManager instance.
    ///
    /// # Errors
    ///
    /// Returns Error::CreateStore if:
    /// - The directory cannot be created
    /// - The RocksDB database cannot be opened
    ///
    /// # Behavior
    ///
    /// - Creates the directory if it doesn't exist
    /// - Lists and opens all existing column families
    /// - Enables "create_if_missing" option
    ///
    pub fn new(path: &PathBuf) -> Result<Self, Error> {
        info!("Creating RocksDB database manager");
        if !Path::new(&path).exists() {
            debug!("Path does not exist, creating it");
            fs::create_dir_all(path).map_err(|e| {
                error!(path = ?path, error = %e, "Failed to create RocksDB directory");
                Error::CreateStore {
                    reason: format!(
                    "fail RockDB create directory: {}",
                    e
                ),
                }
            })?;
        }

        let cores = resolve_hardware_cores();
        let ram_mb = resolve_hardware_ram_mb();
        let profile = resolve_profile(cores, ram_mb);
        info!(
            "RocksDB profile {:?} selected (cores: {}, RAM MB: {})",
            profile, cores, ram_mb
        );

        let strong_durability =
            env_bool("ROCKSDB_STRONG_DURABILITY").unwrap_or(false);

        let mut options = Options::default();
        apply_common_tuning(&mut options);
        match profile {
            MachineProfile::Tiny   => apply_tiny_tuning(&mut options),
            MachineProfile::Small  => apply_small_tuning(&mut options),
            MachineProfile::Medium => apply_medium_tuning(&mut options, cores),
            MachineProfile::Large  => apply_large_tuning(&mut options, cores),
            MachineProfile::XLarge => apply_xlarge_tuning(&mut options, cores),
        }

        let cfs = match DB::list_cf(&options, path) {
            Ok(cf_names) => {
                debug!(count = cf_names.len(), "Found existing column families");
                cf_names
            }
            Err(_) => {
                debug!("No existing column families, using default");
                vec!["default".to_string()]
            }
        };

        // Crear descriptores para cada column family
        let cf_opts = options.clone();
        let cf_descriptors: Vec<_> = cfs
            .iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf, cf_opts.clone()))
            .collect();

        // Abrir la base de datos con las column families existentes
        debug!(path = ?path, "Opening RocksDB database");
        let db = DB::open_cf_descriptors(&options, path, cf_descriptors)
            .map_err(|e| {
                error!(path = ?path, error = ?e, "Failed to open RocksDB");
                Error::CreateStore { reason: format!("Can not open RockDB: {}", e) }
            })?;

        debug!("RocksDB database manager created successfully");
        Ok(Self {
            opts: options,
            db: Arc::new(db),
            strong_durability,
        })
    }
}

fn apply_common_tuning(options: &mut Options) {
    options.create_if_missing(true);
    options.set_compaction_style(DBCompactionStyle::Level);
    options.set_level_compaction_dynamic_level_bytes(true);
    options.set_level_zero_file_num_compaction_trigger(8);
    options.set_level_zero_slowdown_writes_trigger(20);
    options.set_level_zero_stop_writes_trigger(36);
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_bottommost_compression_type(DBCompressionType::Zstd);
    options.set_enable_pipelined_write(true);
    options.set_bytes_per_sync(2 * 1024 * 1024); // 2MB
    options.set_wal_bytes_per_sync(512 * 1024); // 512KB
    options.set_log_level(LogLevel::Warn);
    options.set_max_log_file_size(10 * 1024 * 1024); // 10MB per LOG
    options.set_keep_log_file_num(5);
    options.set_recycle_log_file_num(2);
    options.set_log_file_time_to_roll(60 * 60); // rotate hourly at worst
}

fn apply_tiny_tuning(options: &mut Options) {
    options.increase_parallelism(1);
    options.set_max_background_jobs(1);
    options.set_write_buffer_size(8 * 1024 * 1024);       // 8MB
    options.set_max_write_buffer_number(2);
    options.set_min_write_buffer_number_to_merge(1);
    options.set_target_file_size_base(8 * 1024 * 1024);   // 8MB SST
    options.set_max_total_wal_size(16 * 1024 * 1024);     // 16MB WAL
    let mut bb = BlockBasedOptions::default();
    bb.set_bloom_filter(10.0, false);                      // ~10 bits/key, full-key
    bb.set_block_cache(&Cache::new_lru_cache(8 * 1024 * 1024)); // 8MB
    options.set_block_based_table_factory(&bb);
}

fn apply_small_tuning(options: &mut Options) {
    options.increase_parallelism(1);
    options.set_max_background_jobs(1);
    options.set_write_buffer_size(32 * 1024 * 1024);      // 32MB
    options.set_max_write_buffer_number(2);
    options.set_min_write_buffer_number_to_merge(1);
    options.set_target_file_size_base(32 * 1024 * 1024);  // 32MB SST
    options.set_max_total_wal_size(64 * 1024 * 1024);     // 64MB WAL
    let mut bb = BlockBasedOptions::default();
    bb.set_bloom_filter(10.0, false);
    bb.set_block_cache(&Cache::new_lru_cache(16 * 1024 * 1024)); // 16MB
    options.set_block_based_table_factory(&bb);
}

fn apply_medium_tuning(options: &mut Options, cores: usize) {
    let parallelism = (cores as i32).min(4).max(2);
    options.increase_parallelism(parallelism);
    options.set_max_background_jobs(parallelism);
    options.set_write_buffer_size(64 * 1024 * 1024);      // 64MB
    options.set_max_write_buffer_number(3);
    options.set_min_write_buffer_number_to_merge(1);
    options.set_target_file_size_base(64 * 1024 * 1024);  // 64MB SST
    options.set_max_total_wal_size(128 * 1024 * 1024);    // 128MB WAL
    let mut bb = BlockBasedOptions::default();
    bb.set_bloom_filter(10.0, false);
    bb.set_block_cache(&Cache::new_lru_cache(64 * 1024 * 1024)); // 64MB
    options.set_block_based_table_factory(&bb);
}

fn apply_large_tuning(options: &mut Options, cores: usize) {
    let parallelism = (cores as i32).min(8).max(2);
    options.increase_parallelism(parallelism);
    options.set_max_background_jobs(parallelism);
    options.set_write_buffer_size(128 * 1024 * 1024);     // 128MB
    options.set_max_write_buffer_number(4);
    options.set_min_write_buffer_number_to_merge(2);
    options.set_target_file_size_base(128 * 1024 * 1024); // 128MB SST
    options.set_max_total_wal_size(256 * 1024 * 1024);    // 256MB WAL
    let mut bb = BlockBasedOptions::default();
    bb.set_bloom_filter(10.0, false);
    bb.set_block_cache(&Cache::new_lru_cache(256 * 1024 * 1024)); // 256MB
    options.set_block_based_table_factory(&bb);
}

fn apply_xlarge_tuning(options: &mut Options, cores: usize) {
    let parallelism = (cores as i32).min(16).max(4);
    options.increase_parallelism(parallelism);
    options.set_max_background_jobs(parallelism);
    options.set_write_buffer_size(256 * 1024 * 1024);     // 256MB
    options.set_max_write_buffer_number(6);
    options.set_min_write_buffer_number_to_merge(2);
    options.set_target_file_size_base(256 * 1024 * 1024); // 256MB SST
    options.set_max_total_wal_size(512 * 1024 * 1024);    // 512MB WAL
    let mut bb = BlockBasedOptions::default();
    bb.set_bloom_filter(10.0, false);
    bb.set_block_cache(&Cache::new_lru_cache(1024 * 1024 * 1024)); // 1GB
    options.set_block_based_table_factory(&bb);
}

#[cfg(target_os = "linux")]
fn detect_total_memory_mb() -> Option<u64> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb_str = rest.split_whitespace().next()?;
            let kb: u64 = kb_str.parse().ok()?;
            return Some(kb / 1024);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn detect_total_memory_mb() -> Option<u64> {
    None
}

fn resolve_hardware_cores() -> usize {
    match env::var("AVE_CPU_CORES") {
        Ok(v) => v.parse::<usize>().unwrap_or_else(|_| {
            warn!("AVE_CPU_CORES='{}' is not a valid integer, using auto-detect", v);
            num_cpus::get()
        }),
        Err(_) => num_cpus::get(),
    }
}

fn resolve_hardware_ram_mb() -> u64 {
    match env::var("AVE_RAM_MB") {
        Ok(v) => v.parse::<u64>().unwrap_or_else(|_| {
            warn!("AVE_RAM_MB='{}' is not a valid integer, using auto-detect", v);
            detect_total_memory_mb().unwrap_or(4096)
        }),
        Err(_) => detect_total_memory_mb().unwrap_or(4096),
    }
}

fn resolve_profile(_cores: usize, ram_mb: u64) -> MachineProfile {
    if let Some(override_profile) = profile_override_from_env() {
        return override_profile;
    }
    match ram_mb {
        0..=255      => MachineProfile::Tiny,
        256..=1023   => MachineProfile::Small,
        1024..=4095  => MachineProfile::Medium,
        4096..=16383 => MachineProfile::Large,
        _            => MachineProfile::XLarge,
    }
}

fn profile_override_from_env() -> Option<MachineProfile> {
    match env::var("ROCKSDB_PROFILE") {
        Ok(val) => match val.to_lowercase().as_str() {
            "tiny"   => Some(MachineProfile::Tiny),
            "small"  => Some(MachineProfile::Small),
            "medium" => Some(MachineProfile::Medium),
            "large"  => Some(MachineProfile::Large),
            "xlarge" => Some(MachineProfile::XLarge),
            "auto"   => None,
            other => {
                info!(
                    "Ignoring ROCKSDB_PROFILE='{}', valid values: tiny|small|medium|large|xlarge|auto",
                    other
                );
                None
            }
        },
        Err(_) => None,
    }
}

fn env_bool(var: &str) -> Option<bool> {
    match env::var(var) {
        Ok(val) => {
            let v = val.to_lowercase();
            if ["1", "true", "yes", "on"].contains(&v.as_str()) {
                Some(true)
            } else if ["0", "false", "no", "off"].contains(&v.as_str()) {
                Some(false)
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

fn write_options(sync: bool) -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(sync);
    opts
}

impl RocksDbManager {
    fn ensure_cf(&self, name: &str) -> Result<(), Error> {
        if self.db.cf_handle(name).is_none() {
            debug!(cf = name, "Creating column family");
            self.db.create_cf(name, &self.opts).map_err(|e| {
                error!(cf = name, error = ?e, "Failed to create column family");
                Error::CreateStore { reason: format!("{:?}", e) }
            })?;
        }
        Ok(())
    }
}

impl DbManager<RocksDbStore, RocksDbStore> for RocksDbManager {
    fn create_collection(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<RocksDbStore, Error> {
        self.ensure_cf(name)?;
        debug!(cf = name, prefix = prefix, "Collection created");
        Ok(RocksDbStore {
            name: name.to_owned(),
            prefix: prefix.to_owned(),
            store: self.db.clone(),
            strong_durability: self.strong_durability,
        })
    }

    fn create_state(
        &self,
        name: &str,
        prefix: &str,
    ) -> Result<RocksDbStore, Error> {
        self.ensure_cf(name)?;
        debug!(cf = name, prefix = prefix, "State created");
        Ok(RocksDbStore {
            name: name.to_owned(),
            prefix: prefix.to_owned(),
            store: self.db.clone(),
            strong_durability: self.strong_durability,
        })
    }

    fn stop(self) -> Result<(), Error> {
        debug!("Stopping RocksDB manager, flushing WAL");
        self.db.flush_wal(true).map_err(|e| {
            error!(error = ?e, "Failed to flush WAL on stop");
            Error::Store {
                operation: "flush_wal".to_owned(),
                reason: format!("{:?}", e),
            }
        })
    }
}

/// RocksDB store that implements both Collection and State traits.
/// Stores key-value pairs in a RocksDB column family with prefix-based keys.
///
/// # Storage Layout
///
/// - **Column Family**: Separate namespace identified by `name`
/// - **Keys**: Prefixed with actor identifier for isolation
/// - **Values**: Raw bytes (serialized data)
///
/// # Thread Safety
///
/// Uses Arc<DB> for safe concurrent access across multiple stores.
///
pub struct RocksDbStore {
    /// Column family name.
    name: String,
    /// Prefix for keys (actor namespace).
    prefix: String,
    /// Shared RocksDB instance.
    store: Arc<DB>,
    /// Per-write durability policy.
    strong_durability: bool,
}

impl State for RocksDbStore {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self) -> Result<Vec<u8>, Error> {
        if let Some(handle) = self.store.cf_handle(&self.name) {
            let result = self
                .store
                .get_cf(&handle, self.prefix.clone())
                .map_err(|e| {
                    error!(cf = %self.name, error = ?e, "Failed to get state");
                    Error::Get { key: self.prefix.clone(), reason: format!("{:?}", e) }
                })?;
            match result {
                Some(value) => Ok(value),
                _ => Err(Error::EntryNotFound {
                    key: "Query returned no rows".to_owned(),
                }),
            }
        } else {
            error!(cf = %self.name, "Column family not found for state get");
            Err(Error::Store {
                operation: "column_access".to_owned(),
                reason: "RocksDB column for the store does not exist.".to_owned(),
            })
        }
    }

    fn put(&mut self, data: &[u8]) -> Result<(), Error> {
        if let Some(handle) = self.store.cf_handle(&self.name) {
            let wopts = write_options(self.strong_durability);
            Ok(self
                .store
                .put_cf_opt(&handle, self.prefix.clone(), data, &wopts)
                .map_err(|e| {
                    error!(cf = %self.name, error = ?e, "Failed to put state");
                    Error::Store { operation: "rocksdb_operation".to_owned(), reason: format!("{:?}", e) }
                })?)
        } else {
            error!(cf = %self.name, "Column family not found for state put");
            Err(Error::Store {
                operation: "column_access".to_owned(),
                reason: "RocksDB column for the store does not exist.".to_owned(),
            })
        }
    }

    fn del(&mut self) -> Result<(), Error> {
        if let Some(handle) = self.store.cf_handle(&self.name) {
            let wopts = write_options(self.strong_durability);
            Ok(self
                .store
                .delete_cf_opt(&handle, self.prefix.clone(), &wopts)
                .map_err(|e| {
                    warn!(cf = %self.name, error = ?e, "Failed to delete state");
                    Error::Store { operation: "rocksdb_operation".to_owned(), reason: format!("{:?}", e) }
                })?)
        } else {
            error!(cf = %self.name, "Column family not found for state delete");
            Err(Error::Store {
                operation: "column_access".to_owned(),
                reason: "RocksDB column for the store does not exist.".to_owned(),
            })
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        if let Some(handle) = self.store.cf_handle(&self.name) {
            let wopts = write_options(self.strong_durability);
            // Delete only the exact state key to avoid touching other prefixes,
            // even if someone reused/anidated prefixes.
            self.store
                .delete_cf_opt(&handle, self.prefix.clone(), &wopts)
                .map_err(|e| {
                    error!(cf = %self.name, error = ?e, "Failed to purge state");
                    Error::Store { operation: "rocksdb_operation".to_owned(), reason: format!("{:?}", e) }
                })
        } else {
            error!(cf = %self.name, "Column family not found for state purge");
            Err(Error::Store {
                operation: "column_access".to_owned(),
                reason: "RocksDB column for the store does not exist.".to_owned(),
            })
        }
    }

    fn flush(&self) -> Result<(), Error> {
        if let Some(handle) = self.store.cf_handle(&self.name) {
            Ok(self
                .store
                .flush_cf(&handle)
                .map_err(|e| {
                    error!(cf = %self.name, error = ?e, "Failed to flush state");
                    Error::Store { operation: "rocksdb_operation".to_owned(), reason: format!("{:?}", e) }
                })?)
        } else {
            error!(cf = %self.name, "Column family not found for state flush");
            Err(Error::Store {
                operation: "column_access".to_owned(),
                reason: "RocksDB column for the store does not exist.".to_owned(),
            })
        }
    }
}

impl Collection for RocksDbStore {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        if let Some(handle) = self.store.cf_handle(&self.name) {
            let full_key = format!("{}.{}", self.prefix, key);
            let result = self
                .store
                .get_cf(&handle, &full_key)
                .map_err(|e| {
                    error!(cf = %self.name, key = %full_key, error = ?e, "Failed to get collection entry");
                    Error::Get { key: full_key.clone(), reason: format!("{:?}", e) }
                })?;
            match result {
                Some(value) => Ok(value),
                _ => Err(Error::EntryNotFound {
                    key: "Query returned no rows".to_owned(),
                }),
            }
        } else {
            error!(cf = %self.name, "Column family not found for collection get");
            Err(Error::Store {
                operation: "column_access".to_owned(),
                reason: "RocksDB column for the store does not exist.".to_owned(),
            })
        }
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), Error> {
        if let Some(handle) = self.store.cf_handle(&self.name) {
            let key = format!("{}.{}", self.prefix, key);
            let wopts = write_options(self.strong_durability);
            Ok(self
                .store
                .put_cf_opt(&handle, key, data, &wopts)
                .map_err(|e| {
                    error!(cf = %self.name, error = ?e, "Failed to put collection entry");
                    Error::Store { operation: "rocksdb_operation".to_owned(), reason: format!("{:?}", e) }
                })?)
        } else {
            error!(cf = %self.name, "Column family not found for collection put");
            Err(Error::Store {
                operation: "column_access".to_owned(),
                reason: "RocksDB column for the store does not exist.".to_owned(),
            })
        }
    }

    fn del(&mut self, key: &str) -> Result<(), Error> {
        if let Some(handle) = self.store.cf_handle(&self.name) {
            let key = format!("{}.{}", self.prefix, key);
            let wopts = write_options(self.strong_durability);
            Ok(self
                .store
                .delete_cf_opt(&handle, key, &wopts)
                .map_err(|e| {
                    warn!(cf = %self.name, error = ?e, "Failed to delete collection entry");
                    Error::Store { operation: "rocksdb_operation".to_owned(), reason: format!("{:?}", e) }
                })?)
        } else {
            error!(cf = %self.name, "Column family not found for collection delete");
            Err(Error::Store {
                operation: "column_access".to_owned(),
                reason: "RocksDB column for the store does not exist.".to_owned(),
            })
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        if let Some(handle) = self.store.cf_handle(&self.name) {
            let wopts = write_options(self.strong_durability);
            let start = format!("{}.", self.prefix).into_bytes();
            let mut end = start.clone();
            end.push(0xFF);
            // Contract: todas las claves de colección deben seguir "{prefix}.{key}"
            // y no debe haber claves en esta CF que no empiecen por "{prefix}.";
            // este rango asume ese layout para borrar solo los eventos de este actor.
            debug!(cf = %self.name, "Purging collection with range delete");
            self.store
                .delete_range_cf_opt(&handle, start, end, &wopts)
                .map_err(|e| {
                    error!(cf = %self.name, error = ?e, "Failed to purge collection");
                    Error::Store { operation: "rocksdb_operation".to_owned(), reason: format!("{:?}", e) }
                })
        } else {
            error!(cf = %self.name, "Column family not found for collection purge");
            Err(Error::Store {
                operation: "column_access".to_owned(),
                reason: "RocksDB column for the store does not exist.".to_owned(),
            })
        }
    }

    fn iter<'a>(
        &'a self,
        reverse: bool,
    ) -> Box<dyn Iterator<Item = (String, Vec<u8>)> + 'a> {
        Box::new(RocksDbIterator::new(
            &self.store,
            self.name.clone(),
            self.prefix.clone(),
            reverse,
        ))
    }

    fn flush(&self) -> Result<(), Error> {
        if let Some(handle) = self.store.cf_handle(&self.name) {
            Ok(self
                .store
                .flush_cf(&handle)
                .map_err(|e| {
                    error!(cf = %self.name, error = ?e, "Failed to flush collection");
                    Error::Store { operation: "rocksdb_operation".to_owned(), reason: format!("{:?}", e) }
                })?)
        } else {
            error!(cf = %self.name, "Column family not found for collection flush");
            Err(Error::Store {
                operation: "column_access".to_owned(),
                reason: "RocksDB column for the store does not exist.".to_owned(),
            })
        }
    }
}

pub struct RocksDbIterator<'a> {
    prefix_dot: Vec<u8>,
    iter: DBIteratorWithThreadMode<'a, DB>,
}

impl<'a> RocksDbIterator<'a> {
    pub fn new(
        store: &'a Arc<DB>,
        name: String,
        prefix: String,
        reverse: bool,
    ) -> Self {
        let prefix_dot = format!("{}.", prefix).into_bytes();
        let mut upper_bound = prefix_dot.clone();
        upper_bound.push(0xFF);

        let handle = store
            .cf_handle(&name)
            .expect("RocksDB column for the store does not exist.");

        let mode = if reverse {
            IteratorMode::From(&upper_bound, Direction::Reverse)
        } else {
            IteratorMode::From(&prefix_dot, Direction::Forward)
        };

        let iter = store.iterator_cf(&handle, mode);
        Self { prefix_dot, iter }
    }
}

impl Iterator for RocksDbIterator<'_> {
    type Item = (String, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        for item in self.iter.by_ref() {
            match item {
                Ok((key, value)) => {
                    if !key.starts_with(&self.prefix_dot) {
                        return None;
                    }
                    let suffix = &key[self.prefix_dot.len()..];
                    let key_str = String::from_utf8(suffix.to_vec())
                        .expect("Can not convert key to string.");
                    return Some((key_str, value.to_vec()));
                }
                Err(e) => {
                    error!(error = %e, "RocksDB iteration error");
                    return None;
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    impl Default for RocksDbManager {
        fn default() -> Self {
            let dir = tempfile::tempdir()
                .expect("Can not create temporal directory.");
            let path = dir.keep();
            RocksDbManager::new(&path)
                .expect("Can not create the database.")
        }
    }

    use super::*;
    use ave_actors_store::test_store_trait;
    test_store_trait! {
        unit_test_rocksdb_manager:crate::db::RocksDbManager:RocksDbStore
    }
}
