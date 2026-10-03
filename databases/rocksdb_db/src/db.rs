//! RocksDB store implementation.
//!

use ave_actors_store::{
    Error, StoreOperation,
    config::{MachineSpec, resolve_spec},
    database::{BatchOp, BatchWrite, Collection, DbManager, State},
};

use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamilyDescriptor, DB,
    DBCompactionStyle, DBCompressionType, DBIteratorWithThreadMode, Direction,
    IteratorMode, LogLevel, Options, ReadOptions, WriteBatch, WriteOptions,
};
use tracing::{debug, error, info, warn};

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
/// Atomic multi-write handle over a shared RocksDB instance.
///
/// Every op lands in a single [`WriteBatch`] applied with one `write_opt`
/// call, which RocksDB guarantees all-or-nothing (durable across crashes
/// once acknowledged). Key mapping matches the handle implementations:
/// events as `{prefix}.{key}`, states as `{prefix}`.
struct RocksBatchWriter {
    db: Arc<DB>,
    durability: bool,
}

impl BatchWrite for RocksBatchWriter {
    fn write_batch(
        &self,
        prefix: &str,
        ops: &[BatchOp<'_>],
    ) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        for op in ops {
            match *op {
                BatchOp::PutEvent {
                    collection,
                    key,
                    data,
                } => {
                    let Some(handle) = self.db.cf_handle(collection) else {
                        error!(
                            cf = collection,
                            "Column family not found for batch event"
                        );
                        return Err(Error::Store {
                            source: None,
                            code: None,
                            operation: StoreOperation::ColumnAccess,
                            reason: "RocksDB column for the store does not \
                                     exist."
                                .to_owned(),
                        });
                    };
                    batch.put_cf(&handle, format!("{prefix}.{key}"), data);
                }
                BatchOp::PutState { store, data } => {
                    let Some(handle) = self.db.cf_handle(store) else {
                        error!(
                            cf = store,
                            "Column family not found for batch state"
                        );
                        return Err(Error::Store {
                            source: None,
                            code: None,
                            operation: StoreOperation::ColumnAccess,
                            reason: "RocksDB column for the store does not \
                                     exist."
                                .to_owned(),
                        });
                    };
                    batch.put_cf(&handle, prefix, data);
                }
            }
        }
        let wopts = write_options(self.durability);
        self.db.write_opt(batch, &wopts).map_err(|e| {
            error!(error = %e, "Failed to write batch");
            Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::RocksdbOperation,
                reason: format!("{:?}", e),
            }
        })
    }
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
pub struct RocksDbManager {
    /// RocksDB configuration options.
    opts: Options,
    /// Path to the database directory (needed for CF listing on stop).
    path: PathBuf,
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
    ///   Unlike `SqliteManager::new` (which creates `path/database.db`),
    ///   `path` itself is opened as the RocksDB database directory.
    /// * `durability` - when `true`, every write is synced to the WAL
    ///   (`WriteOptions::set_sync(true)`); when `false`, writes return
    ///   once in the OS buffers (faster, small window of loss on crash).
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
    pub fn new(
        path: &Path,
        durability: bool,
        spec: Option<MachineSpec>,
    ) -> Result<Self, Error> {
        info!("Creating RocksDB database manager");
        if !path.exists() {
            debug!("Path does not exist, creating it");
            fs::create_dir_all(path).map_err(|e| {
                error!(path = %path.display(), error = %e, "Failed to create RocksDB directory");
                Error::CreateStore {
                    reason: format!(
                    "fail RocksDB create directory: {}",
                    e
                ),
                }
            })?;
        }

        let spec = resolve_spec(spec).map_err(|e| {
            error!(error = %e, "Invalid machine spec for RocksDB manager");
            e
        })?;
        let (ram_mb, cores) = (spec.ram_mb, spec.cpu_cores);
        info!("RocksDB tuning: ram_mb={}, cpu_cores={}", ram_mb, cores);

        let mut options = Options::default();
        apply_common_tuning(&mut options);
        apply_tuning(&mut options, ram_mb, cores);

        let cfs = DB::list_cf(&options, path).map_or_else(
            |_| {
                debug!("No existing column families, using default");
                vec!["default".to_string()]
            },
            |cf_names| {
                debug!(
                    count = cf_names.len(),
                    "Found existing column families"
                );
                cf_names
            },
        );

        // Build descriptors for each existing column family.
        let cf_opts = options.clone();
        let cf_descriptors: Vec<_> = cfs
            .iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf, cf_opts.clone()))
            .collect();

        // Open the database with all existing column families.
        debug!(path = %path.display(), "Opening RocksDB database");
        let db = DB::open_cf_descriptors(&options, path, cf_descriptors)
            .map_err(|e| {
                error!(path = %path.display(), error = %e, "Failed to open RocksDB");
                Error::CreateStore { reason: format!("Can not open RocksDB: {}", e) }
            })?;

        debug!("RocksDB database manager created successfully");
        Ok(Self {
            opts: options,
            path: path.to_path_buf(),
            db: Arc::new(db),
            strong_durability: durability,
        })
    }

    #[cfg(test)]
    fn raw_db(&self) -> Arc<DB> {
        Arc::clone(&self.db)
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

/// Compute RocksDB tuning parameters directly from available RAM and CPU cores.
///
/// These values are the TOTAL machine specs, not exclusive resources for the DB.
/// The OS, actor runtime, and other processes share the same RAM, so the budget
/// is intentionally conservative: 5 % of total RAM.
///
/// Distribution: 40 % → block cache · 40 % → write buffers · 20 % → WAL.
fn apply_tuning(options: &mut Options, ram_mb: u64, cores: usize) {
    // ── Parallelism ────────────────────────────────────────────────────────────
    // Cap at half the cores (floor 1, ceiling 4) so compaction threads don't
    // starve libp2p and the actor runtime on the same machine.
    let parallelism = ((cores / 2) as i32).clamp(1, 4);
    options.increase_parallelism(parallelism);

    // ── Memory budget: 5 % of total RAM ───────────────────────────────────────
    let budget = ram_mb * 1024 * 1024 * 5 / 100; // bytes

    // Block cache: 40 % of budget, floor 4 MB, cap 512 MB
    let cache_bytes =
        (budget * 40 / 100).clamp(4 * 1024 * 1024, 512 * 1024 * 1024);

    // Write buffer count: scales with RAM
    let wb_count: u64 = match ram_mb {
        0..=1024 => 2,
        1025..=4096 => 3,
        4097..=16384 => 4,
        _ => 6,
    };

    // Write buffer size: 40 % of budget across all buffers, floor 4 MB, cap 256 MB
    let wb_size = (budget * 40 / 100 / wb_count)
        .clamp(4 * 1024 * 1024, 256 * 1024 * 1024);

    // WAL: 20 % of budget, floor 8 MB, cap 512 MB
    let wal_bytes =
        (budget * 20 / 100).clamp(8 * 1024 * 1024, 512 * 1024 * 1024);

    let merge: i32 = if wb_count <= 2 { 1 } else { 2 };

    options.set_write_buffer_size(wb_size as usize);
    options.set_max_write_buffer_number(wb_count as i32);
    options.set_min_write_buffer_number_to_merge(merge);
    // SST size follows the write buffer but stays portable: unbounded it
    // would swing from ~5 MB on tiny hosts to ~100 MB on large ones,
    // making compaction behavior machine-dependent.
    options.set_target_file_size_base(sst_target_bytes(wb_size));
    options.set_max_total_wal_size(wal_bytes);

    // Bound open files with cores (flush/compaction parallelism scales the
    // same way); unbounded FDs risk exhaustion with many column families.
    options.set_max_open_files(max_open_files(cores));

    // ── Block cache ────────────────────────────────────────────────────────────
    let mut bb = BlockBasedOptions::default();
    // 16 KiB blocks favor the sequential event-log scans over point reads.
    bb.set_block_size(16 * 1024);
    bb.set_bloom_filter(10.0, false);
    bb.set_cache_index_and_filter_blocks(true);
    bb.set_block_cache(&Cache::new_lru_cache(cache_bytes as usize));
    options.set_block_based_table_factory(&bb);
}

fn write_options(sync: bool) -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(sync);
    opts
}

/// SST target size derived from the write-buffer size, clamped to a
/// portable band so compaction behavior does not depend on host size.
fn sst_target_bytes(wb_size: u64) -> u64 {
    wb_size.clamp(16 * 1024 * 1024, 64 * 1024 * 1024)
}

/// Open-file ceiling derived from CPU cores.
fn max_open_files(cores: usize) -> i32 {
    ((cores * 128) as i32).clamp(256, 2048)
}

impl RocksDbManager {
    /// Validates a column family name: same `[A-Za-z_][A-Za-z0-9_]*` rule
    /// as SQLite identifiers, so both backends accept the same names.
    fn validate_cf_name(name: &str) -> Result<(), Error> {
        let mut chars = name.chars();
        let Some(first) = chars.next() else {
            return Err(Error::CreateStore {
                reason: "invalid column family name: empty".to_owned(),
            });
        };
        let valid_start = first == '_' || first.is_ascii_alphabetic();
        let valid_rest =
            chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric());
        if valid_start && valid_rest {
            return Ok(());
        }
        Err(Error::CreateStore {
            reason: format!(
                "invalid column family name '{name}': allowed pattern is [A-Za-z_][A-Za-z0-9_]*"
            ),
        })
    }

    fn ensure_cf(&self, name: &str) -> Result<(), Error> {
        Self::validate_cf_name(name)?;
        if self.db.cf_handle(name).is_some() {
            return Ok(());
        }
        match self.db.create_cf(name, &self.opts) {
            Ok(()) => Ok(()),
            Err(e) => {
                // Lost the creation race: another thread created it first.
                if self.db.cf_handle(name).is_some() {
                    debug!(cf = name, "Column family appeared concurrently");
                    return Ok(());
                }
                error!(cf = name, error = %e, "Failed to create column family");
                Err(Error::CreateStore {
                    reason: format!("{:?}", e),
                })
            }
        }
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
            store: Arc::clone(&self.db),
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
            store: Arc::clone(&self.db),
            strong_durability: self.strong_durability,
        })
    }

    fn batch_writer(&self) -> Option<Box<dyn BatchWrite>> {
        Some(Box::new(RocksBatchWriter {
            db: Arc::clone(&self.db),
            durability: self.strong_durability,
        }))
    }

    fn stop(self) -> Result<(), Error> {
        debug!("Stopping RocksDB manager, flushing memtables and WAL");

        // Sync WAL first: ensures all committed writes survive even if the
        // memtable flush below is interrupted.
        self.db.flush_wal(true).map_err(|e| {
            error!(error = %e, "Failed to flush WAL on stop");
            Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::FlushWal,
                reason: format!("{:?}", e),
            }
        })?;

        // Flush every column family's memtable → SST so the next startup
        // does not need WAL replay. Errors here are non-fatal because the
        // WAL sync above already guarantees durability. Column families
        // created concurrently after the listing below are covered by the
        // WAL sync (they only miss the best-effort memtable flush).
        let cf_names =
            DB::list_cf(&self.opts, &self.path).map_err(|e| Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ListCf,
                reason: format!("{:?}", e),
            })?;
        for name in &cf_names {
            if let Some(handle) = self.db.cf_handle(name)
                && let Err(e) = self.db.flush_cf(&handle)
            {
                warn!(cf = name, error = %e, "Failed to flush column family on stop");
            }
        }

        // Dropping `self` releases this manager's strong reference to the
        // shared `Arc<DB>`. RocksDB closes and the file lock is freed only
        // once the last clone (managers, stores and iterators) is dropped:
        // callers must drop every `RocksDbStore` and iterator before
        // reopening the same path, or the open fails with a file lock
        // error. `Ok` here means "flushed", not "closed".
        debug!("RocksDB stop complete");
        Ok(())
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

impl RocksDbStore {
    /// Returns the column-family handle for this store, or `None` if the
    /// column family has not been created yet.
    fn cf(&self) -> Option<Arc<BoundColumnFamily<'_>>> {
        self.store.cf_handle(&self.name)
    }
}

impl State for RocksDbStore {
    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self) -> Result<Vec<u8>, Error> {
        if let Some(handle) = self.cf() {
            let result =
                self.store.get_cf(&handle, &self.prefix).map_err(|e| {
                    error!(cf = %self.name, error = %e, "Failed to get state");
                    Error::Get {
                        key: self.prefix.clone(),
                        reason: format!("{:?}", e),
                    }
                })?;
            result.map_or_else(
                || {
                    Err(Error::EntryNotFound {
                        key: self.prefix.clone(),
                    })
                },
                Ok,
            )
        } else {
            error!(cf = %self.name, "Column family not found for state get");
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            })
        }
    }

    fn put(&mut self, data: &[u8]) -> Result<(), Error> {
        if let Some(handle) = self.cf() {
            let wopts = write_options(self.strong_durability);
            Ok(self
                .store
                .put_cf_opt(&handle, &self.prefix, data, &wopts)
                .map_err(|e| {
                    error!(cf = %self.name, error = %e, "Failed to put state");
                    Error::Store {
                        source: None,
                        code: None,
                        operation: StoreOperation::RocksdbOperation,
                        reason: format!("{:?}", e),
                    }
                })?)
        } else {
            error!(cf = %self.name, "Column family not found for state put");
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            })
        }
    }

    fn del(&mut self) -> Result<(), Error> {
        if let Some(handle) = self.cf() {
            let key = self.prefix.clone();
            let exists = self
                .store
                .get_cf(&handle, &key)
                .map_err(|e| {
                    error!(cf = %self.name, key = %key, error = %e, "Failed to check state before delete");
                    Error::Get {
                        key: key.clone(),
                        reason: format!("{:?}", e),
                    }
                })?
                .is_some();
            if !exists {
                return Err(Error::EntryNotFound { key });
            }

            let wopts = write_options(self.strong_durability);
            Ok(self
                .store
                .delete_cf_opt(&handle, &self.prefix, &wopts)
                .map_err(|e| {
                    warn!(cf = %self.name, error = %e, "Failed to delete state");
                    Error::Store {
                source: None,
                code: None,
                        operation: StoreOperation::RocksdbOperation,
                        reason: format!("{:?}", e),
                    }
                })?)
        } else {
            error!(cf = %self.name, "Column family not found for state delete");
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            })
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        if let Some(handle) = self.cf() {
            let wopts = write_options(self.strong_durability);
            // Delete only the exact state key to avoid touching other prefixes,
            // even if someone reused or nested prefixes.
            self.store
                .delete_cf_opt(&handle, &self.prefix, &wopts)
                .map_err(|e| {
                    error!(cf = %self.name, error = %e, "Failed to purge state");
                    Error::Store {
                source: None,
                code: None,
                        operation: StoreOperation::RocksdbOperation,
                        reason: format!("{:?}", e),
                    }
                })
        } else {
            error!(cf = %self.name, "Column family not found for state purge");
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            })
        }
    }
}

impl Collection for RocksDbStore {
    fn last(&self) -> Result<Option<(String, Vec<u8>)>, Error> {
        let Some(handle) = self.cf() else {
            error!(cf = %self.name, "Column family not found for last");
            return Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            });
        };
        // Consistent point read: the snapshot pins the view for this call,
        // unlike the lazy iterators below (documented quiescent-only).
        let snapshot = self.store.snapshot();
        let prefix_dot = format!("{}.", self.prefix).into_bytes();
        let mut upper_bound = prefix_dot.clone();
        upper_bound.push(0xFF);
        let mut read_options = ReadOptions::default();
        read_options.set_snapshot(&snapshot);
        read_options.set_iterate_upper_bound(upper_bound.clone());
        read_options.set_iterate_lower_bound(prefix_dot.clone());
        let mut iter = self.store.iterator_cf_opt(
            &handle,
            read_options,
            IteratorMode::From(&upper_bound, Direction::Reverse),
        );
        let result = match iter.next() {
            None => None,
            Some(Err(e)) => {
                error!(error = %e, "RocksDB iteration error");
                return Err(Error::Get {
                    key: String::from_utf8_lossy(&prefix_dot).into_owned(),
                    reason: format!("{}", e),
                });
            }
            Some(Ok((key, value))) => decode_entry(&prefix_dot, &key, &value),
        };
        let value = result.transpose()?;
        debug!(has_value = value.is_some(), "last() fetched");
        Ok(value)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        if let Some(handle) = self.cf() {
            let full_key = format!("{}.{}", self.prefix, key);
            let result = self
                .store
                .get_cf(&handle, &full_key)
                .map_err(|e| {
                    error!(cf = %self.name, key = %full_key, error = %e, "Failed to get collection entry");
                    Error::Get { key: full_key.clone(), reason: format!("{:?}", e) }
                })?;
            result
                .map_or_else(|| Err(Error::EntryNotFound { key: full_key }), Ok)
        } else {
            error!(cf = %self.name, "Column family not found for collection get");
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            })
        }
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), Error> {
        if let Some(handle) = self.cf() {
            let key = format!("{}.{}", self.prefix, key);
            let wopts = write_options(self.strong_durability);
            Ok(self
                .store
                .put_cf_opt(&handle, key, data, &wopts)
                .map_err(|e| {
                    error!(cf = %self.name, error = %e, "Failed to put collection entry");
                    Error::Store {
                source: None,
                code: None,
                        operation: StoreOperation::RocksdbOperation,
                        reason: format!("{:?}", e),
                    }
                })?)
        } else {
            error!(cf = %self.name, "Column family not found for collection put");
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            })
        }
    }

    fn del(&mut self, key: &str) -> Result<(), Error> {
        if let Some(handle) = self.cf() {
            let key = format!("{}.{}", self.prefix, key);
            let exists = self
                .store
                .get_cf(&handle, &key)
                .map_err(|e| {
                    error!(cf = %self.name, key = %key, error = %e, "Failed to check collection entry before delete");
                    Error::Get {
                        key: key.clone(),
                        reason: format!("{:?}", e),
                    }
                })?
                .is_some();
            if !exists {
                return Err(Error::EntryNotFound { key });
            }

            let wopts = write_options(self.strong_durability);
            Ok(self
                .store
                .delete_cf_opt(&handle, key, &wopts)
                .map_err(|e| {
                    warn!(cf = %self.name, error = %e, "Failed to delete collection entry");
                    Error::Store {
                source: None,
                code: None,
                        operation: StoreOperation::RocksdbOperation,
                        reason: format!("{:?}", e),
                    }
                })?)
        } else {
            error!(cf = %self.name, "Column family not found for collection delete");
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            })
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        if let Some(handle) = self.cf() {
            let wopts = write_options(self.strong_durability);
            let start = format!("{}.", self.prefix).into_bytes();
            let mut end = start.clone();
            end.push(0xFF);
            // Contract: collection keys in this column family follow
            // "{prefix}.{key}", and unrelated keys are not mixed into the same
            // range. This lets the backend delete only this actor's entries.
            debug!(cf = %self.name, "Purging collection with range delete");
            self.store
                .delete_range_cf_opt(&handle, start.clone(), end.clone(), &wopts)
                .map_err(|e| {
                    error!(cf = %self.name, error = %e, "Failed to purge collection");
                    Error::Store {
                source: None,
                code: None,
                        operation: StoreOperation::RocksdbOperation,
                        reason: format!("{:?}", e),
                    }
                })?;
            // Reclaim the range tombstones just written, scoped to this
            // prefix only: a full-CF compaction would punish neighbors
            // sharing the column family. Best-effort: the delete above
            // already succeeded.
            self.store.compact_range_cf(
                &handle,
                Some(&start[..]),
                Some(&end[..]),
            );
            Ok(())
        } else {
            error!(cf = %self.name, "Column family not found for collection purge");
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            })
        }
    }

    fn iter<'a>(
        &'a self,
        reverse: bool,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(String, Vec<u8>), Error>> + 'a>,
        Error,
    > {
        let Some(_handle) = self.cf() else {
            error!(cf = %self.name, "Column family not found for collection iter");
            return Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            });
        };
        Ok(Box::new(RocksDbIterator::new(
            &self.store,
            self.name.clone(),
            self.prefix.clone(),
            reverse,
        )?))
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
        let Some(_handle) = self.cf() else {
            error!(cf = %self.name, "Column family not found for collection iter_range");
            return Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            });
        };
        Ok(Box::new(RocksDbRangeIterator::new(
            &self.store,
            self.name.clone(),
            self.prefix.clone(),
            start,
            end,
            reverse,
        )?))
    }

    fn del_range(&mut self, start: &str, end: &str) -> Result<(), Error> {
        if let Some(handle) = self.cf() {
            let wopts = write_options(self.strong_durability);
            let start_key = format!("{}.{}", self.prefix, start).into_bytes();
            // Exclusive end: `end + \x00` is the smallest key strictly
            // greater than the inclusive `end` key, so `[start, end]`
            // is deleted exactly without catching `end_extra` keys
            // (the previous `0xFF` suffix over-deleted siblings).
            let mut end_key = format!("{}.{}", self.prefix, end).into_bytes();
            end_key.push(0x00);
            debug!(cf = %self.name, "Deleting collection range");
            self.store
                .delete_range_cf_opt(&handle, start_key.clone(), end_key.clone(), &wopts)
                .map_err(|e| {
                    error!(cf = %self.name, error = %e, "Failed to delete collection range");
                    Error::Store {
                source: None,
                code: None,
                        operation: StoreOperation::RocksdbOperation,
                        reason: format!("{:?}", e),
                    }
                })?;
            // Reclaim just-deleted range tombstones, scoped to the range
            // (see `purge`): best-effort, the delete already succeeded.
            self.store.compact_range_cf(
                &handle,
                Some(&start_key[..]),
                Some(&end_key[..]),
            );
            Ok(())
        } else {
            error!(cf = %self.name, "Column family not found for collection del_range");
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            })
        }
    }
}

pub(crate) struct RocksDbIterator<'a> {
    prefix_dot: Vec<u8>,
    iter: DBIteratorWithThreadMode<'a, DB>,
}

/// Strips the `{prefix}.` namespace from one raw iterator entry.
///
/// Returns `None` when the key leaves the prefix (end of range for the
/// caller to stop at).
fn decode_entry(
    prefix_dot: &[u8],
    key: &[u8],
    value: &[u8],
) -> Option<Result<(String, Vec<u8>), Error>> {
    if !key.starts_with(prefix_dot) {
        return None;
    }
    let suffix = &key[prefix_dot.len()..];
    let key_str = match std::str::from_utf8(suffix) {
        Ok(s) => s.to_owned(),
        Err(error) => {
            return Some(Err(Error::Get {
                key: String::from_utf8_lossy(key).into_owned(),
                reason: format!("{}", error),
            }));
        }
    };
    Some(Ok((key_str, value.to_vec())))
}

impl<'a> RocksDbIterator<'a> {
    pub(crate) fn new(
        store: &'a Arc<DB>,
        name: String,
        prefix: String,
        reverse: bool,
    ) -> Result<Self, Error> {
        let prefix_dot = format!("{}.", prefix).into_bytes();
        let mut upper_bound = prefix_dot.clone();
        upper_bound.push(0xFF);

        let Some(handle) = store.cf_handle(&name) else {
            return Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            });
        };

        // Hard bounds keep the scan inside this prefix instead of walking
        // to the end of the column family when it is the last one.
        let mut read_options = ReadOptions::default();
        read_options.set_iterate_upper_bound(upper_bound.clone());
        read_options.set_iterate_lower_bound(prefix_dot.clone());

        let mode = if reverse {
            IteratorMode::From(&upper_bound, Direction::Reverse)
        } else {
            IteratorMode::From(&prefix_dot, Direction::Forward)
        };

        let iter = store.iterator_cf_opt(&handle, read_options, mode);
        Ok(Self { prefix_dot, iter })
    }
}

impl Iterator for RocksDbIterator<'_> {
    type Item = Result<(String, Vec<u8>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?;
        match item {
            Ok((key, value)) => decode_entry(&self.prefix_dot, &key, &value),
            Err(e) => {
                error!(error = %e, "RocksDB iteration error");
                Some(Err(Error::Get {
                    key: String::from_utf8_lossy(&self.prefix_dot).into_owned(),
                    reason: format!("{}", e),
                }))
            }
        }
    }
}

pub(crate) struct RocksDbRangeIterator<'a> {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    prefix_dot: Vec<u8>,
    reverse: bool,
    iter: DBIteratorWithThreadMode<'a, DB>,
}

impl<'a> RocksDbRangeIterator<'a> {
    pub(crate) fn new(
        store: &'a Arc<DB>,
        name: String,
        prefix: String,
        start: &str,
        end: &str,
        reverse: bool,
    ) -> Result<Self, Error> {
        let prefix_dot = format!("{}.", prefix).into_bytes();
        let start_key = format!("{}.{}", prefix, start).into_bytes();
        let end_key = format!("{}.{}", prefix, end).into_bytes();
        // Exclusive upper bound matching the inclusive `end` (same trick
        // as `del_range`).
        let mut end_exclusive = end_key.clone();
        end_exclusive.push(0x00);

        let Some(handle) = store.cf_handle(&name) else {
            return Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                reason: "RocksDB column for the store does not exist."
                    .to_owned(),
            });
        };

        let mut read_options = ReadOptions::default();
        read_options.set_iterate_lower_bound(start_key.clone());
        read_options.set_iterate_upper_bound(end_exclusive.clone());

        let mode = if reverse {
            IteratorMode::From(&end_key, Direction::Reverse)
        } else {
            IteratorMode::From(&start_key, Direction::Forward)
        };

        let iter = store.iterator_cf_opt(&handle, read_options, mode);
        Ok(Self {
            start_key,
            end_key,
            prefix_dot,
            reverse,
            iter,
        })
    }
}

impl Iterator for RocksDbRangeIterator<'_> {
    type Item = Result<(String, Vec<u8>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?;
        match item {
            Ok((key, value)) => {
                // The engine bounds already confine the scan; these stays
                // as defense for hand-built iterators.
                if !key.starts_with(&self.prefix_dot) {
                    return None;
                }
                if !self.reverse && key.as_ref() > self.end_key.as_slice() {
                    return None;
                }
                if self.reverse && key.as_ref() < self.start_key.as_slice() {
                    return None;
                }
                decode_entry(&self.prefix_dot, &key, &value)
            }
            Err(e) => {
                error!(error = %e, "RocksDB range iteration error");
                Some(Err(Error::Get {
                    key: String::from_utf8_lossy(&self.prefix_dot).into_owned(),
                    reason: format!("{}", e),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    /// Retains every `TempDir` created during tests so they are cleaned up
    /// automatically when the test process exits.
    static TEMP_DIRS: Mutex<Vec<tempfile::TempDir>> = Mutex::new(Vec::new());

    impl Default for RocksDbManager {
        fn default() -> Self {
            let dir = tempfile::tempdir()
                .expect("Can not create temporal directory.");
            let path = dir.path().to_path_buf();
            TEMP_DIRS.lock().unwrap().push(dir);
            Self::new(&path, false, None).expect("Can not create the database.")
        }
    }

    use super::*;
    use ave_actors_store::test_store_trait;
    test_store_trait! {
        unit_test_rocksdb_manager:crate::db::RocksDbManager:RocksDbStore
    }

    #[test]
    fn test_missing_cf_state_and_collection() {
        let manager = RocksDbManager::default();
        let mut store = RocksDbStore {
            name: "no_such_cf".to_owned(),
            prefix: "pref".to_owned(),
            store: manager.raw_db(),
            strong_durability: false,
        };

        assert!(matches!(
            State::get(&store),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
        assert!(matches!(
            State::put(&mut store, b"x"),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
        assert!(matches!(
            State::del(&mut store),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
        assert!(matches!(
            State::purge(&mut store),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));

        assert!(matches!(
            Collection::last(&store),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
        assert!(matches!(
            Collection::get(&store, "k"),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
        assert!(matches!(
            Collection::put(&mut store, "k", b"v"),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
        assert!(matches!(
            Collection::del(&mut store, "k"),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
        assert!(matches!(
            Collection::purge(&mut store),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
        assert!(matches!(
            Collection::iter(&store, false),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
        assert!(matches!(
            Collection::iter_range(&store, "a", "z", false),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
        assert!(matches!(
            Collection::del_range(&mut store, "a", "z"),
            Err(Error::Store {
                source: None,
                code: None,
                operation: StoreOperation::ColumnAccess,
                ..
            })
        ));
    }

    #[test]
    fn test_missing_cf_iterators() {
        let manager = RocksDbManager::default();
        let db = manager.raw_db();
        assert!(
            RocksDbIterator::new(
                &db,
                "missing".to_owned(),
                "p".to_owned(),
                false
            )
            .is_err()
        );
        assert!(
            RocksDbRangeIterator::new(
                &db,
                "missing".to_owned(),
                "p".to_owned(),
                "a",
                "z",
                false
            )
            .is_err()
        );
    }

    #[test]
    fn test_iterator_invalid_utf8() {
        let manager = RocksDbManager::default();
        let mut store = manager.create_collection("c", "pref").unwrap();
        Collection::put(&mut store, "a", b"1").unwrap();

        let db = manager.raw_db();
        let handle = db.cf_handle("c").unwrap();
        let bad_key = b"pref.\xFE";
        db.put_cf(&handle, bad_key, b"bad").unwrap();

        let mut iter = store.iter(false).unwrap();
        assert_eq!(
            iter.next().unwrap().unwrap(),
            ("a".to_string(), b"1".to_vec())
        );
        let err = iter.next().unwrap().unwrap_err();
        assert!(matches!(err, Error::Get { .. }));

        let mut iter = store.iter(true).unwrap();
        let err = iter.next().unwrap().unwrap_err();
        assert!(matches!(err, Error::Get { .. }));
    }

    #[test]
    fn test_ensure_cf_null_name_fails() {
        let manager = RocksDbManager::default();
        let result = manager.ensure_cf("test\0name");
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_applies_event_and_states_atomically() {
        use ave_actors_store::database::BatchOp;

        let manager = RocksDbManager::default();
        manager.create_collection("b_events", "p").unwrap();
        manager.create_state("b_states", "p").unwrap();
        manager.create_state("b_metadata", "p").unwrap();

        let writer = manager.batch_writer().expect("rocksdb supports batch");
        writer
            .write_batch(
                "p",
                &[
                    BatchOp::PutEvent {
                        collection: "b_events",
                        key: "00000000000000000000",
                        data: b"event",
                    },
                    BatchOp::PutState {
                        store: "b_states",
                        data: b"snapshot",
                    },
                    BatchOp::PutState {
                        store: "b_metadata",
                        data: b"metadata",
                    },
                ],
            )
            .unwrap();

        let events = manager.create_collection("b_events", "p").unwrap();
        assert_eq!(
            Collection::get(&events, "00000000000000000000").unwrap(),
            b"event".to_vec()
        );
        let states = manager.create_state("b_states", "p").unwrap();
        assert_eq!(State::get(&states).unwrap(), b"snapshot".to_vec());
        let metadata = manager.create_state("b_metadata", "p").unwrap();
        assert_eq!(State::get(&metadata).unwrap(), b"metadata".to_vec());
    }

    #[test]
    fn test_batch_with_missing_cf_applies_nothing() {
        use ave_actors_store::database::BatchOp;

        let manager = RocksDbManager::default();
        manager.create_collection("rb2_events", "p").unwrap();

        let writer = manager.batch_writer().expect("rocksdb supports batch");
        let result = writer.write_batch(
            "p",
            &[
                BatchOp::PutEvent {
                    collection: "rb2_events",
                    key: "00000000000000000000",
                    data: b"event",
                },
                BatchOp::PutState {
                    store: "no_such_cf",
                    data: b"snapshot",
                },
            ],
        );
        assert!(result.is_err(), "batch with a missing CF must fail");

        // Nothing was applied: the batch is validated before the single
        // engine write, so the event is absent too.
        let events = manager.create_collection("rb2_events", "p").unwrap();
        assert!(
            Collection::get(&events, "00000000000000000000").is_err(),
            "event from a failed batch must not be visible"
        );
    }

    #[test]
    fn test_del_range_is_exact_inclusive() {
        let manager = RocksDbManager::default();
        let mut store = manager.create_collection("c", "pref").unwrap();
        Collection::put(&mut store, "a", b"1").unwrap();
        Collection::put(&mut store, "a_extra", b"2").unwrap();
        Collection::put(&mut store, "b", b"3").unwrap();

        Collection::del_range(&mut store, "a", "a").unwrap();

        assert!(matches!(
            Collection::get(&store, "a"),
            Err(Error::EntryNotFound { .. })
        ));
        // Sibling sharing the `a` prefix must survive an exact range delete.
        assert_eq!(Collection::get(&store, "a_extra").unwrap(), b"2".to_vec());
        assert_eq!(Collection::get(&store, "b").unwrap(), b"3".to_vec());
    }
}

#[cfg(test)]
mod tuning_tests {
    use super::{max_open_files, sst_target_bytes};

    #[test]
    fn test_sst_target_stays_portable() {
        // Tiny host buffer: floored, not 5 MB.
        assert_eq!(sst_target_bytes(5 * 1024 * 1024), 16 * 1024 * 1024);
        // Huge host buffer: capped, not 100+ MB.
        assert_eq!(sst_target_bytes(256 * 1024 * 1024), 64 * 1024 * 1024);
        // In-band value passes through.
        assert_eq!(sst_target_bytes(32 * 1024 * 1024), 32 * 1024 * 1024);
    }

    #[test]
    fn test_max_open_files_scales_with_cores() {
        assert_eq!(max_open_files(1), 256);
        assert_eq!(max_open_files(4), 512);
        assert_eq!(max_open_files(64), 2048);
    }
}

#[cfg(test)]
mod cf_validation_tests {
    use super::*;

    #[test]
    fn test_ensure_cf_rejects_invalid_names() {
        let manager = RocksDbManager::default();
        for bad in [
            "",
            "has space",
            "with.dot",
            "with/slash",
            "0abc",
            " Ünicode",
        ] {
            assert!(
                manager.ensure_cf(bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
        manager.ensure_cf("valid_name_123").unwrap();
        manager.ensure_cf("_also_valid").unwrap();
    }
}
