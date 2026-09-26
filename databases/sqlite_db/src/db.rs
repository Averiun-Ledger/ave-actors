//! # SQLite database backend.
//!
//! This module contains the SQLite database backend implementation.
//!

use ave_actors_store::{
    Error, StoreOperation,
    config::{MachineSpec, resolve_spec},
    database::{BatchOp, BatchWrite, Collection, DbManager, State},
};

use rusqlite::{
    Connection, Error as SqliteError, OpenFlags, OptionalExtension, params,
};
use tracing::{debug, error, info, warn};

use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
use std::{fs, path::Path};

type EntryIterator = Box<dyn Iterator<Item = Result<(String, Vec<u8>), Error>>>;
const ITER_CHUNK_SIZE: usize = 1_000;

/// How long a `checkout` waits for a connection before failing instead of
/// parking the caller forever. Bounded waits turn pool exhaustion into a
/// visible, supervisable error rather than a silent worker stall.
const DEFAULT_CHECKOUT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long `stop` waits for checked-out connections to return before
/// giving up instead of hanging shutdown forever.
const POOL_STOP_TIMEOUT: Duration = Duration::from_secs(30);

/// Share of host RAM budgeting all pooled connection page caches together.
const POOL_RAM_BUDGET_PERCENT: u64 = 6;

/// SQLite database manager for persistent actor storage.
/// Manages SQLite database connections and provides factory methods
/// for creating collections (event storage) and state storage (snapshots).
///
/// # Storage Model
///
/// - **Collections**: SQLite tables with (prefix, sn, value) schema
/// - **State**: SQLite tables with (prefix, value) schema
/// - **Connection**: Administrative connection in the manager plus a shared
///   connection pool sized from machine specs.
///
#[derive(Clone)]
pub struct SqliteManager {
    /// Administrative SQLite connection for DDL and shutdown maintenance.
    admin_conn: Arc<Mutex<Connection>>,
    /// Shared connection pool for all actor handles.
    pool: Arc<SqlitePool>,
}

/// Internal connection pool.
///
/// The pool is elastic: it creates connections on demand but never retains
/// more than `max_size` total connections, and shrinks idle connections
/// above `idle_keep` on checkin so bursts do not pin page cache forever.
/// This matches the actor model where database access is sporadic — actors
/// keep state in memory and only touch persistence during recovery, persist,
/// or snapshot.
///
/// Creation is bounded: if `max_size` connections already exist (idle or
/// checked-out), `checkout` waits up to `checkout_timeout` and then fails
/// instead of parking the caller forever. Failures are loud (error +
/// warning with pool stats) so exhaustion surfaces in supervision instead
/// of stalling workers silently.
///
/// The blocking is synchronous (`Mutex` + `Condvar`): while waiting, the
/// calling thread is parked instead of yielding, so on an async runtime it
/// holds a worker thread for at most `checkout_timeout`. `max_size` is
/// sized from the host (CPU and RAM) to keep this bounded; checkout scopes
/// must stay short so connections return promptly and the runtime is not
/// stalled.
struct SqlitePool {
    path: PathBuf,
    durability: bool,
    tuning: SqliteTuning,
    max_size: usize,
    /// Idle connections retained on checkin; extras are closed (`total`
    /// decremented) instead of accumulating page cache.
    idle_keep: usize,
    checkout_timeout: Duration,
    state: Mutex<PoolState>,
    condvar: Condvar,
}

struct PoolState {
    available: Vec<Connection>,
    total: usize,
}

/// A connection checked out from the pool.
///
/// On drop the connection is returned to the pool's idle set (see
/// `SqlitePool::checkin` for the defensive discard fallback).
struct PooledConnection {
    conn: std::mem::ManuallyDrop<Connection>,
    pool: Arc<SqlitePool>,
}

impl std::ops::Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        // `conn` is never moved out while `self` is alive; Drop is the only
        // place that consumes it, and Drop cannot run concurrently with Deref.
        &self.conn
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Same invariant as `Deref`: the connection is present until Drop.
        &mut self.conn
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        // SAFETY: `conn` is only accessed through Deref/DerefMut while `self`
        // is alive, and this is the only place that moves it out. After this
        // call the value is no longer used, satisfying ManuallyDrop's contract.
        let conn = unsafe { std::mem::ManuallyDrop::take(&mut self.conn) };
        if let Err(err) = self.pool.checkin(conn) {
            error!(
                error = %err,
                "Failed to return SQLite connection to pool on drop"
            );
        }
    }
}

impl SqlitePool {
    /// Obtains a connection from the pool, creating a new one only if the
    /// total number of connections (idle + checked-out) is below `max_size`.
    ///
    /// Waits up to `checkout_timeout` for a slot instead of blocking
    /// forever; on timeout returns an error carrying pool stats so the
    /// caller can back off or escalate through supervision.
    fn checkout(self: &Arc<Self>) -> Result<PooledConnection, Error> {
        let deadline = Instant::now() + self.checkout_timeout;
        let mut state = self.state.lock().map_err(|e| Error::Store {
            source: None,
            operation: StoreOperation::LockManagerData,
            reason: format!("connection pool mutex poisoned: {}", e),
        })?;

        // Wait until an idle connection is available or we have a free slot.
        while state.available.is_empty() && state.total >= self.max_size {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let (total, idle) = (state.total, state.available.len());
                drop(state);
                warn!(
                    total = total,
                    idle = idle,
                    max_size = self.max_size,
                    timeout_ms = self.checkout_timeout.as_millis(),
                    "SQLite pool exhausted: checkout timed out"
                );
                return Err(Error::Store {
                    source: None,
                    operation: StoreOperation::LockManagerData,
                    reason: format!(
                        "SQLite connection pool exhausted: {} total, {} \
                         idle, max {}; checkout timed out after {:?}",
                        total, idle, self.max_size, self.checkout_timeout
                    ),
                });
            }
            let (guard, _wait) = self
                .condvar
                .wait_timeout(state, remaining)
                .map_err(|e| Error::Store {
                    source: None,
                    operation: StoreOperation::LockManagerData,
                    reason: format!("connection pool condvar poisoned: {}", e),
                })?;
            state = guard;
        }

        if let Some(conn) = state.available.pop() {
            return Ok(PooledConnection {
                conn: std::mem::ManuallyDrop::new(conn),
                pool: self.clone(),
            });
        }

        // We have a slot to create a new connection. The slot (`total`)
        // is reserved under the lock; the I/O itself runs outside it so
        // open latency never blocks other pool users.
        state.total += 1;
        drop(state);

        let conn =
            match open_with_tuning(&self.path, self.durability, self.tuning) {
                Ok(conn) => conn,
                Err(e) => {
                    let mut state =
                        self.state.lock().map_err(|poison| Error::Store {
                            source: None,
                            operation: StoreOperation::LockManagerData,
                            reason: format!(
                                "connection pool mutex poisoned: {}",
                                poison
                            ),
                        })?;
                    state.total -= 1;
                    drop(state);
                    self.condvar.notify_one();
                    return Err(e);
                }
            };

        Ok(PooledConnection {
            conn: std::mem::ManuallyDrop::new(conn),
            pool: self.clone(),
        })
    }

    /// Returns a connection to the idle set.
    ///
    /// Idle connections above `idle_keep` are closed instead of retained
    /// (`total` decremented) so load bursts do not pin a full pool of page
    /// caches afterwards. The `total` discard also covers any future
    /// invariant break; this `Drop` path never panics.
    fn checkin(&self, conn: Connection) -> Result<(), Error> {
        let mut state = self.state.lock().map_err(|poison| Error::Store {
            source: None,
            operation: StoreOperation::LockManagerData,
            reason: format!("connection pool mutex poisoned: {}", poison),
        })?;
        if state.available.len() < self.idle_keep.min(self.max_size) {
            state.available.push(conn);
        } else {
            // Shrink idle above keep, or cover any accounting break: never
            // underflow `total` (this runs in `Drop`, which must not panic).
            state.total = state.total.checked_sub(1).unwrap_or_else(|| {
                error!("SQLite pool accounting underflow on checkin");
                0
            });
        }
        drop(state);
        self.condvar.notify_one();
        Ok(())
    }

    /// Wait until all checked-out connections have been returned, or fail
    /// after `timeout` instead of hanging shutdown forever.
    fn drain(&self, timeout: Duration) -> Result<(), Error> {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().map_err(|e| Error::Store {
            source: None,
            operation: StoreOperation::LockManagerData,
            reason: format!("connection pool mutex poisoned: {}", e),
        })?;
        while state.total != state.available.len() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let (total, idle) = (state.total, state.available.len());
                drop(state);
                error!(
                    total = total,
                    idle = idle,
                    timeout_ms = timeout.as_millis(),
                    "SQLite pool drain timed out with connections still \
                     checked out"
                );
                return Err(Error::Store {
                    source: None,
                    operation: StoreOperation::LockManagerData,
                    reason: format!(
                        "SQLite pool drain timed out after {:?} with {} of \
                         {} connections still checked out",
                        timeout,
                        total - idle,
                        total
                    ),
                });
            }
            let (guard, _wait) = self
                .condvar
                .wait_timeout(state, remaining)
                .map_err(|e| Error::Store {
                    source: None,
                    operation: StoreOperation::LockManagerData,
                    reason: format!("connection pool condvar poisoned: {}", e),
                })?;
            state = guard;
        }
        drop(state);
        Ok(())
    }
}

impl SqliteManager {
    fn validate_identifier(identifier: &str) -> Result<(), Error> {
        let mut chars = identifier.chars();
        let Some(first) = chars.next() else {
            return Err(Error::CreateStore {
                reason: "invalid SQLite identifier: empty".to_owned(),
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
                "invalid SQLite identifier '{identifier}': allowed pattern is [A-Za-z_][A-Za-z0-9_]*"
            ),
        })
    }

    /// Creates a new SQLite database manager.
    /// Opens or creates a SQLite database file at the specified path.
    ///
    /// # Arguments
    ///
    /// * `path` - Directory holding the database file. Unlike
    ///   `RocksDbManager::new` (which opens `path` itself as the database
    ///   directory), the SQLite file is created as `path/database.db`.
    ///   The database file will be named "database.db" within this directory.
    /// * `durability` - when `true`, every write is fsynced
    ///   (`synchronous=FULL`); when `false`, the OS may delay durability
    ///   (`synchronous=NORMAL`, faster, small window of loss on power cut).
    ///
    /// # Returns
    ///
    /// Returns a new SqliteManager instance.
    ///
    /// # Errors
    ///
    /// Returns Error::CreateStore if:
    /// - The directory cannot be created
    /// - The SQLite connection cannot be opened
    ///
    pub fn new(
        path: &Path,
        durability: bool,
        spec: Option<MachineSpec>,
    ) -> Result<Self, Error> {
        info!("Creating SQLite database manager");
        if !path.exists() {
            debug!("Path does not exist, creating it");
            fs::create_dir_all(path).map_err(|e| {
                error!(path = %path.display(), error = %e, "Failed to create SQLite directory");
                Error::CreateStore {
                    reason: format!(
                    "fail SQLite create directory: {}",
                    e
                ),
                }
            })?;
        }

        let db_path = path.join("database.db");

        let spec = resolve_spec(spec).map_err(|e| {
            error!(error = %e, "Invalid machine spec for SQLite manager");
            e
        })?;
        let tuning = tuning_for_ram(spec.ram_mb);
        info!(
            "SQLite tuning: ram_mb={}, cpu_cores={}",
            spec.ram_mb, spec.cpu_cores
        );

        debug!("Opening SQLite connection");
        let conn = open_with_tuning(&db_path, durability, tuning).map_err(|e| {
            error!(path = %db_path.display(), error = %e, "Failed to open SQLite connection");
            Error::CreateStore { reason: format!("fail SQLite open connection: {}", e) }
        })?;

        // Pool size: 1× vCPU, clamped between 4 and 16 — then capped by
        // RAM so the pooled page caches stay within budget. SQLite is
        // single-writer and each connection carries its own page cache,
        // so excess connections hurt more than help.
        let cache_mb_per_conn = (-tuning.cache_size_kb / 1024).max(1) as u64;
        let ram_cap =
            (spec.ram_mb * POOL_RAM_BUDGET_PERCENT / 100 / cache_mb_per_conn)
                .clamp(1, 16);
        let max_size =
            (spec.cpu_cores.clamp(4, 16) as u64).min(ram_cap) as usize;
        info!(
            "SQLite connection pool size: {} (cpu clamp {}, ram cap {} \
             from {} MB host / {} MB per connection)",
            max_size,
            spec.cpu_cores.clamp(4, 16),
            ram_cap,
            spec.ram_mb,
            cache_mb_per_conn
        );

        let pool = Arc::new(SqlitePool {
            path: db_path,
            durability,
            tuning,
            max_size,
            idle_keep: max_size.div_ceil(2).max(1),
            checkout_timeout: DEFAULT_CHECKOUT_TIMEOUT,
            state: Mutex::new(PoolState {
                available: Vec::new(),
                total: 0,
            }),
            condvar: Condvar::new(),
        });

        debug!("SQLite database manager created successfully");
        Ok(Self {
            admin_conn: Arc::new(Mutex::new(conn)),
            pool,
        })
    }
}

impl DbManager<SqliteCollection, SqliteCollection> for SqliteManager {
    fn create_state(
        &self,
        identifier: &str,
        prefix: &str,
    ) -> Result<SqliteCollection, Error> {
        Self::validate_identifier(identifier)?;
        let stmt = format!(
            "CREATE TABLE IF NOT EXISTS {} (prefix TEXT NOT NULL, value \
            BLOB NOT NULL, PRIMARY KEY (prefix))",
            identifier
        );

        {
            let conn = self.admin_conn.lock().map_err(|e| {
                error!(error = %e, "Failed to acquire connection lock for state creation");
                Error::Store {
                source: None,
                    operation: StoreOperation::LockConnection,
                    reason: format!("{}", e),
                }
            })?;

            conn.execute(stmt.as_str(), ()).map_err(|e| {
                error!(table = identifier, error = %e, "Failed to create state table");
                Error::CreateStore { reason: format!("fail SQLite create table: {}", e) }
            })?;
        }

        debug!(table = identifier, prefix = prefix, "State table created");
        SqliteCollection::new(self.clone(), identifier, prefix)
    }

    fn create_collection(
        &self,
        identifier: &str,
        prefix: &str,
    ) -> Result<SqliteCollection, Error> {
        Self::validate_identifier(identifier)?;
        let stmt = format!(
            "CREATE TABLE IF NOT EXISTS {} (prefix TEXT NOT NULL, sn TEXT NOT NULL, value \
            BLOB NOT NULL, PRIMARY KEY (prefix, sn))",
            identifier
        );

        {
            let conn = self.admin_conn.lock().map_err(|e| {
                error!(error = %e, "Failed to acquire connection lock for collection creation");
                Error::Store {
                source: None,
                    operation: StoreOperation::LockConnection,
                    reason: format!("{}", e),
                }
            })?;

            conn.execute(stmt.as_str(), ()).map_err(|e| {
                error!(table = identifier, error = %e, "Failed to create collection table");
                Error::CreateStore { reason: format!("fail SQLite create table: {}", e) }
            })?;
        }

        debug!(
            table = identifier,
            prefix = prefix,
            "Collection table created"
        );
        SqliteCollection::new(self.clone(), identifier, prefix)
    }

    fn batch_writer(&self) -> Option<Box<dyn BatchWrite>> {
        Some(Box::new(SqliteBatchWriter {
            manager: self.clone(),
        }))
    }

    fn stop(self) -> Result<(), Error> {
        debug!("Stopping SQLite manager, draining pool and flushing WAL");
        self.pool.drain(POOL_STOP_TIMEOUT).map_err(|e| {
            error!(error = %e, "Failed to drain connection pool on stop");
            e
        })?;
        let conn = self.admin_conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock on stop");
            Error::Store {
                source: None,
                operation: StoreOperation::LockConnection,
                reason: format!("{}", e),
            }
        })?;
        conn.execute_batch("PRAGMA optimize;").map_err(|e| {
            error!(error = %e, "Failed to optimize on stop");
            Error::Store {
                source: None,
                operation: StoreOperation::WalCheckpoint,
                reason: format!("{}", e),
            }
        })?;
        // Verify the checkpoint instead of assuming it: with another
        // writer holding the WAL (or leftover pool connections), SQLite
        // reports busy=1 and the log is NOT truncated.
        let (busy, log_frames, checkpointed): (i64, i64, i64) = conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE);", (), |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(|e| {
                error!(error = %e, "Failed to checkpoint WAL on stop");
                Error::Store {
                    source: None,
                    operation: StoreOperation::WalCheckpoint,
                    reason: format!("{}", e),
                }
            })?;
        drop(conn);
        if busy != 0 {
            warn!(
                busy = busy,
                log_frames = log_frames,
                checkpointed = checkpointed,
                "WAL checkpoint incomplete at shutdown (another writer \
                 holds the log); data remains durable in the WAL"
            );
        } else {
            debug!("SQLite WAL checkpoint complete");
        }
        Ok(())
    }
}

/// Atomic multi-write handle over [`SqliteManager`]'s connection pool.
///
/// Holds one pooled connection for the whole batch and wraps every op in a
/// single `BEGIN IMMEDIATE .. COMMIT` transaction: either all rows are
/// durable or none are, including across crashes. `ROLLBACK` runs on every
/// error path so the connection never returns to the pool mid-transaction.
#[derive(Clone)]
struct SqliteBatchWriter {
    manager: SqliteManager,
}

impl BatchWrite for SqliteBatchWriter {
    fn write_batch(
        &self,
        prefix: &str,
        ops: &[BatchOp<'_>],
    ) -> Result<(), Error> {
        for op in ops {
            match *op {
                BatchOp::PutEvent { collection, .. } => {
                    SqliteManager::validate_identifier(collection)?;
                }
                BatchOp::PutState { store, .. } => {
                    SqliteManager::validate_identifier(store)?;
                }
            }
        }

        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for batch");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        if ops.is_empty() {
            return Ok(());
        }

        conn.execute_batch("BEGIN IMMEDIATE").map_err(|e| {
            error!(error = %e, "Failed to begin batch transaction");
            Error::Store {
                source: None,
                operation: StoreOperation::ExecuteBatch,
                reason: format!("{}", e),
            }
        })?;

        let result = (|| -> Result<(), Error> {
            for op in ops {
                match *op {
                    BatchOp::PutEvent {
                        collection,
                        key,
                        data,
                    } => {
                        let stmt = format!(
                            "INSERT OR REPLACE INTO {} (prefix, sn, value) \
                             VALUES (?1, ?2, ?3)",
                            collection
                        );
                        conn.execute(&stmt, params![prefix, key, data])
                            .map_err(|e| {
                                error!(
                                    table = collection,
                                    error = %e,
                                    "Failed to put event in batch"
                                );
                                Error::Store {
                                    source: None,
                                    operation: StoreOperation::Insert,
                                    reason: format!("{}", e),
                                }
                            })?;
                    }
                    BatchOp::PutState { store, data } => {
                        let stmt = format!(
                            "INSERT OR REPLACE INTO {} (prefix, value) \
                             VALUES (?1, ?2)",
                            store
                        );
                        conn.execute(&stmt, params![prefix, data]).map_err(
                            |e| {
                                error!(
                                    table = store,
                                    error = %e,
                                    "Failed to put state in batch"
                                );
                                Error::Store {
                                    source: None,
                                    operation: StoreOperation::Insert,
                                    reason: format!("{}", e),
                                }
                            },
                        )?;
                    }
                }
            }
            Ok(())
        })();

        match result {
            Ok(()) => conn.execute_batch("COMMIT").map_err(|e| {
                error!(error = %e, "Failed to commit batch transaction");
                // A failed COMMIT may leave the transaction open: roll
                // back so the connection never returns to the pool dirty
                // (the next checkout would otherwise inherit uncommitted
                // writes or hit "cannot start a transaction within a
                // transaction").
                if let Err(rollback_err) = conn.execute_batch("ROLLBACK") {
                    error!(
                        error = %rollback_err,
                        "Failed to roll back after commit failure"
                    );
                }
                Error::Store {
                    source: None,
                    operation: StoreOperation::ExecuteBatch,
                    reason: format!("{}", e),
                }
            }),
            Err(batch_err) => {
                if let Err(rollback_err) = conn.execute_batch("ROLLBACK") {
                    error!(
                        error = %rollback_err,
                        "Failed to roll back batch transaction"
                    );
                }
                Err(batch_err)
            }
        }
    }
}

/// SQLite collection that implements both Collection and State traits.
/// Stores key-value pairs in a SQLite table with prefix-based namespacing.
///
////// # Schema
///
/// **For Collections**: (prefix TEXT, sn TEXT, value BLOB, PRIMARY KEY (prefix, sn))
/// **For State**: (prefix TEXT, value BLOB, PRIMARY KEY (prefix))
///
/// where:
/// - `prefix` is the actor's namespace identifier
/// - `sn` is the sequence number (for events)
/// - `value` is the serialized data
///
pub struct SqliteCollection {
    /// Reference back to the manager so we can check out a pooled connection
    /// on every operation.
    manager: SqliteManager,
    /// Table name in the database.
    table: String,
    /// Prefix for filtering rows (actor namespace).
    prefix: String,
}

impl SqliteCollection {
    /// Creates a new SQLite collection.
    ///
    /// # Arguments
    ///
    /// * `manager` - The SQLite manager that owns the connection pool.
    /// * `table` - Name of the table in the database.
    /// * `prefix` - Prefix for namespacing this collection's data.
    ///
    /// # Returns
    ///
    /// Returns a new SqliteCollection instance.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CreateStore`] when `table` is not a valid SQLite
    /// identifier. The table name is interpolated into SQL statements, so
    /// unvalidated names would allow SQL injection.
    ///
    pub fn new(
        manager: SqliteManager,
        table: &str,
        prefix: &str,
    ) -> Result<Self, Error> {
        SqliteManager::validate_identifier(table)?;
        Ok(Self {
            manager,
            table: table.to_owned(),
            prefix: prefix.to_owned(),
        })
    }

    /// Create a new iterator filtering by prefix.
    fn make_iter(&self, reverse: bool) -> EntryIterator {
        Box::new(SqliteChunkedIterator::new(
            self.manager.clone(),
            self.table.clone(),
            self.prefix.clone(),
            reverse,
        ))
    }

    fn state_key(&self) -> String {
        self.prefix.clone()
    }

    fn collection_key(&self, key: &str) -> String {
        format!("{}.{}", self.prefix, key)
    }

    fn map_get_error(&self, error: SqliteError, key: String) -> Error {
        match error {
            SqliteError::QueryReturnedNoRows => Error::EntryNotFound { key },
            other => Error::Get {
                key,
                reason: format!("{}", other),
            },
        }
    }
}

/// Chunked iterator over a SQLite collection using keyset pagination.
///
/// This works correctly when `sn` values are zero-padded, so lexicographic
/// order matches numeric order. It fetches `ITER_CHUNK_SIZE` rows per chunk and
/// releases the connection back to the pool between chunks so concurrent
/// operations are not blocked for the entire scan.
struct SqliteChunkedIterator {
    manager: SqliteManager,
    table: String,
    prefix: String,
    reverse: bool,
    /// Rows already fetched for the current chunk and not yet yielded.
    buffer: VecDeque<(String, Vec<u8>)>,
    /// Last `sn` seen, reused as the cursor for the next chunk.
    last_key: Option<String>,
    /// Set when the last query returned 0 rows and there is no more data.
    exhausted: bool,
}

impl SqliteChunkedIterator {
    const fn new(
        manager: SqliteManager,
        table: String,
        prefix: String,
        reverse: bool,
    ) -> Self {
        Self {
            manager,
            table,
            prefix,
            reverse,
            buffer: VecDeque::new(),
            last_key: None,
            exhausted: false,
        }
    }

    fn fetch_chunk(&mut self) -> Result<(), Error> {
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(table = %self.table, error = %e, "Failed to check out connection for chunk fetch");
            Error::Store {
                source: None,
                operation: StoreOperation::LockConnection,
                reason: format!("{}", e),
            }
        })?;

        let order = if self.reverse { "DESC" } else { "ASC" };
        let cmp = if self.reverse { "<" } else { ">" };

        let rows: Vec<(String, Vec<u8>)> = match &self.last_key {
            None => {
                let q = format!(
                    "SELECT sn, value FROM {} WHERE prefix = ?1 ORDER BY sn {} LIMIT {}",
                    self.table, order, ITER_CHUNK_SIZE
                );
                conn.prepare_cached(&q).and_then(|mut s| {
                    s.query_map(params![self.prefix], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                    .and_then(|rows| rows.collect())
                })
                .map_err(|e| {
                    error!(table = %self.table, error = %e, "Failed to fetch first chunk from DB");
                    Error::Get {
                        key: self.prefix.clone(),
                        reason: format!("{}", e),
                    }
                })?
            }
            Some(last) => {
                let q = format!(
                    "SELECT sn, value FROM {} WHERE prefix = ?1 AND sn {} ?2 ORDER BY sn {} LIMIT {}",
                    self.table, cmp, order, ITER_CHUNK_SIZE
                );
                let last = last.clone();
                conn.prepare_cached(&q).and_then(|mut s| {
                    s.query_map(params![self.prefix, last], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                    .and_then(|rows| rows.collect())
                })
                .map_err(|e| {
                    error!(table = %self.table, error = %e, "Failed to fetch next chunk from DB");
                    Error::Get {
                        key: self.prefix.clone(),
                        reason: format!("{}", e),
                    }
                })?
            }
        };

        if rows.is_empty() {
            self.exhausted = true;
        } else {
            self.last_key = rows.last().map(|(k, _)| k.clone());
            self.buffer.extend(rows);
        }
        Ok(())
    }
}

impl Iterator for SqliteChunkedIterator {
    type Item = Result<(String, Vec<u8>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty()
            && !self.exhausted
            && let Err(error) = self.fetch_chunk()
        {
            self.exhausted = true;
            return Some(Err(error));
        }

        self.buffer.pop_front().map(Ok)
    }
}

/// Chunked iterator bounded by an inclusive `[start, end]` key range.
struct SqliteRangeChunkedIterator {
    manager: SqliteManager,
    table: String,
    prefix: String,
    start: String,
    end: String,
    reverse: bool,
    buffer: VecDeque<(String, Vec<u8>)>,
    last_key: Option<String>,
    exhausted: bool,
}

impl SqliteRangeChunkedIterator {
    const fn new(
        manager: SqliteManager,
        table: String,
        prefix: String,
        start: String,
        end: String,
        reverse: bool,
    ) -> Self {
        Self {
            manager,
            table,
            prefix,
            start,
            end,
            reverse,
            buffer: VecDeque::new(),
            last_key: None,
            exhausted: false,
        }
    }

    fn fetch_chunk(&mut self) -> Result<(), Error> {
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(table = %self.table, error = %e, "Failed to check out connection for range chunk fetch");
            Error::Store {
                source: None,
                operation: StoreOperation::LockConnection,
                reason: format!("{}", e),
            }
        })?;

        let order = if self.reverse { "DESC" } else { "ASC" };
        let cmp = if self.reverse { "<" } else { ">" };

        let rows: Vec<(String, Vec<u8>)> = match &self.last_key {
            None => {
                let q = format!(
                    "SELECT sn, value FROM {} WHERE prefix = ?1 AND sn >= ?2 AND sn <= ?3 ORDER BY sn {} LIMIT {}",
                    self.table, order, ITER_CHUNK_SIZE
                );
                conn.prepare_cached(&q).and_then(|mut s| {
                    s.query_map(
                        params![self.prefix, self.start, self.end],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .and_then(|rows| rows.collect())
                })
                .map_err(|e| {
                    error!(table = %self.table, error = %e, "Failed to fetch first range chunk from DB");
                    Error::Get {
                        key: self.prefix.clone(),
                        reason: format!("{}", e),
                    }
                })?
            }
            Some(last) => {
                let q = format!(
                    "SELECT sn, value FROM {} WHERE prefix = ?1 AND sn >= ?2 AND sn <= ?3 AND sn {} ?4 ORDER BY sn {} LIMIT {}",
                    self.table, cmp, order, ITER_CHUNK_SIZE
                );
                let last = last.clone();
                conn.prepare_cached(&q).and_then(|mut s| {
                    s.query_map(
                        params![self.prefix, self.start, self.end, last],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .and_then(|rows| rows.collect())
                })
                .map_err(|e| {
                    error!(table = %self.table, error = %e, "Failed to fetch next range chunk from DB");
                    Error::Get {
                        key: self.prefix.clone(),
                        reason: format!("{}", e),
                    }
                })?
            }
        };

        if rows.is_empty() {
            self.exhausted = true;
        } else {
            self.last_key = rows.last().map(|(k, _)| k.clone());
            self.buffer.extend(rows);
        }
        Ok(())
    }
}

impl Iterator for SqliteRangeChunkedIterator {
    type Item = Result<(String, Vec<u8>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty()
            && !self.exhausted
            && let Err(error) = self.fetch_chunk()
        {
            self.exhausted = true;
            return Some(Err(error));
        }

        self.buffer.pop_front().map(Ok)
    }
}

impl State for SqliteCollection {
    fn get(&self) -> Result<Vec<u8>, Error> {
        let query =
            format!("SELECT value FROM {} WHERE prefix = ?1", self.table);
        let key = self.state_key();
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for state get");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        let row: Vec<u8> = conn
            .prepare_cached(&query)
            .map_err(|e| self.map_get_error(e, key.clone()))?
            .query_row(params![self.prefix], |row| row.get(0))
            .map_err(|e| self.map_get_error(e, key))?;

        Ok(row)
    }

    fn put(&mut self, data: &[u8]) -> Result<(), Error> {
        let stmt = format!(
            "INSERT OR REPLACE INTO {} (prefix, value) VALUES (?1, ?2)",
            self.table
        );
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for state put");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        conn.prepare_cached(&stmt)
            .map_err(|e| {
                error!(table = %self.table, error = %e, "Failed to prepare state put");
                Error::Store {
                    source: None,
                    operation: StoreOperation::Insert,
                    reason: format!("{}", e),
                }
            })?
            .execute(params![self.prefix, data])
            .map_err(|e| {
                error!(table = %self.table, error = %e, "Failed to put state");
                Error::Store {
                    source: None,
                    operation: StoreOperation::Insert,
                    reason: format!("{}", e),
                }
            })?;
        Ok(())
    }

    fn del(&mut self) -> Result<(), Error> {
        let stmt = format!("DELETE FROM {} WHERE prefix = ?1", self.table);
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for state delete");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        let affected_rows = conn
            .execute(&stmt, params![self.prefix,])
            .map_err(|e| {
                error!(table = %self.table, error = %e, "Failed to delete state");
                Error::Store {
                source: None,
                    operation: StoreOperation::Delete,
                    reason: format!("{}", e),
                }
            })?;

        if affected_rows == 0 {
            return Err(Error::EntryNotFound {
                key: self.state_key(),
            });
        }
        Ok(())
    }

    fn purge(&mut self) -> Result<(), Error> {
        let stmt = format!("DELETE FROM {} WHERE prefix = ?1", self.table);
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for state purge");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        conn.execute(&stmt, params![self.prefix]).map_err(|e| {
            error!(table = %self.table, error = %e, "Failed to purge state");
            Error::Store {
                source: None,
                operation: StoreOperation::Purge,
                reason: format!("{}", e),
            }
        })?;
        debug!(table = %self.table, "State purged");
        Ok(())
    }

    fn name(&self) -> &str {
        self.table.as_str()
    }
}

impl Collection for SqliteCollection {
    fn get(&self, key: &str) -> Result<Vec<u8>, Error> {
        let query = format!(
            "SELECT value FROM {} WHERE prefix = ?1 AND sn = ?2",
            self.table
        );
        let collection_key = self.collection_key(key);
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for collection get");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        let row: Vec<u8> = conn
            .prepare_cached(&query)
            .map_err(|e| self.map_get_error(e, collection_key.clone()))?
            .query_row(params![self.prefix, key], |row| row.get(0))
            .map_err(|e| self.map_get_error(e, collection_key))?;

        Ok(row)
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), Error> {
        let stmt = format!(
            "INSERT OR REPLACE INTO {} (prefix, sn, value) VALUES (?1, ?2, ?3)",
            self.table
        );
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for collection put");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        conn.prepare_cached(&stmt)
            .map_err(|e| {
                error!(table = %self.table, key = key, error = %e, "Failed to prepare collection put");
                Error::Store {
                source: None,
                    operation: StoreOperation::Insert,
                    reason: format!("{}", e),
                }
            })?
            .execute(params![self.prefix, key, data])
            .map_err(|e| {
                error!(table = %self.table, key = key, error = %e, "Failed to put collection entry");
                Error::Store {
                source: None,
                    operation: StoreOperation::Insert,
                    reason: format!("{}", e),
                }
            })?;
        Ok(())
    }

    fn del(&mut self, key: &str) -> Result<(), Error> {
        let stmt =
            format!("DELETE FROM {} WHERE prefix = ?1 AND sn = ?2", self.table);
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for collection delete");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        let affected_rows = conn
            .prepare_cached(&stmt)
            .map_err(|e| {
                error!(table = %self.table, key = key, error = %e, "Failed to prepare collection delete");
                Error::Store {
                source: None,
                    operation: StoreOperation::Delete,
                    reason: format!("{}", e),
                }
            })?
            .execute(params![self.prefix, key])
            .map_err(|e| {
                error!(table = %self.table, key = key, error = %e, "Failed to delete collection entry");
                Error::Store {
                source: None,
                    operation: StoreOperation::Delete,
                    reason: format!("{}", e),
                }
            })?;

        if affected_rows == 0 {
            return Err(Error::EntryNotFound {
                key: self.collection_key(key),
            });
        }
        Ok(())
    }

    fn purge(&mut self) -> Result<(), Error> {
        let stmt = format!("DELETE FROM {} WHERE prefix = ?1", self.table);
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for collection purge");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        conn.execute(&stmt, params![self.prefix])
            .map_err(|e| {
                error!(table = %self.table, error = %e, "Failed to purge collection");
                Error::Store {
                source: None,
                    operation: StoreOperation::Purge,
                    reason: format!("{}", e),
                }
            })?;
        debug!(table = %self.table, "Collection purged");
        Ok(())
    }

    fn last(&self) -> Result<Option<(String, Vec<u8>)>, Error> {
        // Single-row lookup: the previous `iter(true).next()` fetched a
        // 1000-row chunk to return one entry, on the hot recovery path.
        let query = format!(
            "SELECT sn, value FROM {} WHERE prefix = ?1 ORDER BY sn DESC \
             LIMIT 1",
            self.table
        );
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for collection last");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        let mut stmt = conn.prepare_cached(&query).map_err(|e| {
            error!(table = %self.table, error = %e, "Failed to prepare last query");
            Error::Store {
                source: None,
                operation: StoreOperation::GetLatestEvents,
                reason: format!("{}", e),
            }
        })?;
        match stmt.query_row(params![self.prefix], |row| {
            Ok((row.get(0)?, row.get(1)?))
        }) {
            Ok(entry) => Ok(Some(entry)),
            Err(SqliteError::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(Error::Store {
                source: None,
                operation: StoreOperation::GetLatestEvents,
                reason: format!("{}", e),
            }),
        }
    }

    fn iter<'a>(
        &'a self,
        reverse: bool,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(String, Vec<u8>), Error>> + 'a>,
        Error,
    > {
        Ok(self.make_iter(reverse))
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
        Ok(Box::new(SqliteRangeChunkedIterator::new(
            self.manager.clone(),
            self.table.clone(),
            self.prefix.clone(),
            start.to_owned(),
            end.to_owned(),
            reverse,
        )))
    }

    fn get_by_range(
        &self,
        from: Option<&str>,
        quantity: isize,
    ) -> Result<Vec<Vec<u8>>, Error> {
        // Native keyset pagination: same contract as the default
        // implementation (`from` exclusive, `quantity` signed for
        // direction) without scanning from the start of the log.
        let limit = quantity.unsigned_abs().min(i64::MAX as usize) as i64;
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for collection range");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        if let Some(key) = from {
            let exists: Option<String> = conn
                .query_row(
                    &format!(
                        "SELECT sn FROM {} WHERE prefix = ?1 AND sn = ?2",
                        self.table
                    ),
                    params![self.prefix, key],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    error!(table = %self.table, error = %e, "Failed to locate range start");
                    Error::Store {
                        source: None,
                        operation: StoreOperation::GetEventsRange,
                        reason: format!("{}", e),
                    }
                })?;
            if exists.is_none() {
                return Err(Error::EntryNotFound {
                    key: self.collection_key(key),
                });
            }
        }

        let from_key: Option<&str> = from;
        let (cmp, order) = match (from_key, quantity >= 0) {
            (Some(_), true) => ("sn > ?2", "ASC"),
            (Some(_), false) => ("sn < ?2", "DESC"),
            (None, true) => ("sn >= char(0)", "ASC"),
            (None, false) => ("sn >= char(0)", "DESC"),
        };
        // `from` is exclusive; without `from` the tautology keeps one
        // query shape for all four cases.
        let query = format!(
            "SELECT value FROM {} WHERE prefix = ?1 AND {} ORDER BY sn {} \
             LIMIT ?3",
            self.table, cmp, order
        );
        let mut stmt = conn.prepare_cached(&query).map_err(|e| {
            error!(table = %self.table, error = %e, "Failed to prepare range query");
            Error::Store {
                source: None,
                operation: StoreOperation::GetEventsRange,
                reason: format!("{}", e),
            }
        })?;
        match from_key {
            // `params!` temporaries live to the end of each arm, so the
            // query executes inside the arm that builds them.
            Some(key) => stmt
                .query_map(params![self.prefix, key, limit], |row| row.get(0))
                .and_then(|rows| rows.collect()),
            None => stmt
                .query_map(params![self.prefix, "", limit], |row| row.get(0))
                .and_then(|rows| rows.collect()),
        }
        .map_err(|e| {
            error!(table = %self.table, error = %e, "Failed to fetch range");
            Error::Store {
                source: None,
                operation: StoreOperation::GetEventsRange,
                reason: format!("{}", e),
            }
        })
    }

    fn del_range(&mut self, start: &str, end: &str) -> Result<(), Error> {
        let stmt = format!(
            "DELETE FROM {} WHERE prefix = ?1 AND sn >= ?2 AND sn <= ?3",
            self.table
        );
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for collection del_range");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        conn.execute(&stmt, params![self.prefix, start, end])
            .map_err(|e| {
                error!(table = %self.table, start = %start, end = %end, error = %e, "Failed to delete collection range");
                Error::Store {
                source: None,
                    operation: StoreOperation::Delete,
                    reason: format!("{}", e),
                }
            })?;
        Ok(())
    }

    fn name(&self) -> &str {
        self.table.as_str()
    }
}

fn open_with_tuning<P: AsRef<Path>>(
    path: P,
    durability: bool,
    tuning: SqliteTuning,
) -> Result<Connection, Error> {
    let path = path.as_ref();
    debug!(path = %path.display(), "Opening SQLite database");
    let flags =
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    let conn = Connection::open_with_flags(path, flags).map_err(|e| {
        error!(path = %path.display(), error = %e, "Failed to open SQLite database");
        Error::Store {
                source: None,
            operation: StoreOperation::OpenConnection,
            reason: format!("{}", e),
        }
    })?;

    // Set the busy handler BEFORE any statement: the very first PRAGMA
    // (`journal_mode=WAL`) already takes file locks, and concurrent pool
    // growth opens several connections at once. Without this, losers fail
    // with "database is locked" instead of waiting.
    conn.busy_timeout(std::time::Duration::from_millis(5000))
        .map_err(|e| {
            error!(error = %e, "Failed to set SQLite busy timeout");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

    // Setup is idempotent (PRAGMAs included), so transient lock contention
    // escaping the busy handler (WAL creation, ANALYZE) is retried.
    let mut attempt = 0u32;
    loop {
        match apply_pragmas(&conn, durability, tuning) {
            Ok(()) => {
                debug!("SQLite database opened and configured successfully");
                return Ok(conn);
            }
            Err(e) if is_transient_lock(&e) && attempt < 10 => {
                attempt += 1;
                warn!(
                    attempt = attempt,
                    error = %e,
                    "Retrying SQLite setup after lock contention"
                );
                std::thread::sleep(Duration::from_millis(50 * attempt as u64));
            }
            Err(e) => {
                error!(error = %e, "Failed to execute SQLite PRAGMA statements");
                return Err(Error::Store {
                    source: None,
                    operation: StoreOperation::ExecuteBatch,
                    reason: format!("{}", e),
                });
            }
        }
    }
}

/// Returns `true` for transient SQLite lock-contention errors worth
/// retrying during setup.
fn is_transient_lock(e: &SqliteError) -> bool {
    use rusqlite::ffi::ErrorCode;
    matches!(
        e,
        SqliteError::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::DatabaseBusy,
                ..
            },
            _,
        ) | SqliteError::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::DatabaseLocked,
                ..
            },
            _,
        )
    )
}

fn apply_pragmas(
    conn: &Connection,
    durability: bool,
    tuning: SqliteTuning,
) -> Result<(), SqliteError> {
    let sync_mode = if durability { "FULL" } else { "NORMAL" };

    conn.execute_batch(
        format!(
            "
            PRAGMA journal_mode=WAL;
            PRAGMA busy_timeout=5000;
            PRAGMA synchronous={};
            PRAGMA wal_autocheckpoint={};       -- pages
            PRAGMA journal_size_limit={};       -- bytes
            PRAGMA temp_store=MEMORY;
            PRAGMA cache_size={};               -- negative = KB
            PRAGMA mmap_size={};                -- bytes
            PRAGMA optimize=0x10002;            -- analyze + run on open (cheap)
            ",
            sync_mode,
            tuning.wal_autocheckpoint_pages,
            tuning.journal_size_limit_bytes,
            tuning.cache_size_kb,
            tuning.mmap_size_bytes,
        )
        .as_str(),
    )?;

    debug!("SQLite PRAGMAs applied successfully");
    Ok(())
}

/// Compute SQLite tuning parameters from available RAM.
///
/// SQLite is single-writer so CPU cores don't affect tuning here.
/// Designed for a shared Docker container with 3 co-located SQLite instances
/// plus a libp2p process — total DB cache footprint stays at ~6 % of host RAM.
fn tuning_for_ram(ram_mb: u64) -> SqliteTuning {
    // Cache: 2 % of RAM, floor 8 MB, cap 1 GB.
    let cache_mb = (ram_mb * 2 / 100).clamp(8, 1024);
    let cache_size_kb = -(cache_mb as i64 * 1024); // negative = KB in SQLite

    // mmap: half of cache, hard cap 128 MB.
    // Supplements the page cache for sequential reads; kept below cache to
    // avoid doubling memory pressure in a shared container.
    let mmap_size_bytes = (cache_mb as i64 / 2).min(128) * 1024 * 1024;

    // WAL checkpoint: fire when WAL ≈ cache/2.
    // pages = (cache_mb/2 MB) / (4 KB/page) = cache_mb * 128.
    // Floor 1000 (SQLite default, prevents thrashing on tiny RAM).
    // Cap 8000 (32 MB WAL max, bounds checkpoint stall under write bursts).
    let wal_autocheckpoint_pages = (cache_mb as i64 * 128).clamp(1_000, 8_000);

    // journal_size_limit: 3× the WAL ceiling — a safety net never reached in
    // normal operation (checkpoints fire first); prevents runaway WAL growth
    // if a checkpoint is delayed. Cap 256 MB to bound disk use in Docker.
    let journal_size_limit_bytes = (wal_autocheckpoint_pages * 4096 * 3)
        .clamp(32 * 1024 * 1024, 256 * 1024 * 1024);

    SqliteTuning {
        wal_autocheckpoint_pages,
        journal_size_limit_bytes,
        cache_size_kb,
        mmap_size_bytes,
    }
}

#[derive(Clone, Copy)]
struct SqliteTuning {
    wal_autocheckpoint_pages: i64,
    journal_size_limit_bytes: i64,
    cache_size_kb: i64,
    mmap_size_bytes: i64,
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    /// Retains every `TempDir` created during tests so they are cleaned up
    /// automatically when the test process exits.
    static TEMP_DIRS: Mutex<Vec<tempfile::TempDir>> = Mutex::new(Vec::new());

    pub fn create_temp_dir() -> String {
        let dir =
            tempfile::tempdir().expect("Can not create temporal directory.");
        let path = dir.path().to_str().unwrap().to_owned();
        TEMP_DIRS.lock().unwrap().push(dir);
        path
    }

    impl Default for SqliteManager {
        fn default() -> Self {
            let path = PathBuf::from(create_temp_dir());
            Self::new(&path, false, None).expect("Cannot create the database")
        }
    }

    use super::*;
    use ave_actors_store::{
        database::{Collection, DbManager},
        test_store_trait,
    };

    test_store_trait! {
        unit_test_sqlite_manager:SqliteManager:SqliteCollection
    }

    #[test]
    fn test_open_with_tuning_bad_path() {
        let result = open_with_tuning(
            "/dev/null/invalid_sqlite_path",
            false,
            tuning_for_ram(1024),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_checkout_open_failure() {
        let pool = Arc::new(SqlitePool {
            path: PathBuf::from("/dev/null/invalid_sqlite_path"),
            durability: false,
            tuning: tuning_for_ram(1024),
            max_size: 1,
            idle_keep: 1,
            checkout_timeout: Duration::from_secs(5),
            state: Mutex::new(PoolState {
                available: Vec::new(),
                total: 0,
            }),
            condvar: Condvar::new(),
        });

        let result = pool.checkout();
        assert!(result.is_err());
    }

    #[test]
    fn test_operations_with_broken_pool() {
        let valid_path = PathBuf::from(create_temp_dir()).join("database.db");
        let admin_conn =
            open_with_tuning(&valid_path, false, tuning_for_ram(1024)).unwrap();

        let pool = Arc::new(SqlitePool {
            path: PathBuf::from("/dev/null/invalid_sqlite_path"),
            durability: false,
            tuning: tuning_for_ram(1024),
            max_size: 1,
            idle_keep: 1,
            checkout_timeout: Duration::from_secs(5),
            state: Mutex::new(PoolState {
                available: Vec::new(),
                total: 0,
            }),
            condvar: Condvar::new(),
        });

        let manager = SqliteManager {
            admin_conn: Arc::new(Mutex::new(admin_conn)),
            pool,
        };

        let mut collection =
            SqliteCollection::new(manager.clone(), "test", "test").unwrap();

        assert!(Collection::get(&collection, "key").is_err());
        assert!(Collection::put(&mut collection, "key", b"val").is_err());
        assert!(Collection::del(&mut collection, "key").is_err());
        assert!(Collection::purge(&mut collection).is_err());

        // `iter`, `iter_range` and `last` are lazy: they only touch the pool
        // when the iterator is consumed.  `iter()` and `iter_range()`
        // themselves succeed even with a broken pool.
        {
            let mut iter = collection.iter(false).unwrap();
            assert!(iter.next().unwrap().is_err());
        }

        {
            let mut iter = collection.iter_range("a", "z", false).unwrap();
            assert!(iter.next().unwrap().is_err());
        }

        assert!(Collection::del_range(&mut collection, "a", "z").is_err());

        let mut state =
            SqliteCollection::new(manager, "state", "test").unwrap();
        assert!(State::get(&state).is_err());
        assert!(State::put(&mut state, b"val").is_err());
        assert!(State::del(&mut state).is_err());
        assert!(State::purge(&mut state).is_err());
    }

    #[test]
    fn test_new_create_dir_failure() {
        // Use a read-only system path where directory creation will fail.
        let result = SqliteManager::new(
            &PathBuf::from("/sys/invalid_sqlite_dir"),
            false,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_new_open_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("is_a_dir");
        fs::create_dir(&db_path).unwrap();
        // Make database.db a directory so the connection open fails.
        fs::create_dir(db_path.join("database.db")).unwrap();

        let result = SqliteManager::new(&db_path, false, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_with_tuning_readonly_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("readonly.db");
        fs::write(&db_path, b"").unwrap();

        let mut perms = fs::metadata(&db_path).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&db_path, perms).unwrap();

        let result = open_with_tuning(&db_path, false, tuning_for_ram(1024));
        assert!(result.is_err());
    }

    #[test]
    fn test_admin_conn_read_only() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("readonly_admin");
        // Create a valid database file first.
        {
            let _ = SqliteManager::new(&db_path, false, None).unwrap();
        }

        let db_file = db_path.join("database.db");
        let flags = OpenFlags::SQLITE_OPEN_READ_ONLY;
        let admin_conn = Connection::open_with_flags(&db_file, flags).unwrap();

        let pool = Arc::new(SqlitePool {
            path: db_file,
            durability: false,
            tuning: tuning_for_ram(1024),
            max_size: 1,
            idle_keep: 1,
            checkout_timeout: Duration::from_secs(5),
            state: Mutex::new(PoolState {
                available: Vec::new(),
                total: 0,
            }),
            condvar: Condvar::new(),
        });

        let manager = SqliteManager {
            admin_conn: Arc::new(Mutex::new(admin_conn)),
            pool,
        };

        assert!(manager.create_state("test", "test").is_err());
        assert!(manager.create_collection("test", "test").is_err());
        // `stop()` may succeed on a read-only connection because the
        // PRAGMAs used are non-mutating or no-ops.
        let _ = manager.stop();
    }

    #[test]
    fn test_open_with_tuning_corrupt_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("corrupt.db");
        fs::write(&db_path, b"THIS IS NOT A SQLITE DB").unwrap();

        let result = open_with_tuning(&db_path, false, tuning_for_ram(1024));
        assert!(result.is_err());
    }

    #[test]
    fn test_pool_saturation_backpressure() {
        use std::sync::mpsc;
        use std::time::Duration;

        fn test_pool(timeout: Duration) -> Arc<SqlitePool> {
            let db_path = PathBuf::from(create_temp_dir()).join("database.db");
            Arc::new(SqlitePool {
                path: db_path,
                durability: false,
                tuning: tuning_for_ram(1024),
                max_size: 1,
                idle_keep: 1,
                checkout_timeout: timeout,
                state: Mutex::new(PoolState {
                    available: Vec::new(),
                    total: 0,
                }),
                condvar: Condvar::new(),
            })
        }

        // Phase A: a saturated pool fails after the timeout instead of
        // parking the caller forever.
        let pool = test_pool(Duration::from_millis(100));
        let _conn1 = pool.checkout().unwrap();
        let start = Instant::now();
        let result = pool.checkout();
        let elapsed = start.elapsed();
        assert!(result.is_err(), "saturated checkout must time out");
        assert!(
            elapsed < Duration::from_secs(2),
            "checkout must not block unboundedly, took {:?}",
            elapsed
        );

        // Phase B: returning the connection unblocks a waiting checkout.
        let pool = test_pool(Duration::from_secs(5));
        let conn1 = pool.checkout().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let pool2 = Arc::clone(&pool);
        let handle = std::thread::spawn(move || {
            let _ = started_tx.send(());
            let conn2 = pool2.checkout().unwrap();
            drop(conn2);
            let _ = done_tx.send(());
        });

        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("worker thread did not start");
        // Give the worker a chance to reach `checkout` so the return
        // below unblocks a genuinely waiting checkout.
        std::thread::sleep(Duration::from_millis(50));
        drop(conn1);
        done_rx.recv_timeout(Duration::from_secs(5)).expect(
            "second checkout was not unblocked after returning the connection",
        );
        handle.join().unwrap();
    }

    #[test]
    fn test_pool_drain_times_out_with_checked_out_connection() {
        use std::time::Duration;

        let db_path = PathBuf::from(create_temp_dir()).join("database.db");
        let pool = Arc::new(SqlitePool {
            path: db_path,
            durability: false,
            tuning: tuning_for_ram(1024),
            max_size: 1,
            idle_keep: 1,
            checkout_timeout: Duration::from_secs(5),
            state: Mutex::new(PoolState {
                available: Vec::new(),
                total: 0,
            }),
            condvar: Condvar::new(),
        });

        let _held = pool.checkout().unwrap();
        let start = Instant::now();
        let result = pool.drain(Duration::from_millis(100));
        assert!(result.is_err(), "drain with a held connection must fail");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "drain must not hang forever"
        );

        drop(_held);
        assert!(pool.drain(Duration::from_secs(5)).is_ok());
    }

    #[test]
    fn test_pool_shrinks_idle_above_keep() {
        use std::time::Duration;

        // max_size 4, idle_keep 1: burst to 4 concurrent checkouts, then
        // return all; only 1 idle connection may be retained.
        let db_path = PathBuf::from(create_temp_dir()).join("database.db");
        let pool = Arc::new(SqlitePool {
            path: db_path,
            durability: false,
            tuning: tuning_for_ram(1024),
            max_size: 4,
            idle_keep: 1,
            checkout_timeout: Duration::from_secs(5),
            state: Mutex::new(PoolState {
                available: Vec::new(),
                total: 0,
            }),
            condvar: Condvar::new(),
        });

        let held: Vec<_> = (0..4).map(|_| pool.checkout().unwrap()).collect();
        drop(held);

        let state = pool.state.lock().unwrap();
        assert_eq!(state.total, 1, "idle pool must shrink to idle_keep");
        assert_eq!(state.available.len(), 1);
    }

    #[test]
    fn test_pool_max_size_capped_by_ram() {
        // 128 MB host, 8 MB floor per connection: 6% = ~7 MB keeps a
        // single connection; the CPU clamp alone would allow 4+.
        let spec = ave_actors_store::config::MachineSpec::Custom {
            ram_mb: 128,
            cpu_cores: 8,
        };
        let temp = create_temp_dir();
        let manager =
            SqliteManager::new(&PathBuf::from(temp), false, Some(spec))
                .unwrap();
        let state = manager.pool.state.lock().unwrap();
        assert_eq!(state.total, 0);
        drop(state);
        assert_eq!(manager.pool.max_size, 1);
        assert_eq!(manager.pool.idle_keep, 1);
    }

    #[test]
    fn test_batch_applies_event_snapshot_and_metadata_atomically() {
        use ave_actors_store::database::BatchOp;

        let manager = SqliteManager::default();
        manager.create_collection("batch_events", "p").unwrap();
        manager.create_state("batch_states", "p").unwrap();
        manager.create_state("batch_metadata", "p").unwrap();

        let writer = manager.batch_writer().expect("sqlite supports batch");
        writer
            .write_batch(
                "p",
                &[
                    BatchOp::PutEvent {
                        collection: "batch_events",
                        key: "00000000000000000000",
                        data: b"event",
                    },
                    BatchOp::PutState {
                        store: "batch_states",
                        data: b"snapshot",
                    },
                    BatchOp::PutState {
                        store: "batch_metadata",
                        data: b"metadata",
                    },
                ],
            )
            .unwrap();

        let events = manager.create_collection("batch_events", "p").unwrap();
        assert_eq!(
            Collection::get(&events, "00000000000000000000").unwrap(),
            b"event".to_vec()
        );
        let states = manager.create_state("batch_states", "p").unwrap();
        assert_eq!(State::get(&states).unwrap(), b"snapshot".to_vec());
        let metadata = manager.create_state("batch_metadata", "p").unwrap();
        assert_eq!(State::get(&metadata).unwrap(), b"metadata".to_vec());
    }

    #[test]
    fn test_batch_rolls_back_when_second_op_fails() {
        use ave_actors_store::database::BatchOp;

        let manager = SqliteManager::default();
        manager.create_collection("rb_events", "p").unwrap();
        // "ghost_states" passes identifier validation but the table does
        // not exist, so the second op fails mid-transaction.
        let writer = manager.batch_writer().expect("sqlite supports batch");
        let result = writer.write_batch(
            "p",
            &[
                BatchOp::PutEvent {
                    collection: "rb_events",
                    key: "00000000000000000000",
                    data: b"event",
                },
                BatchOp::PutState {
                    store: "ghost_states",
                    data: b"snapshot",
                },
            ],
        );
        assert!(result.is_err(), "batch with a failing op must fail");

        // The first op must have been rolled back: all-or-nothing.
        let events = manager.create_collection("rb_events", "p").unwrap();
        assert!(
            Collection::get(&events, "00000000000000000000").is_err(),
            "rolled-back event must not be visible"
        );
    }
}

#[cfg(test)]
mod batch_extra_tests {
    use super::*;

    #[test]
    fn test_collection_new_rejects_malicious_table() {
        let manager = SqliteManager::default();
        let result = SqliteCollection::new(
            manager,
            "t; DROP TABLE batch_events; --",
            "p",
        );
        assert!(
            result.is_err(),
            "table names must be validated at construction"
        );
    }

    #[test]
    fn test_batch_empty_is_noop() {
        let manager = SqliteManager::default();
        let writer = manager.batch_writer().expect("sqlite supports batch");
        assert!(writer.write_batch("p", &[]).is_ok());
    }

    #[test]
    fn test_last_on_large_table_returns_last_key() {
        let manager = SqliteManager::default();
        let mut events = manager.create_collection("big", "p").unwrap();
        for i in 0..1500u64 {
            Collection::put(&mut events, &format!("{:020}", i), b"v").unwrap();
        }
        let (key, _) = Collection::last(&events)
            .unwrap()
            .expect("table is not empty");
        assert_eq!(key, format!("{:020}", 1499u64));
    }

    #[test]
    fn test_get_by_range_matches_default_semantics() {
        let manager = SqliteManager::default();
        let mut events = manager.create_collection("rng", "p").unwrap();
        for i in 0..10u64 {
            Collection::put(&mut events, &format!("{:020}", i), &[i as u8])
                .unwrap();
        }

        // Forward from exclusive key.
        let values =
            Collection::get_by_range(&events, Some(&format!("{:020}", 2)), 3)
                .unwrap();
        assert_eq!(values, vec![vec![3u8], vec![4u8], vec![5u8]]);

        // Reverse from exclusive key.
        let values =
            Collection::get_by_range(&events, Some(&format!("{:020}", 7)), -2)
                .unwrap();
        assert_eq!(values, vec![vec![6u8], vec![5u8]]);

        // Missing `from` is an error, like the default implementation.
        assert!(Collection::get_by_range(&events, Some("nope"), 3).is_err());

        // Unbounded reverse.
        let values = Collection::get_by_range(&events, None, -2).unwrap();
        assert_eq!(values, vec![vec![9u8], vec![8u8]]);
    }

    #[test]
    fn test_concurrent_writers_all_succeed() {
        let manager = SqliteManager::default();
        manager.create_collection("conc", "p").unwrap();

        let handles: Vec<_> = (0..8u64)
            .map(|t| {
                let manager = manager.clone();
                std::thread::spawn(move || {
                    let mut events =
                        manager.create_collection("conc", "p").unwrap();
                    for i in 0..25u64 {
                        let key = format!("{:020}", t * 25 + i);
                        Collection::put(&mut events, &key, b"v").unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("writer thread panicked");
        }

        let events = manager.create_collection("conc", "p").unwrap();
        let (last_key, _) =
            Collection::last(&events).unwrap().expect("rows expected");
        assert_eq!(last_key, format!("{:020}", 8u64 * 25 - 1));
    }
}
