//! # SQLite database backend.
//!
//! This module contains the SQLite database backend implementation.
//!

use ave_actors_store::{
    Error, StoreOperation,
    config::{MachineSpec, resolve_spec},
    database::{Collection, DbManager, State},
};

use rusqlite::{Connection, Error as SqliteError, OpenFlags, params};
use tracing::{debug, error, info};

use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
};
use std::{fs, path::Path};

type EntryIterator = Box<dyn Iterator<Item = Result<(String, Vec<u8>), Error>>>;
const ITER_CHUNK_SIZE: usize = 1_000;

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
/// more than `max_size` idle connections. This matches the actor model where
/// database access is sporadic — actors keep state in memory and only touch
/// persistence during recovery, persist, or snapshot.
///
/// Creation is also bounded: if `max_size` connections already exist (idle or
/// checked-out), `checkout` blocks until a connection is returned.
struct SqlitePool {
    path: PathBuf,
    durability: bool,
    tuning: SqliteTuning,
    max_size: usize,
    state: Mutex<PoolState>,
    condvar: Condvar,
}

struct PoolState {
    available: Vec<Connection>,
    total: usize,
}

/// A connection checked out from the pool.
///
/// On drop the connection is returned to the pool (or discarded if the pool
/// already has `max_size` idle connections).
struct PooledConnection {
    conn: Option<Connection>,
    pool: Arc<SqlitePool>,
}

impl std::ops::Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        // Invariant: `conn` is only `None` after `Drop` has consumed it.
        // `Deref` is never called after Drop, so this is guaranteed to succeed.
        self.conn
            .as_ref()
            .expect("PooledConnection accessed after drop")
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // Invariant: same as `Deref` — `conn` is `Some` until Drop runs.
        self.conn
            .as_mut()
            .expect("PooledConnection accessed after drop")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.checkin(conn);
        }
    }
}

impl SqlitePool {
    /// Obtains a connection from the pool, creating a new one only if the
    /// total number of connections (idle + checked-out) is below `max_size`.
    fn checkout(self: &Arc<Self>) -> Result<PooledConnection, Error> {
        let mut state = self.state.lock().map_err(|e| Error::Store {
            source: None,
            operation: StoreOperation::LockManagerData,
            reason: format!("connection pool mutex poisoned: {}", e),
        })?;

        // Wait until an idle connection is available or we have a free slot.
        while state.available.is_empty() && state.total >= self.max_size {
            state = self.condvar.wait(state).map_err(|e| Error::Store {
                source: None,
                operation: StoreOperation::LockManagerData,
                reason: format!("connection pool condvar poisoned: {}", e),
            })?;
        }

        if let Some(conn) = state.available.pop() {
            return Ok(PooledConnection {
                conn: Some(conn),
                pool: self.clone(),
            });
        }

        // We have a slot to create a new connection.
        state.total += 1;
        drop(state);

        let conn =
            match open_with_tuning(&self.path, self.durability, self.tuning) {
                Ok(conn) => conn,
                Err(e) => {
                    let mut state =
                        self.state.lock().unwrap_or_else(|e| e.into_inner());
                    state.total -= 1;
                    drop(state);
                    self.condvar.notify_one();
                    return Err(e);
                }
            };

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.clone(),
        })
    }

    /// Returns a connection to the idle set, discarding it if the pool is
    /// already at capacity.
    fn checkin(&self, conn: Connection) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.available.len() < self.max_size {
            state.available.push(conn);
        } else {
            state.total -= 1;
        }
        drop(state);
        self.condvar.notify_one();
    }

    /// Wait until all checked-out connections have been returned.
    fn drain(&self) -> Result<(), Error> {
        let mut state = self.state.lock().map_err(|e| Error::Store {
            source: None,
            operation: StoreOperation::LockManagerData,
            reason: format!("connection pool mutex poisoned: {}", e),
        })?;
        while state.total != state.available.len() {
            state = self.condvar.wait(state).map_err(|e| Error::Store {
                source: None,
                operation: StoreOperation::LockManagerData,
                reason: format!("connection pool condvar poisoned: {}", e),
            })?;
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
    /// * `path` - Directory path where the database file will be created.
    ///   The database file will be named "database.db" within this directory.
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
        path: &PathBuf,
        durability: bool,
        spec: Option<MachineSpec>,
    ) -> Result<Self, Error> {
        info!("Creating SQLite database manager");
        if !Path::new(&path).exists() {
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

        let spec = resolve_spec(spec);
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

        // Pool size: 1× vCPU, clamped between 4 and 16. SQLite is single-writer
        // and each connection carries its own page cache, so excess connections
        // hurt more than help.
        let max_size = spec.cpu_cores.clamp(4, 16);
        info!("SQLite connection pool size: {}", max_size);

        let pool = Arc::new(SqlitePool {
            path: db_path,
            durability,
            tuning,
            max_size,
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
        Ok(SqliteCollection::new(self.clone(), identifier, prefix))
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
        Ok(SqliteCollection::new(self.clone(), identifier, prefix))
    }

    fn stop(&mut self) -> Result<(), Error> {
        debug!("Stopping SQLite manager, draining pool and flushing WAL");
        self.pool.drain().map_err(|e| {
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
        conn.execute_batch("PRAGMA optimize; PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(|e| {
                error!(error = %e, "Failed to checkpoint WAL on stop");
                Error::Store {
                    source: None,
                    operation: StoreOperation::WalCheckpoint,
                    reason: format!("{}", e),
                }
            })?;
        drop(conn);
        debug!("SQLite WAL checkpoint complete");
        Ok(())
    }
}

/// SQLite collection that implements both Collection and State traits.
/// Stores key-value pairs in a SQLite table with prefix-based namespacing.
///
/// # Schema
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
    pub fn new(manager: SqliteManager, table: &str, prefix: &str) -> Self {
        Self {
            manager,
            table: table.to_owned(),
            prefix: prefix.to_owned(),
        }
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
                conn.prepare(&q).and_then(|mut s| {
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
                conn.prepare(&q).and_then(|mut s| {
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
                conn.prepare(&q).and_then(|mut s| {
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
                conn.prepare(&q).and_then(|mut s| {
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
            format!("SELECT value FROM {} WHERE prefix = ?1", &self.table);
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
            .query_row(&query, params![self.prefix], |row| row.get(0))
            .map_err(|e| self.map_get_error(e, key))?;

        Ok(row)
    }

    fn put(&mut self, data: &[u8]) -> Result<(), Error> {
        let stmt = format!(
            "INSERT OR REPLACE INTO {} (prefix, value) VALUES (?1, ?2)",
            &self.table
        );
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for state put");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        conn.execute(&stmt, params![self.prefix, data])
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
        let stmt = format!("DELETE FROM {} WHERE prefix = ?1", &self.table);
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
        let stmt = format!("DELETE FROM {} WHERE prefix = ?1", &self.table);
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
            &self.table
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
            .query_row(&query, params![self.prefix, key], |row| row.get(0))
            .map_err(|e| self.map_get_error(e, collection_key))?;

        Ok(row)
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), Error> {
        let stmt = format!(
            "INSERT OR REPLACE INTO {} (prefix, sn, value) VALUES (?1, ?2, ?3)",
            &self.table
        );
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for collection put");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        conn.execute(&stmt, params![self.prefix, key, data])
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
        let stmt = format!(
            "DELETE FROM {} WHERE prefix = ?1 AND sn = ?2",
            &self.table
        );
        let conn = self.manager.pool.checkout().map_err(|e| {
            error!(error = %e, "Failed to check out connection for collection delete");
            Error::Store {
                source: None,
                operation: StoreOperation::OpenConnection,
                reason: format!("{}", e),
            }
        })?;

        let affected_rows = conn
            .execute(&stmt, params![self.prefix, key])
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
        let stmt = format!("DELETE FROM {} WHERE prefix = ?1", &self.table);
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
        let mut iter = self.iter(true)?;
        iter.next().transpose()
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

    fn del_range(&mut self, start: &str, end: &str) -> Result<(), Error> {
        let stmt = format!(
            "DELETE FROM {} WHERE prefix = ?1 AND sn >= ?2 AND sn <= ?3",
            &self.table
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
    )
    .map_err(|e| {
        error!(error = %e, "Failed to execute SQLite PRAGMA statements");
        Error::Store {
            source: None,
            operation: StoreOperation::ExecuteBatch,
            reason: format!("{}", e),
        }
    })?;

    debug!("SQLite database opened and configured successfully");
    Ok(conn)
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
            SqliteCollection::new(manager.clone(), "test", "test");

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

        let mut state = SqliteCollection::new(manager, "state", "test");
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
            state: Mutex::new(PoolState {
                available: Vec::new(),
                total: 0,
            }),
            condvar: Condvar::new(),
        });

        let mut manager = SqliteManager {
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
}
