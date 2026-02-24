

//! # SQLite database backend.
//!
//! This module contains the SQLite database backend implementation.
//!

use ave_actors_store::{
    Error, config::{MachineSpec, resolve_spec}, database::{Collection, DbManager, State}
};

use rusqlite::{Connection, OpenFlags, params};
use tracing::{debug, error, info, warn};

use std::{collections::VecDeque, path::PathBuf, sync::{Arc, Mutex}};
use std::{fs, path::Path};

type EntryIterator = Box<dyn Iterator<Item = (String, Vec<u8>)>>;
const ITER_CHUNK_SIZE: usize = 1_000;

/// SQLite database manager for persistent actor storage.
/// Manages SQLite database connections and provides factory methods
/// for creating collections (event storage) and state storage (snapshots).
///
/// # Storage Model
///
/// - **Collections**: SQLite tables with (prefix, sn, value) schema
/// - **State**: SQLite tables with (prefix, value) schema
/// - **Connection**: Thread-safe shared connection using Arc<Mutex<Connection>>
///
#[derive(Clone)]
pub struct SqliteManager {
    /// Thread-safe shared SQLite connection.
    conn: Arc<Mutex<Connection>>,
}

impl SqliteManager {
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
    pub fn new(path: &PathBuf, durability: bool, spec: Option<MachineSpec>) -> Result<Self, Error> {
        info!("Creating SQLite database manager");
        if !Path::new(&path).exists() {
            debug!("Path does not exist, creating it");
            fs::create_dir_all(path).map_err(|e| {
                error!(path = ?path, error = %e, "Failed to create SQLite directory");
                Error::CreateStore {
                    reason: format!(
                    "fail SQLite create directory: {}",
                    e
                ),
                }
            })?;
        }

        let path = path.join("database.db");

        debug!("Opening SQLite connection");
        let conn = open(&path, durability, spec).map_err(|e| {
            error!(path = ?path, error = %e, "Failed to open SQLite connection");
            Error::CreateStore { reason: format!("fail SQLite open connection: {}", e) }
        })?;

        debug!("SQLite database manager created successfully");
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

impl DbManager<SqliteCollection, SqliteCollection> for SqliteManager {
    fn create_state(
        &self,
        identifier: &str,
        prefix: &str,
    ) -> Result<SqliteCollection, Error> {
        let stmt = format!(
            "CREATE TABLE IF NOT EXISTS {} (prefix TEXT NOT NULL, value \
            BLOB NOT NULL, PRIMARY KEY (prefix))",
            identifier
        );

        {
            let conn = self.conn.lock().map_err(|e| {
                error!(error = %e, "Failed to acquire connection lock for state creation");
                Error::Store { operation: "lock_connection".to_owned(), reason: format!("{}", e) }
            })?;

            conn.execute(stmt.as_str(), ()).map_err(|e| {
                error!(table = identifier, error = %e, "Failed to create state table");
                Error::CreateStore { reason: format!("fail SQLite create table: {}", e) }
            })?;
        }

        debug!(table = identifier, prefix = prefix, "State table created");
        Ok(SqliteCollection::new(self.conn.clone(), identifier, prefix))
    }

    fn create_collection(
        &self,
        identifier: &str,
        prefix: &str,
    ) -> Result<SqliteCollection, Error> {
        let stmt = format!(
            "CREATE TABLE IF NOT EXISTS {} (prefix TEXT NOT NULL, sn TEXT NOT NULL, value \
            BLOB NOT NULL, PRIMARY KEY (prefix, sn))",
            identifier
        );

        {
            let conn = self.conn.lock().map_err(|e| {
                error!(error = %e, "Failed to acquire connection lock for collection creation");
                Error::Store { operation: "lock_connection".to_owned(), reason: format!("{}", e) }
            })?;

            conn.execute(stmt.as_str(), ()).map_err(|e| {
                error!(table = identifier, error = %e, "Failed to create collection table");
                Error::CreateStore { reason: format!("fail SQLite create table: {}", e) }
            })?;
        }

        debug!(table = identifier, prefix = prefix, "Collection table created");
        Ok(SqliteCollection::new(self.conn.clone(), identifier, prefix))
    }

    
    fn stop(self) -> Result<(), Error> {
        debug!("Stopping SQLite manager, flushing WAL");
        let conn = self.conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock on stop");
            Error::Store { operation: "lock_connection".to_owned(), reason: format!("{}", e) }
        })?;
        conn.execute_batch("PRAGMA optimize; PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(|e| {
                error!(error = %e, "Failed to checkpoint WAL on stop");
                Error::Store { operation: "wal_checkpoint".to_owned(), reason: format!("{}", e) }
            })?;
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
    /// Shared SQLite connection.
    conn: Arc<Mutex<Connection>>,
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
    /// * `conn` - Shared SQLite connection.
    /// * `table` - Name of the table in the database.
    /// * `prefix` - Prefix for namespacing this collection's data.
    ///
    /// # Returns
    ///
    /// Returns a new SqliteCollection instance.
    ///
    pub fn new(
        conn: Arc<Mutex<Connection>>,
        table: &str,
        prefix: &str,
    ) -> Self {
        Self {
            conn,
            table: table.to_owned(),
            prefix: prefix.to_owned(),
        }
    }

    /// Create a new iterator filtering by prefix.
fn make_iter(&self, reverse: bool) -> EntryIterator {
        Box::new(SqliteChunkedIterator::new(
            self.conn.clone(),
            self.table.clone(),
            self.prefix.clone(),
            reverse,
        ))
    }
}

/// Iterador paginado sobre una colección SQLite usando keyset pagination.
///
/// Funciona correctamente cuando los sn están zero-padded (orden lexicográfico
/// == numérico). Fetcha `ITER_CHUNK_SIZE` filas por chunk, soltando el lock
/// entre chunks para que las escrituras concurrentes no queden bloqueadas.
struct SqliteChunkedIterator {
    conn: Arc<Mutex<Connection>>,
    table: String,
    prefix: String,
    reverse: bool,
    /// Filas ya cargadas del chunk actual, pendientes de entregar.
    buffer: VecDeque<(String, Vec<u8>)>,
    /// Último sn visto; se usa como cursor para el siguiente chunk.
    last_key: Option<String>,
    /// True cuando la última query devolvió 0 filas — no hay más datos.
    exhausted: bool,
}

impl SqliteChunkedIterator {
    fn new(
        conn: Arc<Mutex<Connection>>,
        table: String,
        prefix: String,
        reverse: bool,
    ) -> Self {
        Self {
            conn,
            table,
            prefix,
            reverse,
            buffer: VecDeque::new(),
            last_key: None,
            exhausted: false,
        }
    }

    fn fetch_chunk(&mut self) {
        let conn = match self.conn.lock() {
            Ok(c) => c,
            Err(e) => {
                warn!(table = %self.table, error = %e, "Failed to acquire lock for chunk fetch");
                self.exhausted = true;
                return;
            }
        };

        let order = if self.reverse { "DESC" } else { "ASC" };
        let cmp   = if self.reverse { "<" } else { ">" };

        let rows: Vec<(String, Vec<u8>)> = match &self.last_key {
            None => {
                let q = format!(
                    "SELECT sn, value FROM {} WHERE prefix = ?1 ORDER BY sn {} LIMIT {}",
                    self.table, order, ITER_CHUNK_SIZE
                );
                match conn.prepare(&q).and_then(|mut s| {
                    s.query_map(params![self.prefix], |r| Ok((r.get(0)?, r.get(1)?)))
                        .map(|rows| rows.filter_map(|r| r.ok()).collect())
                }) {
                    Ok(rows) => rows,
                    Err(e) => {
                        warn!(table = %self.table, error = %e, "Failed to fetch first chunk from DB");
                        self.exhausted = true;
                        return;
                    }
                }
            }
            Some(last) => {
                
                let q = format!(
                    "SELECT sn, value FROM {} WHERE prefix = ?1 AND sn {} ?2 ORDER BY sn {} LIMIT {}",
                    self.table, cmp, order, ITER_CHUNK_SIZE
                );
                let last = last.clone();
                match conn.prepare(&q).and_then(|mut s| {
                    s.query_map(params![self.prefix, last], |r| Ok((r.get(0)?, r.get(1)?)))
                        .map(|rows| rows.filter_map(|r| r.ok()).collect())
                }) {
                    Ok(rows) => rows,
                    Err(e) => {
                        warn!(table = %self.table, error = %e, "Failed to fetch next chunk from DB");
                        self.exhausted = true;
                        return;
                    }
                }
            }
        };

        if rows.is_empty() {
            self.exhausted = true;
        } else {
            self.last_key = rows.last().map(|(k, _)| k.clone());
            self.buffer.extend(rows);
        }
    }
}

impl Iterator for SqliteChunkedIterator {
    type Item = (String, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() && !self.exhausted {
            self.fetch_chunk();
        }
        self.buffer.pop_front()
    }
}

impl State for SqliteCollection {
    fn get(&self) -> Result<Vec<u8>, Error> {
        let conn = self.conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock for state get");
            Error::Store { operation: "open_connection".to_owned(), reason: format!("{}", e) }
        })?;
        let query =
            format!("SELECT value FROM {} WHERE prefix = ?1", &self.table);
        let row: Vec<u8> = conn
            .query_row(&query, params![self.prefix], |row| row.get(0))
            .map_err(|e| Error::EntryNotFound { key: e.to_string() })?;

        Ok(row)
    }

    fn put(&mut self, data: &[u8]) -> Result<(), Error> {
        let conn = self.conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock for state put");
            Error::Store { operation: "open_connection".to_owned(), reason: format!("{}", e) }
        })?;
        let stmt = format!(
            "INSERT OR REPLACE INTO {} (prefix, value) VALUES (?1, ?2)",
            &self.table
        );
        conn.execute(&stmt, params![self.prefix, data])
            .map_err(|e| {
                error!(table = %self.table, error = %e, "Failed to put state");
                Error::Store { operation: "insert".to_owned(), reason: format!("{}", e) }
            })?;
        Ok(())
    }

    fn del(&mut self) -> Result<(), Error> {
        let conn = self.conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock for state delete");
            Error::Store { operation: "open_connection".to_owned(), reason: format!("{}", e) }
        })?;
        let stmt = format!("DELETE FROM {} WHERE prefix = ?1", &self.table);
        conn.execute(&stmt, params![self.prefix,])
            .map_err(|e| {
                warn!(table = %self.table, error = %e, "Failed to delete state");
                Error::EntryNotFound { key: e.to_string() }
            })?;
        Ok(())
    }

    fn purge(&mut self) -> Result<(), Error> {
        let conn = self.conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock for state purge");
            Error::Store { operation: "open_connection".to_owned(), reason: format!("{}", e) }
        })?;
        let stmt = format!("DELETE FROM {} WHERE prefix = ?1", &self.table);
        conn.execute(&stmt, params![self.prefix])
            .map_err(|e| {
                error!(table = %self.table, error = %e, "Failed to purge state");
                Error::Store { operation: "purge".to_owned(), reason: format!("{}", e) }
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
        let conn = self.conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock for collection get");
            Error::Store { operation: "open_connection".to_owned(), reason: format!("{}", e) }
        })?;
        let query = format!(
            "SELECT value FROM {} WHERE prefix = ?1 AND sn = ?2",
            &self.table
        );
        let row: Vec<u8> = conn
            .query_row(&query, params![self.prefix, key], |row| row.get(0))
            .map_err(|e| Error::EntryNotFound { key: e.to_string() })?;

        Ok(row)
    }

    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), Error> {
        let conn = self.conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock for collection put");
            Error::Store { operation: "open_connection".to_owned(), reason: format!("{}", e) }
        })?;
        let stmt = format!(
            "INSERT OR REPLACE INTO {} (prefix, sn, value) VALUES (?1, ?2, ?3)",
            &self.table
        );
        conn.execute(&stmt, params![self.prefix, key, data])
            .map_err(|e| {
                error!(table = %self.table, key = key, error = %e, "Failed to put collection entry");
                Error::Store { operation: "insert".to_owned(), reason: format!("{}", e) }
            })?;
        Ok(())
    }

    fn del(&mut self, key: &str) -> Result<(), Error> {
        let conn = self.conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock for collection delete");
            Error::Store { operation: "open_connection".to_owned(), reason: format!("{}", e) }
        })?;
        let stmt = format!(
            "DELETE FROM {} WHERE prefix = ?1 AND sn = ?2",
            &self.table
        );
        conn.execute(&stmt, params![self.prefix, key])
            .map_err(|e| {
                warn!(table = %self.table, key = key, error = %e, "Failed to delete collection entry");
                Error::EntryNotFound { key: e.to_string() }
            })?;
        Ok(())
    }

    fn purge(&mut self) -> Result<(), Error> {
        let conn = self.conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock for collection purge");
            Error::Store { operation: "open_connection".to_owned(), reason: format!("{}", e) }
        })?;
        let stmt = format!("DELETE FROM {} WHERE prefix = ?1", &self.table);
        conn.execute(&stmt, params![self.prefix])
            .map_err(|e| {
                error!(table = %self.table, error = %e, "Failed to purge collection");
                Error::Store { operation: "purge".to_owned(), reason: format!("{}", e) }
            })?;
        debug!(table = %self.table, "Collection purged");
        Ok(())
    }

    fn last(&self) -> Option<(String, Vec<u8>)> {
        let conn = self.conn.lock()
            .map_err(|e| warn!(table = %self.table, error = %e, "Failed to acquire lock in last()"))
            .ok()?;
        let query = format!(
            "SELECT sn, value FROM {} WHERE prefix = ?1 ORDER BY sn DESC LIMIT 1",
            self.table
        );
        conn.query_row(&query, params![self.prefix], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .ok()
    }

    fn iter<'a>(
        &'a self,
        reverse: bool,
    ) -> Box<dyn Iterator<Item = (String, Vec<u8>)> + 'a> {
        self.make_iter(reverse)
    }

    fn name(&self) -> &str {
        self.table.as_str()
    }
}

/// Open a SQLite database connection.
pub fn open<P: AsRef<Path>>(path: P, durability: bool, spec: Option<MachineSpec>) -> Result<Connection, Error> {
    let path = path.as_ref();
    debug!(path = ?path, "Opening SQLite database");
    let flags =
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    let conn = Connection::open_with_flags(path, flags).map_err(|e| {
        error!(path = ?path, error = %e, "Failed to open SQLite database");
        Error::Store { operation: "open_connection".to_owned(), reason: format!("{}", e) }
    })?;

    let spec = resolve_spec(spec);
    let (ram_mb, cores) = (spec.ram_mb, spec.cpu_cores);
    info!("SQLite tuning: ram_mb={}, cpu_cores={}", ram_mb, cores);

    let sync_mode = if durability { "FULL" } else { "NORMAL" };

    let tuning = tuning_for_ram(ram_mb);

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
        Error::Store { operation: "execute_batch".to_owned(), reason: format!("{}", e) }
    })?;

    debug!("SQLite database opened and configured successfully");
    Ok(conn)
}


/// Compute SQLite tuning parameters directly from available RAM.
///
/// SQLite is single-writer so CPU cores don't affect tuning here.
/// Cache target: 2 % of RAM, floor 8 MB, cap 1 GB.
fn tuning_for_ram(ram_mb: u64) -> SqliteTuning {
    // Page cache: 2 % of RAM, floor 8 MB, cap 1 GB
    let cache_mb = (ram_mb * 2 / 100).max(8).min(1024);
    let cache_size_kb = -(cache_mb as i64 * 1024); // negative = KB in SQLite

    // mmap: same size as cache (virtual memory; OS decides what stays in RAM)
    let mmap_size_bytes = cache_mb as i64 * 1024 * 1024;

    // WAL checkpoint: every ram_mb/2 pages (4 KB each), floor 128, cap 8 000
    let wal_autocheckpoint_pages = (ram_mb as i64 / 2).max(128).min(8_000);

    // journal_size_limit: 2× cache on disk, floor 32 MB, cap 1 GB
    let journal_size_limit_bytes = (cache_mb as i64 * 2 * 1024 * 1024)
        .max(32 * 1024 * 1024)
        .min(1024 * 1024 * 1024);

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
    pub fn create_temp_dir() -> String {
        let path = temp_dir();

        if fs::metadata(&path).is_err() {
            fs::create_dir_all(&path).unwrap();
        }
        path
    }

    fn temp_dir() -> String {
        let dir =
            tempfile::tempdir().expect("Can not create temporal directory.");
        dir.path().to_str().unwrap().to_owned()
    }

    impl Default for SqliteManager {
        fn default() -> Self {
            let path = format!("{}/database.db", create_temp_dir());
            let conn = open(&path, false, None)
                .map_err(|e| {
                    Error::CreateStore {
                        reason: format!(
                        "fail SQLite open connection: {}",
                        e
                    ),
                    }
                })
                .expect("Cannot open the database ");

            Self {
                conn: Arc::new(Mutex::new(conn)),
            }
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
}
