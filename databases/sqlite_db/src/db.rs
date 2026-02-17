

//! # SQLite database backend.
//!
//! This module contains the SQLite database backend implementation.
//!

use ave_actors_store::{
    Error,
    database::{Collection, DbManager, State},
};

use rusqlite::{Connection, OpenFlags, params};
use tracing::{debug, error, info, warn};

use std::{path::PathBuf, sync::{Arc, Mutex}};
use std::{env, fs, path::Path};

type EntryIterator = Box<dyn Iterator<Item = (String, Vec<u8>)>>;

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
    pub fn new(path: &PathBuf) -> Result<Self, Error> {
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
        let conn = open(&path).map_err(|e| {
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
    fn make_iter(
        &self,
        reverse: bool,
    ) -> Result<EntryIterator, Error> {
        let order = if reverse { "DESC" } else { "ASC" };
        let conn = self.conn.lock().map_err(|e| {
            error!(error = %e, "Failed to acquire connection lock for iterator");
            Error::Store { operation: "lock_connection".to_owned(), reason: format!("{}", e) }
        })?;
        let query = format!(
            "SELECT sn, value FROM {} WHERE prefix = ?1 ORDER BY sn {}",
            self.table, order,
        );
        let mut stmt = conn.prepare(&query).map_err(|e| {
            error!(table = %self.table, error = %e, "Failed to prepare iterator query");
            Error::Store { operation: "prepare".to_owned(), reason: format!("{}", e) }
        })?;
        let mut rows = stmt.query(params![self.prefix]).map_err(|e| {
            error!(table = %self.table, error = %e, "Failed to execute iterator query");
            Error::Store { operation: "query".to_owned(), reason: format!("{}", e) }
        })?;
        let mut values = Vec::new();
        while let Some(row) = rows.next().map_err(|e| {
            error!(table = %self.table, error = %e, "Failed to read iterator row");
            Error::Store { operation: "next_row".to_owned(), reason: format!("{}", e) }
        })? {
            let key: String = row.get(0).map_err(|e| {
                error!(table = %self.table, column = 0, error = %e, "Failed to read column");
                Error::Store { operation: "get_column".to_owned(), reason: format!("{}", e) }
            })?;
            let value: Vec<u8> = row.get(1).map_err(|e| {
                error!(table = %self.table, column = 1, error = %e, "Failed to read column");
                Error::Store { operation: "get_column".to_owned(), reason: format!("{}", e) }
            })?;
            values.push((key, value));
        }
        Ok(Box::new(values.into_iter()))
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
        .map_err(|e| warn!(table = %self.table, error = %e, "Failed to query last()"))
        .ok()
    }

    fn iter<'a>(
        &'a self,
        reverse: bool,
    ) -> Box<dyn Iterator<Item = (String, Vec<u8>)> + 'a> {
        match self.make_iter(reverse) {
            Ok(iter) => iter,
            Err(e) => {
                warn!(table = %self.table, error = %e, "Failed to create iterator");
                Box::new(std::iter::empty())
            }
        }
    }

    fn name(&self) -> &str {
        self.table.as_str()
    }
}

/// Open a SQLite database connection.
pub fn open<P: AsRef<Path>>(path: P) -> Result<Connection, Error> {
    let path = path.as_ref();
    debug!(path = ?path, "Opening SQLite database");
    let flags =
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE;
    let conn = Connection::open_with_flags(path, flags).map_err(|e| {
        error!(path = ?path, error = %e, "Failed to open SQLite database");
        Error::Store { operation: "open_connection".to_owned(), reason: format!("{}", e) }
    })?;

    let cores = num_cpus::get();
    let ram_mb = detect_total_memory_mb().unwrap_or(4096);
    let profile = resolve_profile(cores, ram_mb);
    info!(
        "SQLite profile {:?} detected (cores: {}, RAM MB: {})",
        profile, cores, ram_mb
    );

    let sync_mode = if env_bool("SQLITE_STRONG_DURABILITY").unwrap_or(false) {
        "FULL"
    } else {
        "NORMAL"
    };

    let tuning = match profile {
        SqliteProfile::Small => SqliteTuning {
            wal_autocheckpoint_pages: 256,          // ~1MB WAL
            journal_size_limit_bytes: 64 * 1024 * 1024,
            cache_size_kb: -16_384,                 // ~16MB
            mmap_size_bytes: 64 * 1024 * 1024,      // 64MB
        },
        SqliteProfile::Large => SqliteTuning {
            wal_autocheckpoint_pages: 2000,         // ~8MB WAL
            journal_size_limit_bytes: 256 * 1024 * 1024,
            cache_size_kb: -131_072,                // ~128MB
            mmap_size_bytes: 256 * 1024 * 1024,     // 256MB
        },
    };

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

#[derive(Clone, Copy, Debug)]
enum SqliteProfile {
    Small,
    Large,
}

#[derive(Clone, Copy)]
struct SqliteTuning {
    wal_autocheckpoint_pages: i64,
    journal_size_limit_bytes: i64,
    cache_size_kb: i64,
    mmap_size_bytes: i64,
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

fn resolve_profile(cores: usize, ram_mb: u64) -> SqliteProfile {
    if let Some(override_profile) = profile_override_from_env() {
        return override_profile;
    }
    if cores <= 2 || ram_mb <= 1024 {
        SqliteProfile::Small
    } else {
        SqliteProfile::Large
    }
}

fn profile_override_from_env() -> Option<SqliteProfile> {
    match env::var("SQLITE_PROFILE") {
        Ok(val) => match val.to_lowercase().as_str() {
            "small" => Some(SqliteProfile::Small),
            "large" => Some(SqliteProfile::Large),
            "auto" => None,
            _ => None,
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
            let conn = open(&path)
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
