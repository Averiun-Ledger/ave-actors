//! Simple edge case tests for SQLite database to increase coverage

use ave_actors_sqlite::SqliteManager;
use ave_actors_store::{
    Error,
    database::{Collection, DbManager, State},
};
use rusqlite::Connection;
use tempfile::tempdir;

#[test]
fn test_sqlite_manager_edge_cases() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test");
    let manager = SqliteManager::new(&db_path, false, None).unwrap();

    // Test collection operations
    let mut collection = manager.create_collection("test", "prefix").unwrap();

    // Test empty key/value
    Collection::put(&mut collection, "", b"").unwrap();
    let result = Collection::get(&collection, "").unwrap();
    assert_eq!(result, b"");

    // Test overwrite
    Collection::put(&mut collection, "key", b"value1").unwrap();
    Collection::put(&mut collection, "key", b"value2").unwrap();
    let result = Collection::get(&collection, "key").unwrap();
    assert_eq!(result, b"value2");

    // Test delete
    Collection::del(&mut collection, "key").unwrap();
    let result = Collection::get(&collection, "key");
    assert!(result.is_err());

    let result = Collection::del(&mut collection, "non-existent");
    assert_eq!(
        result,
        Err(Error::EntryNotFound {
            key: "prefix.non-existent".to_owned(),
        })
    );
}

#[test]
fn test_sqlite_state_operations() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("state_test");
    let manager = SqliteManager::new(&db_path, false, None).unwrap();

    let mut state = manager.create_state("state", "prefix").unwrap();

    // Test get empty state
    let result = State::get(&state);
    assert_eq!(
        result,
        Err(Error::EntryNotFound {
            key: "prefix".to_owned(),
        })
    );

    // Test put/get
    State::put(&mut state, b"state_data").unwrap();
    let result = State::get(&state).unwrap();
    assert_eq!(result, b"state_data");

    // Test delete
    State::del(&mut state).unwrap();
    let result = State::get(&state);
    assert_eq!(
        result,
        Err(Error::EntryNotFound {
            key: "prefix".to_owned(),
        })
    );

    let result = State::del(&mut state);
    assert_eq!(
        result,
        Err(Error::EntryNotFound {
            key: "prefix".to_owned(),
        })
    );
}

#[test]
fn test_sqlite_iteration() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("iteration");
    let manager = SqliteManager::new(&db_path, false, None).unwrap();

    let mut collection = manager.create_collection("iter", "prefix").unwrap();

    // Test empty iteration
    let items: Vec<_> = collection
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(items.len(), 0);

    // Add items
    Collection::put(&mut collection, "a", b"1").unwrap();
    Collection::put(&mut collection, "b", b"2").unwrap();
    Collection::put(&mut collection, "c", b"3").unwrap();

    // Test forward iteration
    let items: Vec<_> = collection
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(items.len(), 3);

    // Test reverse iteration
    let items: Vec<_> = collection
        .iter(true)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(items.len(), 3);
}

#[test]
fn test_sqlite_get_preserves_operational_errors() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("broken_get");
    let manager = SqliteManager::new(&db_path, false, None).unwrap();

    let state = manager.create_state("state_table", "prefix").unwrap();
    let collection = manager
        .create_collection("collection_table", "prefix")
        .unwrap();

    let sqlite_path = db_path.join("database.db");
    let conn = Connection::open(sqlite_path).unwrap();
    conn.execute("DROP TABLE state_table", ()).unwrap();
    conn.execute("DROP TABLE collection_table", ()).unwrap();

    let state_result = State::get(&state);
    assert!(matches!(
        state_result,
        Err(Error::Get { key, .. }) if key == "prefix"
    ));

    let collection_result = Collection::get(&collection, "key");
    assert!(matches!(
        collection_result,
        Err(Error::Get { key, .. }) if key == "prefix.key"
    ));
}

#[test]
fn test_sqlite_collection_get_reports_requested_key() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("missing_collection_key");
    let manager = SqliteManager::new(&db_path, false, None).unwrap();

    let collection = manager.create_collection("test", "prefix").unwrap();
    let result = Collection::get(&collection, "missing");

    assert_eq!(
        result,
        Err(Error::EntryNotFound {
            key: "prefix.missing".to_owned(),
        })
    );
}

#[test]
fn test_sqlite_identifier_validation() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("identifier_validation");
    let manager = SqliteManager::new(&db_path, false, None).unwrap();

    manager
        .create_collection("valid_name_42", "prefix")
        .unwrap();
    manager.create_state("_valid_state", "prefix").unwrap();

    let invalid_collection =
        manager.create_collection("users; DROP TABLE x", "prefix");
    assert!(matches!(
        invalid_collection,
        Err(Error::CreateStore { reason }) if reason.contains("invalid SQLite identifier")
    ));

    let invalid_state = manager.create_state("bad-name", "prefix");
    assert!(matches!(
        invalid_state,
        Err(Error::CreateStore { reason }) if reason.contains("invalid SQLite identifier")
    ));
}
