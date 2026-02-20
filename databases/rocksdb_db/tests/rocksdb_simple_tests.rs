

//! Simple edge case tests for RocksDB database to increase coverage

use ave_actors_rocksdb::RocksDbManager;
use ave_actors_store::{config::Config, database::{Collection, DbManager, State}};
use tempfile::tempdir;

#[test]
fn test_rocksdb_manager_edge_cases() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test");
    let manager = RocksDbManager::new(&db_path,  Config::default()).unwrap();

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

    // Test delete non-existent (may or may not fail depending on implementation)
    let _result = Collection::del(&mut collection, "non-existent");
    // Some implementations might return Ok() for non-existent deletes
}

#[test]
fn test_rocksdb_state_operations() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("state_test");
    let manager = RocksDbManager::new(&db_path, Config::default()).unwrap();

    let mut state = manager.create_state("state", "prefix").unwrap();

    // Test get empty state
    let result = State::get(&state);
    assert!(result.is_err());

    // Test put/get
    State::put(&mut state, b"state_data").unwrap();
    let result = State::get(&state).unwrap();
    assert_eq!(result, b"state_data");

    // Test delete
    State::del(&mut state).unwrap();
    let result = State::get(&state);
    assert!(result.is_err());
}

#[test]
fn test_rocksdb_iteration() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("iteration");
    let manager = RocksDbManager::new(&db_path, Config::default()).unwrap();

    let mut collection = manager.create_collection("iter", "prefix").unwrap();

    // Test empty iteration
    let items: Vec<_> = collection.iter(false).collect();
    assert_eq!(items.len(), 0);

    // Add items
    Collection::put(&mut collection, "a", b"1").unwrap();
    Collection::put(&mut collection, "b", b"2").unwrap();
    Collection::put(&mut collection, "c", b"3").unwrap();

    // Test forward iteration
    let items: Vec<_> = collection.iter(false).collect();
    assert_eq!(items.len(), 3);

    // Test reverse iteration
    let items: Vec<_> = collection.iter(true).collect();
    assert_eq!(items.len(), 3);

    // Test last
    let last = collection.last();
    assert!(last.is_some());
}

#[test]
fn test_rocksdb_purge() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("purge_test");
    let manager = RocksDbManager::new(&db_path, Config::default()).unwrap();

    let mut collection = manager.create_collection("purge", "prefix").unwrap();

    // Add items
    Collection::put(&mut collection, "key1", b"value1").unwrap();
    Collection::put(&mut collection, "key2", b"value2").unwrap();

    // Verify items exist
    let items: Vec<_> = collection.iter(false).collect();
    assert_eq!(items.len(), 2);

    // Purge
    Collection::purge(&mut collection).unwrap();

    // Verify empty
    let items: Vec<_> = collection.iter(false).collect();
    assert_eq!(items.len(), 0);
}