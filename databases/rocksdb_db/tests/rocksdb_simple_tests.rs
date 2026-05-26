//! Simple edge case tests for RocksDB database to increase coverage

use ave_actors_rocksdb::RocksDbManager;
use ave_actors_store::{
    Error,
    config::MachineSpec,
    database::{Collection, DbManager, State},
};
use tempfile::tempdir;

#[test]
fn test_rocksdb_manager_edge_cases() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test");
    let manager = RocksDbManager::new(&db_path, false, None).unwrap();

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
fn test_rocksdb_state_operations() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("state_test");
    let manager = RocksDbManager::new(&db_path, false, None).unwrap();

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

    let result = State::del(&mut state);
    assert_eq!(
        result,
        Err(Error::EntryNotFound {
            key: "prefix".to_owned(),
        })
    );
}

#[test]
fn test_rocksdb_iteration() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("iteration");
    let manager = RocksDbManager::new(&db_path, false, None).unwrap();

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

    // Test last
    let last = collection.last().unwrap();
    assert!(last.is_some());
}

#[test]
fn test_rocksdb_purge() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("purge_test");
    let manager = RocksDbManager::new(&db_path, false, None).unwrap();

    let mut collection = manager.create_collection("purge", "prefix").unwrap();

    // Add items
    Collection::put(&mut collection, "key1", b"value1").unwrap();
    Collection::put(&mut collection, "key2", b"value2").unwrap();

    // Verify items exist
    let items: Vec<_> = collection
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(items.len(), 2);

    // Purge
    Collection::purge(&mut collection).unwrap();

    // Verify empty
    let items: Vec<_> = collection
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(items.len(), 0);
}

#[test]
fn test_rocksdb_reopen_existing_cfs() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("reopen");

    {
        let manager = RocksDbManager::new(&db_path, false, None).unwrap();
        let mut collection =
            manager.create_collection("events", "actor1").unwrap();
        Collection::put(&mut collection, "1", b"data1").unwrap();
        let mut state = manager.create_state("snapshots", "actor1").unwrap();
        State::put(&mut state, b"snapshot1").unwrap();
    }

    {
        let manager = RocksDbManager::new(&db_path, false, None).unwrap();
        let collection = manager.create_collection("events", "actor1").unwrap();
        assert_eq!(Collection::get(&collection, "1").unwrap(), b"data1");
        let state = manager.create_state("snapshots", "actor1").unwrap();
        assert_eq!(State::get(&state).unwrap(), b"snapshot1");
    }
}

#[test]
fn test_rocksdb_strong_durability() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("durability");
    let mut manager = RocksDbManager::new(&db_path, true, None).unwrap();

    let mut collection = manager.create_collection("c", "p").unwrap();
    Collection::put(&mut collection, "k", b"v").unwrap();
    assert_eq!(Collection::get(&collection, "k").unwrap(), b"v");

    let mut state = manager.create_state("s", "p").unwrap();
    State::put(&mut state, b"sv").unwrap();
    assert_eq!(State::get(&state).unwrap(), b"sv");

    assert!(manager.stop().is_ok());
}

#[test]
fn test_rocksdb_collection_get_not_found() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("get_not_found");
    let manager = RocksDbManager::new(&db_path, false, None).unwrap();
    let collection = manager.create_collection("c", "p").unwrap();
    assert_eq!(
        Collection::get(&collection, "missing"),
        Err(Error::EntryNotFound {
            key: "p.missing".to_owned(),
        })
    );
}

#[test]
fn test_rocksdb_state_purge() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("state_purge");
    let manager = RocksDbManager::new(&db_path, false, None).unwrap();
    let mut state = manager.create_state("s", "p").unwrap();

    State::put(&mut state, b"data").unwrap();
    assert_eq!(State::get(&state).unwrap(), b"data");

    State::purge(&mut state).unwrap();
    assert!(matches!(
        State::get(&state),
        Err(Error::EntryNotFound { .. })
    ));
}

#[test]
fn test_rocksdb_collection_last_empty() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("last_empty");
    let manager = RocksDbManager::new(&db_path, false, None).unwrap();
    let collection = manager.create_collection("c", "p").unwrap();
    assert_eq!(Collection::last(&collection).unwrap(), None);
}

#[test]
fn test_rocksdb_iter_range_and_del_range() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("range");
    let manager = RocksDbManager::new(&db_path, false, None).unwrap();
    let mut collection = manager.create_collection("c", "p").unwrap();

    Collection::put(&mut collection, "a", b"1").unwrap();
    Collection::put(&mut collection, "b", b"2").unwrap();
    Collection::put(&mut collection, "c", b"3").unwrap();
    Collection::put(&mut collection, "d", b"4").unwrap();
    Collection::put(&mut collection, "e", b"5").unwrap();

    let items: Vec<_> = collection
        .iter_range("b", "d", false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        items,
        vec![
            ("b".to_string(), b"2".to_vec()),
            ("c".to_string(), b"3".to_vec()),
            ("d".to_string(), b"4".to_vec()),
        ]
    );

    let items: Vec<_> = collection
        .iter_range("b", "d", true)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        items,
        vec![
            ("d".to_string(), b"4".to_vec()),
            ("c".to_string(), b"3".to_vec()),
            ("b".to_string(), b"2".to_vec()),
        ]
    );

    Collection::del_range(&mut collection, "b", "d").unwrap();

    let items: Vec<_> = collection
        .iter(false)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        items,
        vec![
            ("a".to_string(), b"1".to_vec()),
            ("e".to_string(), b"5".to_vec()),
        ]
    );
}

#[test]
fn test_rocksdb_iterator_prefix_boundary() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("boundary");
    let manager = RocksDbManager::new(&db_path, false, None).unwrap();

    let mut coll1 = manager.create_collection("c", "abc").unwrap();
    Collection::put(&mut coll1, "a", b"1").unwrap();
    Collection::put(&mut coll1, "b", b"2").unwrap();
    Collection::put(&mut coll1, "c", b"3").unwrap();

    let mut coll2 = manager.create_collection("c", "abd").unwrap();
    Collection::put(&mut coll2, "x", b"4").unwrap();

    let mut iter = coll1.iter(false).unwrap();
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("a".to_string(), b"1".to_vec())
    );
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("b".to_string(), b"2".to_vec())
    );
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("c".to_string(), b"3".to_vec())
    );
    assert!(iter.next().is_none());

    let mut iter = coll1.iter(true).unwrap();
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("c".to_string(), b"3".to_vec())
    );
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("b".to_string(), b"2".to_vec())
    );
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("a".to_string(), b"1".to_vec())
    );
    assert!(iter.next().is_none());
}

#[test]
fn test_rocksdb_range_iterator_boundary() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("range_boundary");
    let manager = RocksDbManager::new(&db_path, false, None).unwrap();

    let mut coll = manager.create_collection("c", "p").unwrap();
    Collection::put(&mut coll, "a", b"1").unwrap();
    Collection::put(&mut coll, "b", b"2").unwrap();
    Collection::put(&mut coll, "c", b"3").unwrap();
    Collection::put(&mut coll, "d", b"4").unwrap();
    Collection::put(&mut coll, "e", b"5").unwrap();

    let mut iter = coll.iter_range("b", "d", false).unwrap();
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("b".to_string(), b"2".to_vec())
    );
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("c".to_string(), b"3".to_vec())
    );
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("d".to_string(), b"4".to_vec())
    );
    assert!(iter.next().is_none());

    let mut iter = coll.iter_range("b", "d", true).unwrap();
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("d".to_string(), b"4".to_vec())
    );
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("c".to_string(), b"3".to_vec())
    );
    assert_eq!(
        iter.next().unwrap().unwrap(),
        ("b".to_string(), b"2".to_vec())
    );
    assert!(iter.next().is_none());
}

#[test]
fn test_rocksdb_machine_specs() {
    let temp_dir = tempdir().unwrap();

    for (ram, cores) in [(512, 1), (2048, 2), (8192, 4), (32768, 8)] {
        let db_path = temp_dir.path().join(format!("spec_{}_{}", ram, cores));
        let spec = MachineSpec::Custom {
            ram_mb: ram,
            cpu_cores: cores,
        };
        let manager = RocksDbManager::new(&db_path, false, Some(spec)).unwrap();
        let mut coll = manager.create_collection("c", "p").unwrap();
        Collection::put(&mut coll, "k", b"v").unwrap();
    }
}

#[test]
fn test_rocksdb_new_dir_creation_fails() {
    let temp_dir = tempdir().unwrap();
    let parent = temp_dir.path().join("no_write");
    std::fs::create_dir(&parent).unwrap();

    let mut perms = std::fs::metadata(&parent).unwrap().permissions();
    perms.set_readonly(true);
    std::fs::set_permissions(&parent, perms).unwrap();

    let db_path = parent.join("db");
    let result = RocksDbManager::new(&db_path, false, None);
    assert!(result.is_err());

    // Restore permissions so tempdir cleanup works.
    let mut perms = std::fs::metadata(&parent).unwrap().permissions();
    perms.set_readonly(false);
    std::fs::set_permissions(&parent, perms).unwrap();
}

#[test]
fn test_rocksdb_new_open_fails() {
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let path = temp_file.path().to_path_buf();
    let result = RocksDbManager::new(&path, false, None);
    assert!(result.is_err());
}
