// Copyright(C) 2021, Mysten Labs
// SPDX-License-Identifier: Apache-2.0
use super::*;

fn temp_dir() -> std::path::PathBuf {
    tempfile::tempdir()
        .expect("Failed to open temporary directory")
        .into_path()
}

#[test]
fn test_open() {
    let _db = DBMap::<u32, String>::open(temp_dir(), None, None).expect("Failed to open storage");
}

#[test]
fn test_reopen() {
    let arc = {
        let db =
            DBMap::<u32, String>::open(temp_dir(), None, None).expect("Failed to open storage");
        db.insert(&123456789, &"123456789".to_string())
            .expect("Failed to insert");
        db.rocksdb
    };
    let db = DBMap::<u32, String>::reopen(&arc, None).expect("Failed to re-open storage");
    assert!(db
        .contains_key(&123456789)
        .expect("Failed to retrieve item in storage"));
}

#[test]
fn test_wrong_reopen() {
    let rocks = open_cf(temp_dir(), None, &["foo", "bar", "baz"]).unwrap();
    let db = DBMap::<u8, u8>::reopen(&rocks, Some("quux"));
    assert!(db.is_err());
}

#[test]
fn test_contains_key() {
    let db = DBMap::open(temp_dir(), None, None).expect("Failed to open storage");

    db.insert(&123456789, &"123456789".to_string())
        .expect("Failed to insert");
    assert!(db
        .contains_key(&123456789)
        .expect("Failed to call contains key"));
    assert!(!db
        .contains_key(&000000000)
        .expect("Failed to call contains key"));
}

#[test]
fn test_get() {
    let db = DBMap::open(temp_dir(), None, None).expect("Failed to open storage");

    db.insert(&123456789, &"123456789".to_string())
        .expect("Failed to insert");
    assert_eq!(
        Some("123456789".to_string()),
        db.get(&123456789).expect("Failed to get")
    );
    assert_eq!(None, db.get(&000000000).expect("Failed to get"));
}

#[test]
fn test_remove() {
    let db = DBMap::open(temp_dir(), None, None).expect("Failed to open storage");

    db.insert(&123456789, &"123456789".to_string())
        .expect("Failed to insert");
    assert!(db.get(&123456789).expect("Failed to get").is_some());

    db.remove(&123456789).expect("Failed to remove");
    assert!(db.get(&123456789).expect("Failed to get").is_none());
}

#[test]
fn test_iter() {
    let db = DBMap::open(temp_dir(), None, None).expect("Failed to open storage");

    db.insert(&123456789, &"123456789".to_string())
        .expect("Failed to insert");

    let mut iter = db.iter();
    assert_eq!(Some((123456789, "123456789".to_string())), iter.next());
    assert_eq!(None, iter.next());
}

#[test]
fn test_keys() {
    let db = DBMap::open(temp_dir(), None, None).expect("Failed to open storage");

    db.insert(&123456789, &"123456789".to_string())
        .expect("Failed to insert");

    let mut keys = db.keys();
    assert_eq!(Some(123456789), keys.next());
    assert_eq!(None, keys.next());
}

#[test]
fn test_values() {
    let db = DBMap::open(temp_dir(), None, None).expect("Failed to open storage");

    db.insert(&123456789, &"123456789".to_string())
        .expect("Failed to insert");

    let mut values = db.values();
    assert_eq!(Some("123456789".to_string()), values.next());
    assert_eq!(None, values.next());
}

#[test]
fn test_insert_batch() {
    let db = DBMap::open(temp_dir(), None, None).expect("Failed to open storage");
    let keys_vals = (1..100).map(|i| (i, i.to_string()));
    let insert_batch = db
        .batch()
        .insert_batch(&db, keys_vals.clone())
        .expect("Failed to batch insert");
    let _ = insert_batch.write().expect("Failed to execute batch");
    for (k, v) in keys_vals {
        let val = db.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }
}

#[test]
fn test_insert_batch_across_cf() {
    let rocks = open_cf(temp_dir(), None, &["First_CF", "Second_CF"]).unwrap();

    let db_cf_1 = DBMap::reopen(&rocks, Some("First_CF")).expect("Failed to open storage");
    let keys_vals_1 = (1..100).map(|i| (i, i.to_string()));

    let db_cf_2 = DBMap::reopen(&rocks, Some("Second_CF")).expect("Failed to open storage");
    let keys_vals_2 = (1000..1100).map(|i| (i, i.to_string()));

    let batch = db_cf_1
        .batch()
        .insert_batch(&db_cf_1, keys_vals_1.clone())
        .expect("Failed to batch insert")
        .insert_batch(&db_cf_2, keys_vals_2.clone())
        .expect("Failed to batch insert");

    let _ = batch.write().expect("Failed to execute batch");
    for (k, v) in keys_vals_1 {
        let val = db_cf_1.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }

    for (k, v) in keys_vals_2 {
        let val = db_cf_2.get(&k).expect("Failed to get inserted key");
        assert_eq!(Some(v), val);
    }
}

#[test]
fn test_insert_batch_across_different_db() {
    let rocks = open_cf(temp_dir(), None, &["First_CF", "Second_CF"]).unwrap();
    let rocks2 = open_cf(temp_dir(), None, &["First_CF", "Second_CF"]).unwrap();

    let db_cf_1 = DBMap::reopen(&rocks, Some("First_CF")).expect("Failed to open storage");
    let keys_vals_1 = (1..100).map(|i| (i, i.to_string()));

    let db_cf_2 = DBMap::reopen(&rocks2, Some("Second_CF")).expect("Failed to open storage");
    let keys_vals_2 = (1000..1100).map(|i| (i, i.to_string()));

    assert!(db_cf_1
        .batch()
        .insert_batch(&db_cf_1, keys_vals_1.clone())
        .expect("Failed to batch insert")
        .insert_batch(&db_cf_2, keys_vals_2.clone())
        .is_err());
}

#[test]
fn test_delete_batch() {
    let db = DBMap::<i32, String>::open(temp_dir(), None, None).expect("Failed to open storage");

    let keys_vals = (1..100).map(|i| (i, i.to_string()));
    let insert_batch = db
        .batch()
        .insert_batch(&db, keys_vals)
        .expect("Failed to batch insert");

    // delete the odd-index keys
    let deletion_keys = (1..100).step_by(2);
    let delete_batch = insert_batch
        .delete_batch(&db, deletion_keys)
        .expect("Failed to batch delete");

    let _ = delete_batch.write().expect("Failed to execute batch");

    for k in db.keys() {
        assert_eq!(k % 2, 0);
    }
}

#[test]
fn test_delete_range() {
    let db = DBMap::open(temp_dir(), None, None).expect("Failed to open storage");

    // Note that the last element is (100, "100".to_owned()) here
    let keys_vals = (0..101).map(|i| (i, i.to_string()));
    let insert_batch = db
        .batch()
        .insert_batch(&db, keys_vals)
        .expect("Failed to batch insert");

    let delete_range_batch = insert_batch
        .delete_range(&db, &50, &100)
        .expect("Failed to delete range");

    let _ = delete_range_batch.write().expect("Failed to execute batch");

    for k in 0..50 {
        assert!(db.contains_key(&k).expect("Failed to query legal key"),);
    }
    for k in 50..100 {
        assert!(!db.contains_key(&k).expect("Failed to query legal key"));
    }

    // range operator is not inclusive of to
    assert!(db.contains_key(&100).expect("Failed to query legel key"));
}
