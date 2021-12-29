// Copyright(C) 2021, Mysten Labs
// SPDX-License-Identifier: Apache-2.0
mod iter;
mod keys;
mod values;

use crate::traits::Map;
use bincode::Options;
use eyre::{eyre, Result};
use rocksdb::WriteBatch;
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, path::Path, sync::Arc};

use self::{iter::Iter, keys::Keys, values::Values};

#[cfg(test)]
mod tests;

/// An interface to a rocksDB database, keyed by a columnfamily
#[derive(Clone, Debug)]
pub struct DBMap<K, V> {
    pub rocksdb: Arc<rocksdb::DB>,
    _phantom: PhantomData<fn(K) -> V>,
    // the rocksDB ColumnFamily under which the map is stored
    cf: String,
}

unsafe impl<K: Send, V: Send> Send for DBMap<K, V> {}

impl<K, V> DBMap<K, V> {
    /// Opens a database from a path, with specific options and an optional column family.
    /// 
    /// This database is used to perform operations on single column family, and parametrizes
    /// all operations in `DBBatch` when writting across column families.
    pub fn open<P: AsRef<Path>>(
        path: P,
        db_options: Option<rocksdb::Options>,
        opt_cf: Option<&str>,
    ) -> Result<Self> {
        let cf_key = opt_cf.unwrap_or(rocksdb::DEFAULT_COLUMN_FAMILY_NAME);
        let cfs = vec![cf_key];
        let rocksdb = open_cf(path, db_options, &cfs)?;

        Ok(DBMap {
            rocksdb,
            _phantom: PhantomData,
            cf: cf_key.to_string(),
        })
    }

    /// Reopens an open database as a typed map operating under a specific column family.
    /// if no column family is passed, the default column family is used.
    ///
    /// ```
    ///    # use store::rocks::*;
    ///    # use std::env::temp_dir;
    ///    /// Open the DB with all needed column families first.
    ///    let rocks = open_cf(temp_dir(), None, &["First_CF", "Second_CF"]).unwrap();
    ///    /// Attach the column families to specific maps.
    ///    let db_cf_1 = DBMap::<u32,u32>::reopen(&rocks, Some("First_CF")).expect("Failed to open storage");
    ///    let db_cf_2 = DBMap::<u32,u32>::reopen(&rocks, Some("Second_CF")).expect("Failed to open storage");
    /// ```
    pub fn reopen(db: &Arc<rocksdb::DB>, opt_cf: Option<&str>) -> Result<Self> {
        let cf_key = opt_cf
            .unwrap_or(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
            .to_owned();

        db.cf_handle(&cf_key)
            .ok_or_else(|| eyre!("required columnfamily is not registered in the database"))?;

        Ok(DBMap {
            rocksdb: db.clone(),
            _phantom: PhantomData,
            cf: cf_key,
        })
    }

    pub fn batch(&self) -> DBBatch {
        DBBatch::new(&self.rocksdb)
    }
}

impl<K, V> AsRef<rocksdb::ColumnFamily> for DBMap<K, V> {
    fn as_ref(&self) -> &rocksdb::ColumnFamily {
        self.rocksdb
            .cf_handle(&self.cf)
            .expect("Map-keying column family should have been checked at DB creation")
    }
}

/// Provides a mutable struct to form a collection of database write operations, and execute them.
///
/// Batching write and delete operations is faster than performing them one by one and ensures their atomicity,
///  ie. they are all written or none is.
/// This is also true of operations across column families in the same database.
///
/// Serializations / Deserialization, and naming of column families is performed by passing a DBMap<K,V>
/// with each operation.
///
/// ```
/// # use store::rocks::*;
/// # use std::env::temp_dir;
/// # use crate::store::Map;
/// let rocks = open_cf(temp_dir(), None, &["First_CF", "Second_CF"]).unwrap();
///
/// let db_cf_1 = DBMap::reopen(&rocks, Some("First_CF"))
///     .expect("Failed to open storage");
/// let keys_vals_1 = (1..100).map(|i| (i, i.to_string()));
///
/// let db_cf_2 = DBMap::reopen(&rocks, Some("Second_CF"))
///     .expect("Failed to open storage");
/// let keys_vals_2 = (1000..1100).map(|i| (i, i.to_string()));
///
/// let batch = db_cf_1
///     .batch()
///     .insert_batch(&db_cf_1, keys_vals_1.clone())
///     .expect("Failed to batch insert")
///     .insert_batch(&db_cf_2, keys_vals_2.clone())
///     .expect("Failed to batch insert");
///
/// let _ = batch.write().expect("Failed to execute batch");
/// for (k, v) in keys_vals_1 {
///     let val = db_cf_1.get(&k).expect("Failed to get inserted key");
///     assert_eq!(Some(v), val);
/// }
///
/// for (k, v) in keys_vals_2 {
///     let val = db_cf_2.get(&k).expect("Failed to get inserted key");
///     assert_eq!(Some(v), val);
/// }
/// ```
///
pub struct DBBatch {
    rocksdb: Arc<rocksdb::DB>,
    batch: WriteBatch,
}

impl DBBatch {
    /// Create a new batch associated with a DB reference.
    ///
    /// Use `open_cf` to get the DB reference or an existing open database.
    pub fn new(dbref: &Arc<rocksdb::DB>) -> Self {
        DBBatch {
            rocksdb: dbref.clone(),
            batch: WriteBatch::default(),
        }
    }

    /// Consume the batch and write its operations to the database
    pub fn write(self) -> Result<()> {
        self.rocksdb.write(self.batch)?;
        Ok(())
    }
}

impl DBBatch {
    /// Deletes a set of keys given as an iterator
    #[allow(clippy::map_collect_result_unit)] // we don't want a mutable argument
    pub fn delete_batch<K: Serialize, T: Iterator<Item = K>, V>(
        mut self,
        db: &DBMap<K, V>,
        purged_vals: T,
    ) -> Result<Self> {
        if !Arc::ptr_eq(&db.rocksdb, &self.rocksdb) {
            return Err(eyre!("a batch can't operate over two distinct databases"));
        }

        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();
        purged_vals
            .map(|k| {
                let k_buf = config.serialize(&k)?;
                self.batch.delete_cf(db.as_ref(), k_buf);

                Ok(())
            })
            .collect::<Result<_, eyre::Error>>()?;
        Ok(self)
    }

    /// Deletes a range of keys between `from` (inclusive) and `to` (non-inclusive)
    pub fn delete_range<'a, K: Serialize, V>(
        mut self,
        db: &'a DBMap<K, V>,
        from: &K,
        to: &K,
    ) -> Result<Self> {
        if !Arc::ptr_eq(&db.rocksdb, &self.rocksdb) {
            return Err(eyre!("a batch can't operate over two distinct databases"));
        }

        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();
        let from_buf = config.serialize(from)?;
        let to_buf = config.serialize(to)?;

        self.batch.delete_range_cf(db.as_ref(), from_buf, to_buf);
        Ok(self)
    }
}

impl DBBatch {
    /// inserts a range of (key, value) pairs given as an iterator
    #[allow(clippy::map_collect_result_unit)] // we don't want a mutable argument
    pub fn insert_batch<K: Serialize, V: Serialize, T: Iterator<Item = (K, V)>>(
        mut self,
        db: &DBMap<K, V>,
        new_vals: T,
    ) -> Result<Self> {
        if !Arc::ptr_eq(&db.rocksdb, &self.rocksdb) {
            return Err(eyre!("a batch can't operate over two distinct databases"));
        }

        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();
        new_vals
            .map(|(ref k, ref v)| {
                let k_buf = config.serialize(k)?;
                let v_buf = bincode::serialize(v)?;
                self.batch.put_cf(db.as_ref(), k_buf, v_buf);
                Ok(())
            })
            .collect::<Result<_, eyre::Error>>()?;
        Ok(self)
    }
}

impl<'a, K, V> Map<'a, K, V> for DBMap<K, V>
where
    K: Serialize + DeserializeOwned,
    V: Serialize + DeserializeOwned,
{
    type Iterator = Iter<'a, K, V>;
    type Keys = Keys<'a, K>;
    type Values = Values<'a, V>;

    fn contains_key(&self, key: &K) -> Result<bool> {
        self.get(key).map(|v| v.is_some())
    }

    fn get(&self, key: &K) -> Result<Option<V>> {
        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();

        let key_buf = config.serialize(key)?;
        let res = self.rocksdb.get_pinned_cf(self.as_ref(), &key_buf)?;
        match res {
            Some(data) => Ok(Some(bincode::deserialize(&data)?)),
            None => Ok(None),
        }
    }

    fn insert(&self, key: &K, value: &V) -> Result<()> {
        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();

        let key_buf = config.serialize(key)?;
        let value_buf = bincode::serialize(value)?;

        let _ = self.rocksdb.put_cf(self.as_ref(), &key_buf, &value_buf)?;
        Ok(())
    }

    fn remove(&self, key: &K) -> Result<()> {
        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();
        let key_buf = config.serialize(key)?;

        let _ = self.rocksdb.delete_cf(self.as_ref(), &key_buf)?;
        Ok(())
    }

    fn iter(&'a self) -> Self::Iterator {
        let mut db_iter = self.rocksdb.raw_iterator_cf(self.as_ref());
        db_iter.seek_to_first();

        Iter::new(db_iter)
    }

    fn keys(&'a self) -> Self::Keys {
        let mut db_iter = self.rocksdb.raw_iterator_cf(self.as_ref());
        db_iter.seek_to_first();

        Keys::new(db_iter)
    }

    fn values(&'a self) -> Self::Values {
        let mut db_iter = self.rocksdb.raw_iterator_cf(self.as_ref());
        db_iter.seek_to_first();

        Values::new(db_iter)
    }
}

/// Opens a database with options, and a number of column families that are created if they do not exist.
pub fn open_cf<P: AsRef<Path>>(
    path: P,
    db_options: Option<rocksdb::Options>,
    opt_cfs: &[&str],
) -> Result<Arc<rocksdb::DB>> {
    // Customize database options
    let mut options = db_options.unwrap_or_default();
    let mut cfs = rocksdb::DB::list_cf(&options, &path)
        .ok()
        .unwrap_or_default();

    // Customize CFs

    for cf_key in opt_cfs.iter() {
        let key = (*cf_key).to_owned();
        if !cfs.contains(&key) {
            cfs.push(key);
        }
    }

    let primary = path.as_ref().to_path_buf();

    let rocksdb = {
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        Arc::new(rocksdb::DB::open_cf(&options, &primary, &cfs)?)
    };
    Ok(rocksdb)
}
