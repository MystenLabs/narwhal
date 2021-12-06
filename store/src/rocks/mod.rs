mod iter;
mod keys;
mod values;

use crate::traits::Map;
use bincode::Options;
use eyre::Result;
use rocksdb::WriteBatch;
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, path::Path, sync::Arc};

use self::{iter::Iter, keys::Keys, values::Values};

#[cfg(test)]
mod tests;

/// An interface to a rocksDB database, keyed by a columnfamily
#[derive(Clone, Debug)]
pub struct DBMap<K, V> {
    pub(super) rocksdb: Arc<rocksdb::DB>,
    pub(super) _phantom: PhantomData<(K, V)>,
    pub(super) opt_cf: Option<String>,
}

impl<K, V> DBMap<K, V> {
    pub fn open<P: AsRef<Path>>(
        path: P,
        db_options: Option<rocksdb::Options>,
        opt_cf: Option<String>,
    ) -> Result<Self> {
        // Customize database options.
        let mut options = db_options.unwrap_or_default();
        let mut cfs = rocksdb::DB::list_cf(&options, &path)
            .ok()
            .unwrap_or_default();
        if let Some(ref col) = opt_cf {
            if !cfs.contains(col) {
                cfs.push(col.clone());
            }
        }

        let primary = path.as_ref().to_path_buf();
        let rocksdb = {
            options.create_if_missing(true);
            options.create_missing_column_families(true);
            if !cfs.is_empty() {
                Arc::new(rocksdb::DB::open_cf(&options, &primary, &cfs)?)
            } else {
                Arc::new(rocksdb::DB::open(&options, &primary)?)
            }
        };

        Ok(DBMap {
            rocksdb,
            _phantom: PhantomData,
            opt_cf,
        })
    }

    fn cf_handle(&self) -> Option<&rocksdb::ColumnFamily> {
        self.opt_cf
            .as_ref()
            .and_then(|cf| self.rocksdb.cf_handle(cf))
    }
}

impl<K, V> DBMap<K, V> {
    pub fn batch(&self) -> DBBatch<'_, K, V> {
        DBBatch::new(self)
    }
}

/// Provides a mutable struct to form a collection of database write operations, and execute them
pub struct DBBatch<'a, K, V> {
    target_db: &'a DBMap<K, V>,
    batch: WriteBatch,
}

impl<'a, K, V> DBBatch<'a, K, V> {
    pub fn new(db: &'a DBMap<K, V>) -> Self {
        DBBatch {
            target_db: db,
            batch: WriteBatch::default(),
        }
    }
}

impl<'a, K, V> DBBatch<'a, K, V> {
    /// Consume the batch and write its operations to the database
    pub fn write(self) -> Result<()> {
        self.target_db.rocksdb.write(self.batch)?;
        Ok(())
    }
}

impl<'a, K: Serialize, V> DBBatch<'a, K, V> {
    /// Deletes a set of keys given as an iterator
    #[allow(clippy::map_collect_result_unit)] // we don't want a mutable argument
    pub fn delete_batch<T: Iterator<Item = K>>(mut self, purged_vals: T) -> Result<Self> {
        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();
        purged_vals
            .map(|k| {
                let k_buf = config.serialize(&k)?;
                if let Some(cf) = self.target_db.cf_handle() {
                    self.batch.delete_cf(cf, k_buf);
                } else {
                    self.batch.delete(k_buf);
                }

                Ok(())
            })
            .collect::<Result<_, eyre::Error>>()?;
        Ok(self)
    }

    /// Deletes a range of keys between `from` (inclusive) and `to` (non-inclusive)
    pub fn delete_range(mut self, from: &K, to: &K) -> Result<Self> {
        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();
        let from_buf = config.serialize(from)?;
        let to_buf = config.serialize(to)?;

        if let Some(cf) = self.target_db.cf_handle() {
            self.batch.delete_range_cf(cf, from_buf, to_buf)
        } else {
            self.batch.delete_range(from_buf, to_buf);
        }
        Ok(self)
    }
}

impl<'a, K: Serialize, V: Serialize> DBBatch<'a, K, V> {
    /// inserts a range of (key, value) pairs given as an iterator
    #[allow(clippy::map_collect_result_unit)] // we don't want a mutable argument
    pub fn insert_batch<T: Iterator<Item = (K, V)>>(mut self, new_vals: T) -> Result<Self> {
        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();
        new_vals
            .map(|(ref k, ref v)| {
                let k_buf = config.serialize(k)?;
                let v_buf = bincode::serialize(v)?;
                if let Some(cf) = self.target_db.cf_handle() {
                    self.batch.put_cf(cf, k_buf, v_buf);
                } else {
                    self.batch.put(k_buf, v_buf);
                }
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
        let res = if let Some(cf) = self.cf_handle() {
            self.rocksdb.get_pinned_cf(cf, &key_buf)
        } else {
            self.rocksdb.get_pinned(&key_buf)
        }?;
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

        let _ = if let Some(cf) = self.cf_handle() {
            self.rocksdb.put_cf(cf, &key_buf, &value_buf)
        } else {
            self.rocksdb.put(&key_buf, &value_buf)
        }?;
        Ok(())
    }

    fn remove(&self, key: &K) -> Result<()> {
        let config = bincode::DefaultOptions::new()
            .with_big_endian()
            .with_fixint_encoding();
        let key_buf = config.serialize(key)?;

        let _ = if let Some(cf) = self.cf_handle() {
            self.rocksdb.delete_cf(cf, &key_buf)
        } else {
            self.rocksdb.delete(&key_buf)
        }?;
        Ok(())
    }

    fn iter(&'a self) -> Self::Iterator {
        let mut db_iter = if let Some(cf) = self.cf_handle() {
            self.rocksdb.raw_iterator_cf(cf)
        } else {
            self.rocksdb.raw_iterator()
        };
        db_iter.seek_to_first();

        Iter::new(db_iter)
    }

    fn keys(&'a self) -> Self::Keys {
        let mut db_iter = if let Some(cf) = self.cf_handle() {
            self.rocksdb.raw_iterator_cf(cf)
        } else {
            self.rocksdb.raw_iterator()
        };
        db_iter.seek_to_first();

        Keys::new(db_iter)
    }

    fn values(&'a self) -> Self::Values {
        let mut db_iter = if let Some(cf) = self.cf_handle() {
            self.rocksdb.raw_iterator_cf(cf)
        } else {
            self.rocksdb.raw_iterator()
        };
        db_iter.seek_to_first();

        Values::new(db_iter)
    }
}
