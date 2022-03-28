// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::SequenceNumber;
use crypto::traits::VerifyingKey;
use primary::Round;
use rocksdb::{ColumnFamilyDescriptor, DBCompressionType, DBWithThreadMode, MultiThreaded};
use std::{collections::HashMap, path::Path, sync::Arc};
use store::{
    rocks::{DBMap, TypedStoreError},
    traits::Map,
};

/// The persistent storage of the sequencer.
pub struct ConsensusStore<PublicKey: VerifyingKey> {
    /// The latest committed round of each validator.
    last_committed: DBMap<PublicKey, Round>,
    /// The global consensus index.
    consensus_index: DBMap<u64, SequenceNumber>,
}

impl<PublicKey: VerifyingKey> ConsensusStore<PublicKey> {
    /// The address at which we store the consensus index (rocksdb is a key-value store).
    const CONSENSUS_INDEX_ADDR: u64 = 0;

    /// Open the consensus store.
    pub fn open<P: AsRef<Path>>(path: P, db_options: Option<rocksdb::Options>) -> Self {
        let row_cache = rocksdb::Cache::new_lru_cache(1_000_000).expect("Cache is ok");
        let mut options = db_options.unwrap_or_default();
        options.set_row_cache(&row_cache);
        options.set_table_cache_num_shard_bits(10);
        options.set_compression_type(DBCompressionType::None);

        let db = Self::open_cf_opts(
            &path,
            Some(options.clone()),
            &[("last_committed", &options), ("consensus_index", &options)],
        )
        .expect("Cannot open DB.");

        Self {
            last_committed: DBMap::reopen(&db, Some("last_committed")).expect("Cannot open CF."),
            consensus_index: DBMap::reopen(&db, Some("consensus_index")).expect("Cannot open CF."),
        }
    }

    /// Helper function to open the store.
    fn open_cf_opts<P: AsRef<Path>>(
        path: P,
        db_options: Option<rocksdb::Options>,
        opt_cfs: &[(&str, &rocksdb::Options)],
    ) -> Result<Arc<DBWithThreadMode<MultiThreaded>>, TypedStoreError> {
        let mut options = db_options.unwrap_or_default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let mut cfs = DBWithThreadMode::<MultiThreaded>::list_cf(&options, &path)
            .ok()
            .unwrap_or_default();
        for cf_key in opt_cfs.iter().map(|(name, _)| name) {
            let key = (*cf_key).to_owned();
            if !cfs.contains(&key) {
                cfs.push(key);
            }
        }

        Ok(Arc::new(
            DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(
                &options,
                /* primary */ &path.as_ref(),
                opt_cfs
                    .iter()
                    .map(|(name, opts)| ColumnFamilyDescriptor::new(*name, (*opts).clone())),
            )?,
        ))
    }

    /// Persist the consensus state.
    pub fn store_consensus_state(
        &self,
        last_committed: &HashMap<PublicKey, Round>,
        consensus_index: &SequenceNumber,
    ) -> Result<(), TypedStoreError> {
        let mut write_batch = self.last_committed.batch();
        write_batch = write_batch.insert_batch(&self.last_committed, last_committed.iter())?;
        write_batch = write_batch.insert_batch(
            &self.consensus_index,
            std::iter::once((Self::CONSENSUS_INDEX_ADDR, consensus_index)),
        )?;
        write_batch.write()
    }

    /// Load the last committed round of each validator.
    pub fn get_last_committed(&self) -> HashMap<PublicKey, Round> {
        self.last_committed.iter().collect()
    }

    /// Load the (global) consensus index.
    pub fn get_consensus_index(&self) -> Result<SequenceNumber, TypedStoreError> {
        self.consensus_index
            .get(&Self::CONSENSUS_INDEX_ADDR)
            .map(|x| x.unwrap_or_default())
    }
}
