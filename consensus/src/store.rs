// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::SequenceNumber;
use crypto::traits::VerifyingKey;
use primary::{CertificateDigest, Round};
use rocksdb::DBCompressionType;
use std::{collections::HashMap, path::Path};
use store::{
    rocks::{DBMap, TypedStoreError},
    traits::Map,
};

// The datastore column family names.
const LAST_COMMITTED_CF: &str = "last_committed";
const SEQUENCE_CF: &str = "sequence";

/// Convenience type to propagate store errors.
pub type StoreResult<T> = Result<T, TypedStoreError>;

/// The persistent storage of the sequencer.
pub struct ConsensusStore<PublicKey: VerifyingKey> {
    /// The latest committed round of each validator.
    last_committed: DBMap<PublicKey, Round>,
    /// The global consensus sequence.
    sequence: DBMap<SequenceNumber, CertificateDigest>,
}

impl<PublicKey: VerifyingKey> ConsensusStore<PublicKey> {
    /// Open the consensus store.
    pub fn open<P: AsRef<Path>>(path: P, db_options: Option<rocksdb::Options>) -> Self {
        let row_cache = rocksdb::Cache::new_lru_cache(1_000_000).expect("Cache is ok");
        let mut options = db_options.unwrap_or_default();
        options.set_row_cache(&row_cache);
        options.set_table_cache_num_shard_bits(10);
        options.set_compression_type(DBCompressionType::None);

        let db = store::rocks::open_cf_opts(
            &path,
            Some(options.clone()),
            &[(LAST_COMMITTED_CF, &options), (SEQUENCE_CF, &options)],
        )
        .expect("Cannot open DB.");

        Self {
            last_committed: DBMap::reopen(&db, Some(LAST_COMMITTED_CF)).expect("Cannot open CF."),
            sequence: DBMap::reopen(&db, Some(SEQUENCE_CF)).expect("Cannot open CF."),
        }
    }

    /// Persist the consensus state.
    pub fn write_consensus_state(
        &self,
        last_committed: &HashMap<PublicKey, Round>,
        consensus_index: &SequenceNumber,
        certificate_id: &CertificateDigest,
    ) -> Result<(), TypedStoreError> {
        let mut write_batch = self.last_committed.batch();
        write_batch = write_batch.insert_batch(&self.last_committed, last_committed.iter())?;
        write_batch = write_batch.insert_batch(
            &self.sequence,
            std::iter::once((consensus_index, certificate_id)),
        )?;
        write_batch.write()
    }

    /// Load the last committed round of each validator.
    pub fn read_last_committed(&self) -> HashMap<PublicKey, Round> {
        self.last_committed.iter().collect()
    }

    /// Load the certificate digests sequenced at a specific indices.
    pub fn read_sequenced_certificates(
        &self,
        indices: &[SequenceNumber],
    ) -> StoreResult<Vec<Option<CertificateDigest>>> {
        self.sequence.multi_get(indices)
    }

    /// Load the last (ie. the highest) consensus index associated to a certificate.
    pub fn read_last_consensus_index(&self) -> StoreResult<SequenceNumber> {
        Ok(self
            .sequence
            .keys()
            .skip_prior_to(&SequenceNumber::MAX)?
            .next()
            .unwrap_or_default())
    }
}
