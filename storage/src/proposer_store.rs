// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use config::WorkerId;
use std::iter;
use store::{rocks::DBMap, Map};
use types::{BatchDigest, StoreResult};

/// The storage for the proposer
#[derive(Clone)]
pub struct ProposerStore {
    /// Holds the BatchDigest, and the corresponding Worker ID of the worker that has the batch.
    digests: DBMap<BatchDigest, WorkerId>,
}

impl ProposerStore {
    pub fn new(digests: DBMap<BatchDigest, WorkerId>) -> ProposerStore {
        Self { digests }
    }

    /// Inserts a digest to the store
    pub fn write_digest(&self, digest: BatchDigest, worker_id: WorkerId) -> StoreResult<()> {
        let mut batch = self.digests.batch();

        // write the certificate by its id
        batch = batch.insert_batch(&self.digests, iter::once((digest, worker_id)))?;

        // execute the batch (atomically) and return the result
        batch.write()
    }

    /// Get all the digests in the store
    pub fn get_digests(&self) -> StoreResult<Vec<(BatchDigest, WorkerId)>> {
        let mut digests: Vec<(BatchDigest, WorkerId)> = Vec::new();

        let res = self.digests.iter();
        for (batch_digest, worker_id) in res {
            digests.push((batch_digest, worker_id));
        }

        Ok(digests)
    }

    /// Clear all the digests
    pub fn clear_digests(&self) -> StoreResult<()> {
        self.digests.clear()
    }
}
