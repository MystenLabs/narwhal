// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{ExecutionIndices, ExecutionState, ExecutionStateError, SingleExecutionState};
use async_trait::async_trait;
use consensus::ConsensusOutput;

use futures::executor::block_on;
use std::path::Path;
use store::{
    reopen,
    rocks::{open_cf, DBMap},
    Store,
};
use thiserror::Error;

/// A malformed transaction.
pub const MALFORMED_TRANSACTION: <TestState as ExecutionState>::Transaction = 400;

/// A special transaction that makes the executor engine crash.
pub const KILLER_TRANSACTION: <TestState as ExecutionState>::Transaction = 500;

/// A dumb execution state for testing.
pub struct TestState {
    indices_store: Store<u64, ExecutionIndices>,
}

impl std::fmt::Debug for TestState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", block_on(self.get_execution_indices()))
    }
}

impl Default for TestState {
    fn default() -> Self {
        Self::new(tempfile::tempdir().unwrap().path())
    }
}

#[async_trait]
impl ExecutionState for TestState {
    type Transaction = u64;
    type Error = TestStateError;

    fn ask_consensus_write_lock(&self) -> bool {
        true
    }

    fn release_consensus_write_lock(&self) {}
}

#[async_trait]
impl SingleExecutionState for TestState {
    type Outcome = Vec<u8>;

    async fn handle_consensus_transaction(
        &self,
        _consensus_output: &ConsensusOutput,
        execution_indices: ExecutionIndices,
        transaction: Self::Transaction,
    ) -> Result<Self::Outcome, Self::Error> {
        if transaction == MALFORMED_TRANSACTION {
            Err(Self::Error::ClientError)
        } else if transaction == KILLER_TRANSACTION {
            Err(Self::Error::ServerError)
        } else {
            self.indices_store
                .write(Self::INDICES_ADDRESS, execution_indices)
                .await;
            Ok(Vec::default())
        }
    }

    async fn load_execution_indices(&self) -> Result<ExecutionIndices, Self::Error> {
        let indices = self
            .indices_store
            .read(Self::INDICES_ADDRESS)
            .await
            .unwrap()
            .unwrap_or_default();
        Ok(indices)
    }
}

impl TestState {
    /// The address at which to store the indices (rocksdb is a key-value store).
    pub const INDICES_ADDRESS: u64 = 14;

    /// Create a new test state.
    pub fn new(store_path: &Path) -> Self {
        const INDICES_CF: &str = "test_state_indices";
        let rocksdb = open_cf(store_path, None, &[INDICES_CF]).unwrap();
        let indices_map = reopen!(&rocksdb, INDICES_CF;<u64, ExecutionIndices>);
        Self {
            indices_store: Store::new(indices_map),
        }
    }

    /// Load the execution indices.
    pub async fn get_execution_indices(&self) -> ExecutionIndices {
        self.load_execution_indices().await.unwrap()
    }
}

#[derive(Debug, Error, Clone)]
pub enum TestStateError {
    #[error("Something went wrong in the authority")]
    ServerError,

    #[error("The client made something bad")]
    ClientError,
}

#[async_trait]
impl ExecutionStateError for TestStateError {
    fn node_error(&self) -> bool {
        match self {
            Self::ServerError => true,
            Self::ClientError => false,
        }
    }
}
