// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{ExecutionIndices, ExecutionState, ExecutionStateError};
use async_trait::async_trait;
use futures::executor::block_on;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

/// A malformed transaction.
pub const MALFORMED_TRANSACTION: <TestState as ExecutionState>::Transaction = 400;

/// A special transaction that makes the executor engine crash.
pub const KILLER_TRANSACTION: <TestState as ExecutionState>::Transaction = 500;

/// A dumb execution state for testing.
#[derive(Default)]
pub struct TestState {
    execution_indices: Arc<RwLock<ExecutionIndices>>,
}

impl std::fmt::Debug for TestState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", block_on(self.execution_indices.read()))
    }
}

#[async_trait]
impl ExecutionState for TestState {
    type Transaction = u64;
    type Error = TestStateError;

    async fn handle_consensus_transaction(
        &self,
        execution_indices: ExecutionIndices,
        transaction: Self::Transaction,
    ) -> Result<(), Self::Error> {
        if transaction == MALFORMED_TRANSACTION {
            Err(Self::Error::ClientError)
        } else if transaction == KILLER_TRANSACTION {
            Err(Self::Error::ServerError)
        } else {
            let mut guard = self.execution_indices.write().await;
            *guard = execution_indices;
            Ok(())
        }
    }

    fn ask_consensus_write_lock(&self) -> bool {
        true
    }

    fn release_consensus_write_lock(&self) {}

    async fn load_execution_indices(&self) -> Result<ExecutionIndices, Self::Error> {
        Ok(ExecutionIndices::default())
    }
}

impl TestState {
    pub async fn get_execution_indices(&self) -> ExecutionIndices {
        self.execution_indices.read().await.clone()
    }
}

#[derive(Debug, Error)]
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

    fn to_string(&self) -> String {
        ToString::to_string(&self)
    }
}
