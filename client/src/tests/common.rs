// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{AuthorityState, AuthorityStateError, SubscriberState};
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

/// A dumb execution state for testing.
#[derive(Default)]
pub struct TestExecutionState {
    subscriber_state: Arc<RwLock<SubscriberState>>,
}

#[async_trait]
impl AuthorityState for TestExecutionState {
    type Transaction = u64;
    type Error = TestExecutionStateError;

    async fn handle_consensus_transaction(
        &self,
        subscriber_state: SubscriberState,
        transaction: Self::Transaction,
    ) -> Result<(), Self::Error> {
        if transaction == 0 {
            let mut guard = self.subscriber_state.write().await;
            *guard = subscriber_state;
            Ok(())
        } else if transaction == 1 {
            Err(Self::Error::ClientError)
        } else {
            Err(Self::Error::ServerError)
        }
    }

    fn ask_consensus_write_lock(&self) -> bool {
        true
    }

    fn release_consensus_write_lock(&self) {}

    /// Load the last consensus index from storage.
    async fn load_subscriber_state(&self) -> Result<SubscriberState, Self::Error> {
        Ok(SubscriberState::default())
    }
}

#[derive(Debug, Error)]
pub enum TestExecutionStateError {
    #[error("Something went wrong in the authority")]
    ServerError,

    #[error("The client made something bad")]
    ClientError,
}

#[async_trait]
impl AuthorityStateError for TestExecutionStateError {
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
