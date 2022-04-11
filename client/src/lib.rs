// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
pub mod batch_loader;
pub mod client;
pub mod errors;
pub mod utils;

use crate::errors::AuthorityStateError;
use async_trait::async_trait;
use consensus::SequenceNumber;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Default inter-task channel size.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

/// The state of the subscriber keeping track of the transactions that have already been
/// executed. It ensures we do not process twice the same transaction despite crash-recovery.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct SubscriberState {
    /// The index of the latest consensus message we processed (used for crash-recovery).
    pub next_certificate_index: SequenceNumber,
    /// The index of the last batch we executed (used for crash-recovery).
    pub next_batch_index: SequenceNumber,
    /// The index of the last transaction we executed (used for crash-recovery).
    pub next_transaction_index: SequenceNumber,
}

impl SubscriberState {
    /// Compute the next expected indices.
    pub fn next(&mut self, total_batches: usize, total_transactions: usize) {
        let total_batches = total_batches as SequenceNumber;
        let total_transactions = total_transactions as SequenceNumber;

        if self.next_transaction_index + 1 == total_transactions {
            if self.next_batch_index + 1 == total_batches {
                self.next_certificate_index += 1;
            }
            self.next_batch_index = (self.next_batch_index + 1) % total_batches;
        }
        self.next_transaction_index = (self.next_transaction_index + 1) % total_transactions;
    }

    /// Update the state to skip a batch.
    pub fn skip_batch(&mut self, total_batches: usize) {
        let total_batches = total_batches as SequenceNumber;

        if self.next_batch_index + 1 == total_batches {
            self.next_certificate_index += 1;
        }
        self.next_batch_index = (self.next_batch_index + 1) % total_batches;
        self.next_transaction_index = 0;
    }

    /// Check whether the input index is the next expected batch index.
    pub fn check_next_batch_index(&self, batch_index: SequenceNumber) -> bool {
        batch_index == self.next_batch_index
    }

    /// Check whether the input index is the next expected transaction index.
    pub fn check_next_transaction_index(&self, transaction_index: SequenceNumber) -> bool {
        transaction_index == self.next_transaction_index
    }
}

#[async_trait]
pub trait AuthorityState {
    /// The type of the transaction to process.
    type Transaction: DeserializeOwned + Send;

    /// The error type to return in case something went wrong during execution.
    type Error: AuthorityStateError;

    /// Execute the transaction and atomically persist the consensus index.
    async fn handle_consensus_transaction(
        &self,
        subscriber_state: SubscriberState,
        transaction: Self::Transaction,
    ) -> Result<(), Self::Error>;

    /// Simple guardrail ensuring there is a single instance using the state
    /// to call `handle_consensus_transaction`. Many instances may read the state,
    /// or use it for other purposes.
    fn ask_consensus_write_lock(&self) -> bool;

    /// Tell the state that the caller instance is no longer using calling
    //// `handle_consensus_transaction`.
    fn release_consensus_write_lock(&self);

    /// Load the last consensus index from storage.
    async fn load_subscriber_state(&self) -> Result<SubscriberState, Self::Error>;
}
