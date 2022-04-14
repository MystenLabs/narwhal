// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
mod batch_loader;
mod errors;
mod executor;
mod state;
mod subscriber;

#[cfg(test)]
#[path = "tests/fixtures.rs"]
mod fixtures;

#[cfg(test)]
#[path = "tests/execution_state.rs"]
mod execution_state;

#[cfg(test)]
#[path = "tests/sequencer.rs"]
mod sequencer;

pub use errors::ExecutionStateError;
pub use state::ExecutionIndices;

use crate::{
    batch_loader::BatchLoader,
    errors::{SubscriberError, SubscriberResult},
    executor::Executor,
    subscriber::Subscriber,
};
use async_trait::async_trait;
use config::Committee;
use consensus::{ConsensusOutput, ConsensusSyncRequest};
use crypto::traits::VerifyingKey;
use primary::BatchDigest;
use serde::de::DeserializeOwned;
use std::{fmt::Debug, path::Path, sync::Arc};
use store::{
    reopen,
    rocks::{open_cf, DBMap},
    Store,
};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};
use tracing::info;
use worker::SerializedBatchMessage;

/// Default inter-task channel size.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

/// Convenience type representing a serialized transaction.
pub type SerializedTransaction = Vec<u8>;

/// The datastore column family names.
const BATCHES_CF: &str = "batches";

#[async_trait]
pub trait ExecutionState {
    /// The type of the transaction to process.
    type Transaction: DeserializeOwned + Send + Debug;

    /// The error type to return in case something went wrong during execution.
    type Error: ExecutionStateError;

    /// Execute the transaction and atomically persist the consensus index.
    async fn handle_consensus_transaction(
        &self,
        execution_indices: ExecutionIndices,
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
    async fn load_execution_indices(&self) -> Result<ExecutionIndices, Self::Error>;
}

/// Spawn a new client subscriber.
pub async fn spawn_client_subscriber<State, PublicKey>(
    name: PublicKey,
    committee: Committee<PublicKey>,
    store_path: &Path,
    execution_state: Arc<State>,
    rx_consensus: Receiver<ConsensusOutput<PublicKey>>,
    tx_consensus: Sender<ConsensusSyncRequest>,
    tx_output: Sender<SubscriberResult<SerializedTransaction>>,
) -> SubscriberResult<(
    JoinHandle<SubscriberResult<()>>,
    JoinHandle<SubscriberResult<()>>,
    JoinHandle<SubscriberResult<()>>,
)>
where
    State: ExecutionState + Send + Sync + 'static,
    PublicKey: VerifyingKey,
{
    let (tx_batch_loader, rx_batch_loader) = channel(DEFAULT_CHANNEL_SIZE);
    let (tx_executor, rx_executor) = channel(DEFAULT_CHANNEL_SIZE);

    // Ensure there is a single consensus client modifying the execution state.
    ensure!(
        execution_state.ask_consensus_write_lock(),
        SubscriberError::OnlyOneConsensusClientPermitted
    );

    // Load the subscriber state from storage.
    let execution_indices = execution_state.load_execution_indices().await?;
    let next_consensus_index = execution_indices.next_certificate_index;

    // (Re)open a temporary store to hold downloaded batches.
    let rocksdb = open_cf(store_path, None, &[BATCHES_CF]).expect("Failed creating database");
    let batch_map = reopen!(&rocksdb, BATCHES_CF;<BatchDigest, SerializedBatchMessage>);
    let store = Store::new(batch_map);

    // Spawn the subscriber.
    let subscriber_handle = Subscriber::<PublicKey>::spawn(
        store.clone(),
        rx_consensus,
        tx_consensus,
        tx_batch_loader,
        tx_executor,
        next_consensus_index,
    );

    // Spawn the executor.
    let executor_handle = Executor::<State, PublicKey>::spawn(
        store.clone(),
        execution_state,
        /* rx_subscriber */ rx_executor,
        tx_output,
    );

    // Spawn the batch loader.
    let worker_addresses = committee
        .authorities
        .iter()
        .find(|(x, _)| *x == &name)
        .map(|(_, authority)| authority)
        .expect("Our public key is not in the committee")
        .workers
        .iter()
        .map(|(id, x)| (*id, x.worker_to_worker))
        .collect();
    let batch_loader_handle = BatchLoader::spawn(store, rx_batch_loader, worker_addresses);

    // Return the handle.
    info!("Consensus subscriber successfully started");
    Ok((subscriber_handle, executor_handle, batch_loader_handle))
}
