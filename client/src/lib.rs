// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
mod batch_loader;
mod errors;
mod state;
mod subscriber;
mod utils;

pub use errors::AuthorityStateError;
pub use state::SubscriberState;

use crate::{batch_loader::BatchLoader, errors::SubscriberResult, subscriber::Subscriber};
use async_trait::async_trait;
use config::Committee;
use crypto::traits::VerifyingKey;
use primary::BatchDigest;
use serde::de::DeserializeOwned;
use std::{net::SocketAddr, sync::Arc};
use store::{
    reopen,
    rocks::{open_cf, DBMap},
    Store,
};
use tokio::{sync::mpsc::channel, task::JoinHandle};
use worker::SerializedBatchMessage;

/// Default inter-task channel size.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

/// The datastore column family names.
const BATCHES_CF: &str = "batches";

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

/// Spawn a new client subscriber.ca
pub async fn spawn_client_subscriber<ExecutionState, PublicKey>(
    name: PublicKey,
    committee: Committee<PublicKey>,
    address: SocketAddr,
    store_path: &str,
    execution_state: Arc<ExecutionState>,
) -> SubscriberResult<(
    JoinHandle<SubscriberResult<()>>,
    JoinHandle<SubscriberResult<()>>,
)>
where
    ExecutionState: AuthorityState + Send + Sync + 'static,
    PublicKey: VerifyingKey,
{
    let (tx_batch_loader, rx_batch_loader) = channel(DEFAULT_CHANNEL_SIZE);

    // Create a temporary store to hold downloaded batches.
    let rocksdb = open_cf(store_path, None, &[BATCHES_CF]).expect("Failed creating database");
    let batch_map = reopen!(&rocksdb, BATCHES_CF;<BatchDigest, SerializedBatchMessage>);
    let store = Store::new(batch_map);

    // Spawn the subscriber.
    let subscriber = Subscriber::<ExecutionState, PublicKey>::new(
        address,
        store.clone(),
        execution_state,
        tx_batch_loader,
    )
    .await?;
    let subscriber_handle = Subscriber::spawn(subscriber);

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
    Ok((subscriber_handle, batch_loader_handle))
}
