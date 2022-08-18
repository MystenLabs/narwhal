// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
mod core;
mod errors;
mod state;
mod subscriber;

#[cfg(test)]
#[path = "tests/fixtures.rs"]
mod fixtures;

#[cfg(test)]
#[path = "tests/execution_state.rs"]
mod execution_state;

mod metrics;

pub use errors::{ExecutionStateError, SubscriberError, SubscriberResult};
pub use state::ExecutionIndices;

use crate::{core::Core, metrics::ExecutorMetrics, subscriber::Subscriber};
use async_trait::async_trait;
use consensus::ConsensusOutput;
use primary::BlockCommand;
use prometheus::Registry;
use serde::de::DeserializeOwned;
use std::{fmt::Debug, sync::Arc};
use store::Store;
use tokio::{
    sync::{mpsc::Sender, watch},
    task::JoinHandle,
};
use tracing::info;
use types::{metered_channel, Batch, BatchDigest, ReconfigureNotification};

/// Default inter-task channel size.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

/// Convenience type representing a serialized transaction.
pub type SerializedTransaction = Vec<u8>;

/// Convenience type representing a serialized transaction digest.
pub type SerializedTransactionDigest = u64;

#[async_trait]
pub trait ExecutionState {
    /// The type of the transaction to process.
    type Transaction: DeserializeOwned + Send + Debug;

    /// The error type to return in case something went wrong during execution.
    type Error: ExecutionStateError;

    /// The execution outcome to output.
    type Outcome;

    /// Execute the transaction and atomically persist the consensus index. This function
    /// returns an execution outcome that will be output by the executor channel. It may
    /// also return a new committee to reconfigure the system.
    async fn handle_consensus_transaction(
        &self,
        consensus_output: &ConsensusOutput,
        execution_indices: ExecutionIndices,
        transaction: Self::Transaction,
    ) -> Result<Self::Outcome, Self::Error>;

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

/// The output of the executor.
pub type ExecutorOutput<State> = (
    SubscriberResult<<State as ExecutionState>::Outcome>,
    SerializedTransaction,
);

/// A client subscribing to the consensus output and executing every transaction.
pub struct Executor;

impl Executor {
    /// Spawn a new client subscriber.
    pub async fn spawn<State>(
        store: Store<BatchDigest, Batch>,
        execution_state: Arc<State>,
        tx_reconfigure: &watch::Sender<ReconfigureNotification>,
        rx_consensus: metered_channel::Receiver<ConsensusOutput>,
        tx_output: Sender<ExecutorOutput<State>>,
        tx_get_block_commands: metered_channel::Sender<BlockCommand>,
        registry: &Registry,
    ) -> SubscriberResult<Vec<JoinHandle<()>>>
    where
        State: ExecutionState + Send + Sync + 'static,
        State::Outcome: Send + 'static,
        State::Error: Debug,
    {
        let metrics = ExecutorMetrics::new(registry);

        let (tx_executor, rx_executor) =
            metered_channel::channel(DEFAULT_CHANNEL_SIZE, &metrics.tx_executor);

        // Ensure there is a single consensus client modifying the execution state.
        ensure!(
            execution_state.ask_consensus_write_lock(),
            SubscriberError::OnlyOneConsensusClientPermitted
        );

        // Spawn the subscriber.
        let subscriber_handle = Subscriber::spawn(
            store.clone(),
            tx_get_block_commands,
            tx_reconfigure.subscribe(),
            rx_consensus,
            tx_executor,
        );

        // Spawn the executor's core.
        let executor_handle = Core::<State>::spawn(
            store,
            execution_state,
            tx_reconfigure.subscribe(),
            /* rx_subscriber */ rx_executor,
            tx_output,
        );

        // Return the handle.
        info!("Consensus subscriber successfully started");

        Ok(vec![
            subscriber_handle,
            executor_handle,
        ])
    }
}
