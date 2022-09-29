// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
mod errors;
mod state;
mod subscriber;

mod metrics;
mod notifier;

pub use errors::{SubscriberError, SubscriberResult};
pub use state::ExecutionIndices;
use tracing::info;

use crate::metrics::ExecutorMetrics;
use crate::notifier::Notifier;
use async_trait::async_trait;
use config::{Committee, SharedWorkerCache};
use consensus::ConsensusOutput;
use crypto::PublicKey;
use network::P2pNetwork;

use prometheus::Registry;

use std::sync::Arc;
use storage::CertificateStore;

use crate::subscriber::spawn_subscriber;
use tokio::sync::oneshot;
use tokio::{sync::watch, task::JoinHandle};
use types::{
    metered_channel, CertificateDigest, ConsensusStore, ReconfigureNotification, SequenceNumber,
};

/// Convenience type representing a serialized transaction.
pub type SerializedTransaction = Vec<u8>;

/// Convenience type representing a serialized transaction digest.
pub type SerializedTransactionDigest = u64;

#[async_trait]
pub trait ExecutionState {
    /// Execute the transaction and atomically persist the consensus index. This function
    /// returns an execution outcome that will be output by the executor channel. It may
    /// also return a new committee to reconfigure the system.
    async fn handle_consensus_transaction(
        &self,
        consensus_output: &ConsensusOutput,
        execution_indices: ExecutionIndices,
        transaction: Vec<u8>,
    );

    /// Load the last consensus index from storage.
    async fn load_execution_indices(&self) -> ExecutionIndices;
}

/// The output of the executor.
pub type ExecutorOutput = (SubscriberResult<()>, SerializedTransaction);

/// A client subscribing to the consensus output and executing every transaction.
pub struct Executor;

impl Executor {
    /// Spawn a new client subscriber.
    pub fn spawn<State>(
        name: PublicKey,
        network: oneshot::Receiver<P2pNetwork>,
        worker_cache: SharedWorkerCache,
        committee: Committee,

        execution_state: State,
        tx_reconfigure: &watch::Sender<ReconfigureNotification>,
        rx_consensus: metered_channel::Receiver<ConsensusOutput>,
        registry: &Registry,
        restored_consensus_output: Vec<ConsensusOutput>,
    ) -> SubscriberResult<Vec<JoinHandle<()>>>
    where
        State: ExecutionState + Send + Sync + 'static,
    {
        let metrics = ExecutorMetrics::new(registry);

        let (tx_notifier, rx_notifier) =
            metered_channel::channel(primary::CHANNEL_CAPACITY, &metrics.tx_executor);

        // We expect this will ultimately be needed in the `Core` as well as the `Subscriber`.
        let arc_metrics = Arc::new(metrics);

        // Spawn the subscriber.
        let subscriber_handle = spawn_subscriber(
            name,
            network,
            worker_cache,
            committee,
            tx_reconfigure.subscribe(),
            rx_consensus,
            tx_notifier,
            arc_metrics,
            restored_consensus_output,
        );

        let notifier_handler = Notifier::spawn(rx_notifier, execution_state);

        // Return the handle.
        info!("Consensus subscriber successfully started");

        Ok(vec![subscriber_handle, notifier_handler])
    }
}

pub async fn get_restored_consensus_output<State: ExecutionState>(
    consensus_store: Arc<ConsensusStore>,
    certificate_store: CertificateStore,
    execution_state: &State,
) -> Result<Vec<ConsensusOutput>, SubscriberError> {
    let mut restored_consensus_output = Vec::new();
    let consensus_next_index = consensus_store
        .read_last_consensus_index()
        .map_err(SubscriberError::StoreError)?;

    let next_cert_index = execution_state
        .load_execution_indices()
        .await
        .next_certificate_index;

    if next_cert_index < consensus_next_index {
        let missing = consensus_store
            .read_sequenced_certificates(&(next_cert_index..=consensus_next_index - 1))?
            .iter()
            .zip(next_cert_index..consensus_next_index)
            .filter_map(|(c, seq)| c.map(|digest| (digest, seq)))
            .collect::<Vec<(CertificateDigest, SequenceNumber)>>();

        for (cert_digest, seq) in missing {
            if let Some(cert) = certificate_store.read(cert_digest).unwrap() {
                // Save the missing sequence / cert pair as ConsensusOutput to re-send to the executor.
                restored_consensus_output.push(ConsensusOutput {
                    certificate: cert,
                    consensus_index: seq,
                })
            }
        }
    }
    Ok(restored_consensus_output)
}

#[async_trait]
impl<T: ExecutionState + 'static + Send + Sync> ExecutionState for Arc<T> {
    async fn handle_consensus_transaction(
        &self,
        consensus_output: &ConsensusOutput,
        execution_indices: ExecutionIndices,
        transaction: Vec<u8>,
    ) {
        self.as_ref()
            .handle_consensus_transaction(consensus_output, execution_indices, transaction)
            .await
    }

    async fn load_execution_indices(&self) -> ExecutionIndices {
        self.as_ref().load_execution_indices().await
    }
}
