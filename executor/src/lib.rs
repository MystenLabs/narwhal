// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
mod batch_loader;
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
use multiaddr::{Multiaddr, Protocol};
pub use state::ExecutionIndices;
use storage::CertificateStore;

use crate::{
    batch_loader::BatchLoader, core::Core, metrics::ExecutorMetrics, subscriber::Subscriber,
};
use async_trait::async_trait;
use config::{SharedCommittee, SharedWorkerCache};
use consensus::ConsensusOutput;
use crypto::PublicKey;
use prometheus::Registry;
use serde::de::DeserializeOwned;
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::Debug,
    net::{Ipv4Addr, Ipv6Addr},
    sync::Arc,
};
use store::Store;
use tokio::{
    sync::{mpsc::Sender, watch},
    task::JoinHandle,
};
use tracing::info;
use types::{
    metered_channel, BatchDigest, CertificateDigest, ConsensusStore, ReconfigureNotification,
    SequenceNumber, SerializedBatchMessage,
};

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

    /// Deserialize the message bytes into Transaction type. This allows an implementation of
    /// ExecutionState to customize how to deserialize the message, in case customized
    /// serialization was used when sending the message.
    fn deserialize(bytes: &[u8]) -> Result<Self::Transaction, bincode::Error>;

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
        name: PublicKey,
        committee: SharedCommittee,
        worker_cache: SharedWorkerCache,
        store: Store<BatchDigest, SerializedBatchMessage>,
        execution_state: Arc<State>,
        tx_reconfigure: &watch::Sender<ReconfigureNotification>,
        rx_consensus: metered_channel::Receiver<ConsensusOutput>,
        tx_output: Sender<ExecutorOutput<State>>,
        registry: &Registry,
        restored_consensus_output: Vec<ConsensusOutput>,
    ) -> SubscriberResult<Vec<JoinHandle<()>>>
    where
        State: ExecutionState + Send + Sync + 'static,
        State::Outcome: Send + 'static,
        State::Error: Debug,
    {
        let metrics = ExecutorMetrics::new(registry);

        let (tx_batch_loader, rx_batch_loader) =
            metered_channel::channel(primary::CHANNEL_CAPACITY, &metrics.tx_batch_loader);
        let (tx_executor, rx_executor) =
            metered_channel::channel(primary::CHANNEL_CAPACITY, &metrics.tx_executor);

        // Ensure there is a single consensus client modifying the execution state.
        ensure!(
            execution_state.ask_consensus_write_lock(),
            SubscriberError::OnlyOneConsensusClientPermitted
        );

        // We expect this will ultimately be needed in the `Core` as well as the `Subscriber`.

        // Spawn the subscriber.
        let subscriber_handle = Subscriber::spawn(
            store.clone(),
            tx_reconfigure.subscribe(),
            rx_consensus,
            tx_batch_loader,
            tx_executor,
            restored_consensus_output,
        );

        // Spawn the executor's core.
        let executor_handle = Core::<State>::spawn(
            store.clone(),
            execution_state,
            tx_reconfigure.subscribe(),
            /* rx_subscriber */ rx_executor,
            tx_output,
        );

        // Spawn the batch loader.
        let mut worker_addresses: HashMap<u32, Multiaddr> = worker_cache
            .load()
            .workers
            .iter()
            .find_map(|v| match_opt::match_opt!(v, (nm, authority) if name == *nm => authority))
            .expect("Could not find own key in worker_cache")
            .0
            .iter()
            .map(|(id, x)| (*id, x.worker_to_worker.clone()))
            .collect();
        ////////////////////////////////////////////////////////////////
        // TODO: remove this hack once #706 is fixed
        ////////////////////////////////////////////////////////////////

        // retrieve our primary address
        let our_primary_to_primary_address = committee
            .load()
            .primary(&name)
            .expect("Out public key is not in the committee!")
            .primary_to_primary;
        // extract the hostname portion
        let our_primary_hostname = our_primary_to_primary_address
            .into_iter()
            .flat_map(move |x| match x {
                p @ Protocol::Ip4(_) | p @ Protocol::Ip6(_) | p @ Protocol::Dns(_) => Some(p),
                _ => None,
            })
            .next()
            .expect("Could not find hostname in our primary address!");
        // Modify the worker addresses that we are about to use : would we talk better using a loopback address?
        for worker_address in worker_addresses.values_mut() {
            replace_distant_by_localhost(worker_address, &our_primary_hostname);
        }
        ////////////////////////////////////////////////////////////////

        let batch_loader_handle = BatchLoader::spawn(
            store,
            tx_reconfigure.subscribe(),
            rx_batch_loader,
            worker_addresses,
        );

        // Return the handle.
        info!("Consensus subscriber successfully started");

        Ok(vec![
            subscriber_handle,
            executor_handle,
            batch_loader_handle,
        ])
    }
}

pub async fn get_restored_consensus_output<State>(
    consensus_store: Arc<ConsensusStore>,
    certificate_store: CertificateStore,
    execution_state: Arc<State>,
) -> Result<Vec<ConsensusOutput>, SubscriberError>
where
    State: ExecutionState + Send + Sync + 'static,
    State::Error: Debug,
{
    let mut restored_consensus_output = Vec::new();
    let consensus_next_index = consensus_store
        .read_last_consensus_index()
        .map_err(SubscriberError::StoreError)?;

    let next_cert_index = execution_state
        .load_execution_indices()
        .await?
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

fn replace_distant_by_localhost(target: &mut Multiaddr, hostname_pattern: &Protocol) {
    // does the hostname match our pattern exactly?
    if target.iter().next() == Some(hostname_pattern.clone()) {
        if let Some(replacement) = target.replace(0, move |x| match x {
            Protocol::Ip4(_) => Some(Protocol::Ip4(Ipv4Addr::LOCALHOST)),
            Protocol::Ip6(_) => Some(Protocol::Ip6(Ipv6Addr::LOCALHOST)),
            Protocol::Dns(_) => Some(Protocol::Dns(Cow::Owned("localhost".to_owned()))),
            _ => None,
        }) {
            tracing::debug!("Address for worker {} replaced by {}", target, replacement);
            *target = replacement;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use multiaddr::{multiaddr, Protocol};
    use std::net::Ipv4Addr;

    #[test]
    fn test_replace_distant_by_localhost() {
        // IPV4 positive
        let non_local: Ipv4Addr = "8.8.8.8".parse().unwrap();
        let mut addr1 = multiaddr!(Ip4(non_local), Tcp(10000u16));

        replace_distant_by_localhost(&mut addr1, &Protocol::Ip4(non_local));
        assert_eq!(addr1, multiaddr!(Ip4(Ipv4Addr::LOCALHOST), Tcp(10000u16)));

        // IPV4 negative
        let other_target: Ipv4Addr = "8.8.8.4".parse().unwrap();
        let addr1 = multiaddr!(Ip4(non_local), Tcp(10000u16));
        let mut addr2 = multiaddr!(Ip4(non_local), Tcp(10000u16));

        replace_distant_by_localhost(&mut addr2, &Protocol::Ip4(other_target));
        assert_eq!(addr2, addr1);

        // IPV6 positive
        let non_local: Ipv6Addr = "2607:f0d0:1002:51::4".parse().unwrap();
        let mut addr1 = multiaddr!(Ip6(non_local), Tcp(10000u16));

        replace_distant_by_localhost(&mut addr1, &Protocol::Ip6(non_local));
        assert_eq!(addr1, multiaddr!(Ip6(Ipv6Addr::LOCALHOST), Tcp(10000u16)));

        // IPV6 negative
        let other_target: Ipv6Addr = "2607:f0d0:1002:50::4".parse().unwrap();
        let addr1 = multiaddr!(Ip6(non_local), Tcp(10000u16));
        let mut addr2 = multiaddr!(Ip6(non_local), Tcp(10000u16));

        replace_distant_by_localhost(&mut addr2, &Protocol::Ip6(other_target));
        assert_eq!(addr2, addr1);

        // DNS positive
        let non_local: Cow<str> = Cow::Owned("google.com".to_owned());
        let mut addr1 = multiaddr!(Dns(non_local.clone()), Tcp(10000u16));

        replace_distant_by_localhost(&mut addr1, &Protocol::Dns(non_local.clone()));
        let localhost: Cow<str> = Cow::Owned("localhost".to_owned());
        assert_eq!(addr1, multiaddr!(Dns(localhost), Tcp(10000u16)));

        // DNS negative
        let other_target: Cow<str> = Cow::Owned("apple.com".to_owned());
        let addr1 = multiaddr!(Dns(non_local.clone()), Tcp(10000u16));
        let mut addr2 = multiaddr!(Dns(non_local), Tcp(10000u16));

        replace_distant_by_localhost(&mut addr2, &Protocol::Dns(other_target));
        assert_eq!(addr2, addr1);
    }
}
