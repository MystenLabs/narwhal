// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use arc_swap::ArcSwap;
use config::{Committee, Parameters, SharedCommittee, WorkerId};
use consensus::{
    bullshark::Bullshark, dag::Dag, metrics::ConsensusMetrics, Consensus, SubscriberHandler,
};
use crypto::traits::{KeyPair, Signer, VerifyingKey};
use executor::{ExecutionState, Executor, ExecutorOutput, SerializedTransaction, SubscriberResult};
use futures::future::join_all;
use network::PrimaryToWorkerNetwork;
use primary::{NetworkModel, PayloadToken, Primary, WorkerPrimaryMessage};
use prometheus::Registry;
use std::{fmt::Debug, path::PathBuf, sync::Arc};
use store::{
    reopen,
    rocks::{open_cf, DBMap},
    Store,
};
use tokio::{
    sync::{
        mpsc::{channel, Receiver, Sender},
        watch,
    },
    task::JoinHandle,
};
use tracing::debug;
use types::{
    BatchDigest, Certificate, CertificateDigest, ConsensusStore, Header, HeaderDigest,
    PrimaryWorkerMessage, ReconfigureNotification, Round, SequenceNumber, SerializedBatchMessage,
};
use worker::{metrics::initialise_metrics, Worker, WorkerToPrimaryNetwork};

pub mod metrics;

/// All the data stores of the node.
pub struct NodeStorage<PublicKey: VerifyingKey> {
    pub header_store: Store<HeaderDigest, Header<PublicKey>>,
    pub certificate_store: Store<CertificateDigest, Certificate<PublicKey>>,
    pub payload_store: Store<(BatchDigest, WorkerId), PayloadToken>,
    pub batch_store: Store<BatchDigest, SerializedBatchMessage>,
    pub consensus_store: Arc<ConsensusStore<PublicKey>>,
}

impl<PublicKey: VerifyingKey> NodeStorage<PublicKey> {
    /// The datastore column family names.
    const HEADERS_CF: &'static str = "headers";
    const CERTIFICATES_CF: &'static str = "certificates";
    const PAYLOAD_CF: &'static str = "payload";
    const BATCHES_CF: &'static str = "batches";
    const LAST_COMMITTED_CF: &'static str = "last_committed";
    const SEQUENCE_CF: &'static str = "sequence";

    /// Open or reopen all the storage of the node.
    pub fn reopen<Path: AsRef<std::path::Path>>(store_path: Path) -> Self {
        let rocksdb = open_cf(
            store_path,
            None,
            &[
                Self::HEADERS_CF,
                Self::CERTIFICATES_CF,
                Self::PAYLOAD_CF,
                Self::BATCHES_CF,
                Self::LAST_COMMITTED_CF,
                Self::SEQUENCE_CF,
            ],
        )
        .expect("Cannot open database");

        let (header_map, certificate_map, payload_map, batch_map, last_committed_map, sequence_map) = reopen!(&rocksdb,
            Self::HEADERS_CF;<HeaderDigest, Header<PublicKey>>,
            Self::CERTIFICATES_CF;<CertificateDigest, Certificate<PublicKey>>,
            Self::PAYLOAD_CF;<(BatchDigest, WorkerId), PayloadToken>,
            Self::BATCHES_CF;<BatchDigest, SerializedBatchMessage>,
            Self::LAST_COMMITTED_CF;<PublicKey, Round>,
            Self::SEQUENCE_CF;<SequenceNumber, CertificateDigest>
        );

        let header_store = Store::new(header_map);
        let certificate_store = Store::new(certificate_map);
        let payload_store = Store::new(payload_map);
        let batch_store = Store::new(batch_map);
        let consensus_store = Arc::new(ConsensusStore::new(last_committed_map, sequence_map));

        Self {
            header_store,
            certificate_store,
            payload_store,
            batch_store,
            consensus_store,
        }
    }
}

/// High level functions to spawn the primary and the workers.
pub struct Node;

impl Node {
    /// The default channel capacity.
    pub const CHANNEL_CAPACITY: usize = 1_000;

    /// Spawn a new primary. Optionally also spawn the consensus and a client executing transactions.
    pub async fn spawn_primary<Keys, PublicKey, State>(
        // The private-public key pair of this authority.
        keypair: Keys,
        // The committee information.
        committee: SharedCommittee<PublicKey>,
        // The node's storage.
        store: &NodeStorage<PublicKey>,
        // The configuration parameters.
        parameters: Parameters,
        // Whether to run consensus (and an executor client) or not.
        // If true, an internal consensus will be used, else an external consensus will be used.
        // If an external consensus will be used, then this bool will also ensure that the
        // corresponding gRPC server that is used for communication between narwhal and
        // external consensus is also spawned.
        internal_consensus: bool,
        // The state used by the client to execute transactions.
        execution_state: Arc<State>,
        // A channel to output transactions execution confirmations.
        tx_confirmation: Sender<ExecutorOutput<State>>,
        // A prometheus exporter Registry to use for the metrics
        registry: &Registry,
    ) -> SubscriberResult<Vec<JoinHandle<()>>>
    where
        PublicKey: VerifyingKey,
        Keys: KeyPair<PubKey = PublicKey> + Signer<PublicKey::Sig> + Send + 'static,
        State: ExecutionState + Send + Sync + 'static,
        State::Outcome: Send + 'static,
        State::Error: Debug,
    {
        let initial_committee = ReconfigureNotification::NewCommittee((**committee.load()).clone());
        let (tx_reconfigure, _rx_reconfigure) = watch::channel(initial_committee);

        let (tx_new_certificates, rx_new_certificates) = channel(Self::CHANNEL_CAPACITY);
        let (tx_consensus, rx_consensus) = channel(Self::CHANNEL_CAPACITY);

        // Compute the public key of this authority.
        let name = keypair.public().clone();
        let mut handles = Vec::new();

        let (dag, network_model) = if !internal_consensus {
            debug!("Consensus is disabled: the primary will run w/o Tusk");
            let consensus_metrics = Arc::new(ConsensusMetrics::new(registry));
            let (_handle, dag) =
                Dag::new(&*committee.load(), rx_new_certificates, consensus_metrics);
            (Some(Arc::new(dag)), NetworkModel::Asynchronous)
        } else {
            let consensus_handles = Self::spawn_consensus(
                name.clone(),
                committee.clone(),
                store,
                parameters.clone(),
                execution_state,
                &tx_reconfigure,
                rx_new_certificates,
                tx_consensus.clone(),
                tx_confirmation,
                registry,
            )
            .await?;
            handles.extend(consensus_handles);
            (None, NetworkModel::PartiallySynchronous)
        };

        // Spawn the primary.
        let primary_handles = Primary::spawn(
            name.clone(),
            keypair,
            committee.clone(),
            parameters.clone(),
            store.header_store.clone(),
            store.certificate_store.clone(),
            store.payload_store.clone(),
            /* tx_consensus */ tx_new_certificates,
            /* rx_consensus */ rx_consensus,
            /* dag */ dag,
            network_model,
            tx_reconfigure,
            tx_consensus,
            registry,
        );
        handles.extend(primary_handles);

        Ok(handles)
    }

    /// Spawn the consensus core and the client executing transactions.
    async fn spawn_consensus<PublicKey, State>(
        name: PublicKey,
        committee: SharedCommittee<PublicKey>,
        store: &NodeStorage<PublicKey>,
        parameters: Parameters,
        execution_state: Arc<State>,
        tx_reconfigure: &watch::Sender<ReconfigureNotification<PublicKey>>,
        rx_new_certificates: Receiver<Certificate<PublicKey>>,
        tx_feedback: Sender<Certificate<PublicKey>>,
        tx_confirmation: Sender<(
            SubscriberResult<<State as ExecutionState>::Outcome>,
            SerializedTransaction,
        )>,
        registry: &Registry,
    ) -> SubscriberResult<Vec<JoinHandle<()>>>
    where
        PublicKey: VerifyingKey,
        State: ExecutionState + Send + Sync + 'static,
        State::Outcome: Send + 'static,
        State::Error: Debug,
    {
        let (tx_sequence, rx_sequence) = channel(Self::CHANNEL_CAPACITY);
        let (tx_consensus_to_client, rx_consensus_to_client) = channel(Self::CHANNEL_CAPACITY);
        let (tx_client_to_consensus, rx_client_to_consensus) = channel(Self::CHANNEL_CAPACITY);
        let consensus_metrics = Arc::new(ConsensusMetrics::new(registry));

        // Spawn the consensus core who only sequences transactions.
        let ordering_engine = Bullshark::new(
            (**committee.load()).clone(),
            store.consensus_store.clone(),
            parameters.gc_depth,
        );
        let consensus_handles = Consensus::spawn(
            (**committee.load()).clone(),
            store.consensus_store.clone(),
            tx_reconfigure.subscribe(),
            /* rx_primary */ rx_new_certificates,
            /* tx_primary */ tx_feedback,
            /* tx_output */ tx_sequence,
            ordering_engine,
            consensus_metrics.clone(),
        );

        // The subscriber handler receives the ordered sequence from consensus and feed them
        // to the executor. The executor has its own state and data store who may crash
        // independently of the narwhal node.
        let subscriber_handles = SubscriberHandler::spawn(
            store.consensus_store.clone(),
            store.certificate_store.clone(),
            tx_reconfigure.subscribe(),
            rx_sequence,
            /* rx_client */ rx_client_to_consensus,
            /* tx_client */ tx_consensus_to_client,
        );

        // Spawn the client executing the transactions. It can also synchronize with the
        // subscriber handler if it missed some transactions.
        let executor_handles = Executor::spawn(
            name,
            committee,
            store.batch_store.clone(),
            execution_state,
            tx_reconfigure,
            /* rx_consensus */ rx_consensus_to_client,
            /* tx_consensus */ tx_client_to_consensus,
            /* tx_output */ tx_confirmation,
        )
        .await?;

        Ok(executor_handles
            .into_iter()
            .chain(std::iter::once(consensus_handles))
            .chain(std::iter::once(subscriber_handles))
            .collect())
    }

    /// Spawn a specified number of workers.
    pub fn spawn_workers<PublicKey: VerifyingKey>(
        // The public key of this authority.
        name: PublicKey,
        // The ids of the validators to spawn.
        ids: Vec<WorkerId>,
        // The committee information.
        committee: SharedCommittee<PublicKey>,
        // The node's storage,
        store: &NodeStorage<PublicKey>,
        // The configuration parameters.
        parameters: Parameters,
        // The prometheus metrics Registry
        registry: &Registry,
    ) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();

        let metrics = initialise_metrics(registry);

        for id in ids {
            let worker_handles = Worker::spawn(
                name.clone(),
                id,
                committee.clone(),
                parameters.clone(),
                store.batch_store.clone(),
                metrics.clone(),
            );
            handles.extend(worker_handles);
        }
        handles
    }
}

/// Module to start a node (primary, workers and default consensus), keep it running, and restarting it
/// every time the committee changes.
pub struct NodeController;

impl NodeController {
    pub async fn watch<Keys, PublicKey, State>(
        keypair: Keys,
        committee: &Committee<PublicKey>,
        storage_base_path: PathBuf,
        execution_state: Arc<State>,
        parameters: Parameters,
        mut rx_reconfigure: Receiver<(Keys, Committee<PublicKey>)>,
        tx_output: Sender<ExecutorOutput<State>>,
    ) where
        PublicKey: VerifyingKey,
        State: ExecutionState + Send + Sync + 'static,
        State::Outcome: Send + 'static,
        State::Error: Debug,
        Keys: KeyPair<PubKey = PublicKey> + Signer<PublicKey::Sig> + Send + 'static,
    {
        let mut keypair = keypair;
        let mut name = keypair.public().clone();
        let mut committee = committee.clone();

        let mut handles = Vec::new();
        let mut primary_network = WorkerToPrimaryNetwork::default();
        let mut worker_network = PrimaryToWorkerNetwork::default();

        // Listen for new committees.
        loop {
            // Get a fresh store for the new epoch.
            let mut store_path = storage_base_path.clone();
            store_path.push(format!("epoch{}", committee.epoch()));
            let store = NodeStorage::reopen(store_path);

            // Restart the relevant components.
            let primary_handles = Node::spawn_primary(
                keypair,
                Arc::new(ArcSwap::new(Arc::new(committee.clone()))),
                &store,
                parameters.clone(),
                /* consensus */ true,
                execution_state.clone(),
                tx_output.clone(),
                &Registry::new(),
            )
            .await
            .unwrap();

            let worker_handles = Node::spawn_workers(
                name.clone(),
                /* worker_ids */ vec![0],
                Arc::new(ArcSwap::new(Arc::new(committee.clone()))),
                &store,
                parameters.clone(),
                &Registry::new(),
            );

            handles.extend(primary_handles);
            handles.extend(worker_handles);

            // Wait for a committee change.
            let (new_keypair, new_committee) = match rx_reconfigure.recv().await {
                Some(x) => x,
                None => break,
            };

            // Shutdown all relevant components.
            let address = committee
                .primary(&name)
                .expect("Our key is not in the committee")
                .worker_to_primary;
            let message =
                WorkerPrimaryMessage::<PublicKey>::Reconfigure(ReconfigureNotification::Shutdown);
            let primary_cancel_handle = primary_network.send(address, &message).await;

            let addresses = committee
                .our_workers(&name)
                .expect("Our key is not in the committee")
                .into_iter()
                .map(|x| x.primary_to_worker)
                .collect();
            let message =
                PrimaryWorkerMessage::<PublicKey>::Reconfigure(ReconfigureNotification::Shutdown);
            let worker_cancel_handles = worker_network.broadcast(addresses, &message).await;

            // Ensure the message has been received.
            primary_cancel_handle.await;
            join_all(worker_cancel_handles).await;

            // Wait for the components to shut down.
            join_all(handles.drain(..)).await;

            // Update the settings for the next epoch.
            keypair = new_keypair;
            name = keypair.public().clone();
            committee = new_committee;
        }
    }
}
