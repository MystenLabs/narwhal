// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use config::{Parameters, SharedCommittee, WorkerId};
use consensus::{
    bullshark::Bullshark,
    dag::Dag,
    metrics::{ChannelMetrics, ConsensusMetrics},
    Consensus,
};
use crypto::{
    traits::{KeyPair as _, VerifyingKey},
    KeyPair, PublicKey,
};
use executor::{ExecutionState, Executor, ExecutorOutput, SerializedTransaction, SubscriberResult};
use primary::{NetworkModel, PayloadToken, Primary, PrimaryChannelMetrics};
use prometheus::{IntGauge, Registry};
use std::{fmt::Debug, sync::Arc};
use store::{
    reopen,
    rocks::{open_cf, DBMap},
    Store,
};
use task_group::{TaskGroup, TaskManager};
use tokio::{
    sync::{mpsc::Sender, watch},
    task::{JoinError, JoinHandle},
};
use tracing::debug;
use types::{
    metered_channel, BatchDigest, Certificate, CertificateDigest, ConsensusStore, Header,
    HeaderDigest, ReconfigureNotification, Round, SequenceNumber, SerializedBatchMessage,
};
use worker::{metrics::initialise_metrics, Worker};

pub mod execution_state;
pub mod metrics;
pub mod restarter;

/// All the data stores of the node.
pub struct NodeStorage {
    pub header_store: Store<HeaderDigest, Header>,
    pub certificate_store: Store<CertificateDigest, Certificate>,
    pub payload_store: Store<(BatchDigest, WorkerId), PayloadToken>,
    pub batch_store: Store<BatchDigest, SerializedBatchMessage>,
    pub consensus_store: Arc<ConsensusStore>,
}

impl NodeStorage {
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
            Self::HEADERS_CF;<HeaderDigest, Header>,
            Self::CERTIFICATES_CF;<CertificateDigest, Certificate>,
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
    pub async fn spawn_primary<State>(
        // The private-public key pair of this authority.
        keypair: KeyPair,
        // The committee information.
        committee: SharedCommittee,
        // The node's storage.
        store: &NodeStorage,
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
    ) -> SubscriberResult<TaskManager<JoinError>>
    where
        State: ExecutionState + Send + Sync + 'static,
        State::Outcome: Send + 'static,
        State::Error: Debug,
    {
        let initial_committee = ReconfigureNotification::NewEpoch((**committee.load()).clone());
        let (tx_reconfigure, _rx_reconfigure) = watch::channel(initial_committee);

        // These gauge is porcelain: do not modify it without also modifying `primary::metrics::PrimaryChannelMetrics::replace_registered_new_certificates_metric`
        // This hack avoids a cyclic dependency in the initialization of consensus and primary
        let new_certificates_counter = IntGauge::new(
            PrimaryChannelMetrics::NAME_NEW_CERTS,
            PrimaryChannelMetrics::DESC_NEW_CERTS,
        )
        .unwrap();
        let (tx_new_certificates, rx_new_certificates) =
            metered_channel::channel(Self::CHANNEL_CAPACITY, &new_certificates_counter);

        let committed_certificates_counter = IntGauge::new(
            PrimaryChannelMetrics::NAME_COMMITTED_CERTS,
            PrimaryChannelMetrics::DESC_COMMITTED_CERTS,
        )
        .unwrap();
        let (tx_consensus, rx_consensus) =
            metered_channel::channel(Self::CHANNEL_CAPACITY, &committed_certificates_counter);

        // Compute the public key of this authority.
        let name = keypair.public().clone();
        let mut handles = Vec::new();

        let (dag, network_model) = if !internal_consensus {
            debug!("Consensus is disabled: the primary will run w/o Tusk");
            let consensus_metrics = Arc::new(ConsensusMetrics::new(registry));
            let (handle, dag) = Dag::new(&committee.load(), rx_new_certificates, consensus_metrics);

            handles.push(("dag", handle));

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

        // Inject memory profiling here if we build with dhat-heap feature flag
        // Put name of primary in heap profile to distinguish diff primaries
        #[cfg(feature = "dhat-heap")]
        let profiler = {
            use crypto::traits::EncodeDecodeBase64;
            use std::path::Path;

            let heap_file = format!("dhat-heap-{}.json", name.encode_base64());
            Arc::new(
                dhat::Profiler::builder()
                    .file_name(Path::new(&heap_file))
                    .build(),
            )
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
            tx_new_certificates,
            /* rx_consensus */ rx_consensus,
            /* dag */ dag,
            network_model,
            tx_reconfigure,
            tx_consensus,
            registry,
        );
        handles.extend(primary_handles);

        // Let's spin off a separate thread that waits a while then dumps the profile,
        // otherwise this function exits immediately and the profile is dumped way too soon.
        // See https://github.com/nnethercote/dhat-rs/issues/19 for a panic that happens,
        // but at least 2 primaries should complete and dump their profiles.
        #[cfg(feature = "dhat-heap")]
        {
            use std::time::Duration;

            #[allow(clippy::redundant_clone)]
            let profiler2 = profiler.clone();
            std::thread::spawn(|| {
                std::thread::sleep(Duration::from_secs(240));
                println!("Dropping DHAT profiler...");
                drop(profiler2);
            });
        }

        let (task_group, task_manager) = TaskGroup::new();
        for (name, handle) in handles {
            // The tasks will be awaited with the `task_manager`, so the task handles / futures can be dropped.
            let _ = task_group.spawn(name, handle);
        }

        Ok(task_manager)
    }

    /// Spawn the consensus core and the client executing transactions.
    async fn spawn_consensus<State>(
        name: PublicKey,
        committee: SharedCommittee,
        store: &NodeStorage,
        parameters: Parameters,
        execution_state: Arc<State>,
        tx_reconfigure: &watch::Sender<ReconfigureNotification>,
        rx_new_certificates: metered_channel::Receiver<Certificate>,
        tx_feedback: metered_channel::Sender<Certificate>,
        tx_confirmation: Sender<(
            SubscriberResult<<State as ExecutionState>::Outcome>,
            SerializedTransaction,
        )>,
        registry: &Registry,
    ) -> SubscriberResult<Vec<(&'static str, JoinHandle<()>)>>
    where
        PublicKey: VerifyingKey,
        State: ExecutionState + Send + Sync + 'static,
        State::Outcome: Send + 'static,
        State::Error: Debug,
    {
        let consensus_metrics = Arc::new(ConsensusMetrics::new(registry));
        let channel_metrics = ChannelMetrics::new(registry);

        let (tx_sequence, rx_sequence) =
            metered_channel::channel(Self::CHANNEL_CAPACITY, &channel_metrics.tx_sequence);

        let mut handles = Vec::new();

        // Spawn the consensus core who only sequences transactions.
        let ordering_engine = Bullshark::new(
            (**committee.load()).clone(),
            store.consensus_store.clone(),
            parameters.gc_depth,
        );
        let consensus_handle = Consensus::spawn(
            (**committee.load()).clone(),
            store.consensus_store.clone(),
            store.certificate_store.clone(),
            tx_reconfigure.subscribe(),
            /* rx_primary */ rx_new_certificates,
            /* tx_primary */ tx_feedback,
            /* tx_output */ tx_sequence,
            ordering_engine,
            consensus_metrics.clone(),
            parameters.gc_depth,
        );
        handles.push(("consensus", consensus_handle));

        // Spawn the client executing the transactions. It can also synchronize with the
        // subscriber handler if it missed some transactions.
        let executor_handles = Executor::spawn(
            name,
            committee,
            store.batch_store.clone(),
            execution_state,
            tx_reconfigure,
            /* rx_consensus */ rx_sequence,
            /* tx_output */ tx_confirmation,
            registry,
        )
        .await?;
        handles.extend(executor_handles);

        Ok(handles)
    }

    /// Spawn a specified number of workers.
    pub fn spawn_workers(
        // The public key of this authority.
        name: PublicKey,
        // The ids of the validators to spawn.
        ids: Vec<WorkerId>,
        // The committee information.
        committee: SharedCommittee,
        // The node's storage,
        store: &NodeStorage,
        // The configuration parameters.
        parameters: Parameters,
        // The prometheus metrics Registry
        registry: &Registry,
    ) -> TaskManager<JoinError> {
        let metrics = initialise_metrics(registry);

        let (task_group, task_manager) = TaskGroup::new();
        for id in ids {
            let worker_handles = Worker::spawn(
                name.clone(),
                id,
                committee.clone(),
                parameters.clone(),
                store.batch_store.clone(),
                metrics.clone(),
            );
            // TODO(https://github.com/MystenLabs/narwhal/issues/727): propagate worker task names.
            for (i, h) in worker_handles.into_iter().enumerate() {
                let _ = task_group.spawn(format!("worker_{}_{}", id, i), h);
            }
        }

        task_manager
    }
}
