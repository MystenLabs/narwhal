// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{metrics::ConsensusMetrics, ConsensusOutput, SequenceNumber};
use config::SharedCommittee;
use crypto::{traits::VerifyingKey, Hash};
use std::{cmp::max, collections::HashMap, sync::Arc};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use types::{
    Certificate, CertificateDigest, ConsensusPrimaryMessage, ConsensusStore, Round, StoreResult,
};

/// The representation of the DAG in memory.
pub type Dag<PublicKey> =
    HashMap<Round, HashMap<PublicKey, (CertificateDigest, Certificate<PublicKey>)>>;

/// The state that needs to be persisted for crash-recovery.
pub struct ConsensusState<PublicKey: VerifyingKey> {
    /// The last committed round.
    pub last_committed_round: Round,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    pub last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    pub dag: Dag<PublicKey>,
    /// Metrics handler
    pub metrics: Arc<ConsensusMetrics>,
}

impl<PublicKey: VerifyingKey> ConsensusState<PublicKey> {
    pub fn new(genesis: Vec<Certificate<PublicKey>>, metrics: Arc<ConsensusMetrics>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_committed: genesis
                .iter()
                .map(|(x, (_, y))| (x.clone(), y.round()))
                .collect(),
            dag: [(0, genesis)]
                .iter()
                .cloned()
                .collect::<HashMap<_, HashMap<_, _>>>(),
            metrics,
        }
    }

    /// Update and clean up internal state base on committed certificates.
    pub fn update(&mut self, certificate: &Certificate<PublicKey>, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *std::iter::Iterator::max(self.last_committed.values()).unwrap();
        self.last_committed_round = last_committed_round;

        self.metrics
            .last_committed_round
            .with_label_values(&[])
            .set(last_committed_round as i64);

        // We purge all certificates past the gc depth
        self.dag.retain(|r, _| r + gc_depth >= last_committed_round);
        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                // We purge certificates for `name` prior to its latest commit
                if r < round {
                    authorities.retain(|n, _| n != name);
                }
                !authorities.is_empty()
            });
        }

        self.metrics
            .consensus_dag_size
            .with_label_values(&[])
            .set(self.dag.len() as i64);
    }
}

/// Describe how to sequence input certificates.
pub trait ConsensusProtocol<PublicKey: VerifyingKey> {
    fn process_certificate(
        &mut self,
        // The state of the consensus protocol.
        state: &mut ConsensusState<PublicKey>,
        // The latest consensus index.
        consensus_index: SequenceNumber,
        // The new certificate.
        certificate: Certificate<PublicKey>,
    ) -> StoreResult<Vec<ConsensusOutput<PublicKey>>>;
}

pub struct Consensus<PublicKey: VerifyingKey, ConsensusProtocol> {
    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_primary: Receiver<Certificate<PublicKey>>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_primary: Sender<ConsensusPrimaryMessage<PublicKey>>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<ConsensusOutput<PublicKey>>,

    /// The genesis certificates.
    genesis: Vec<Certificate<PublicKey>>,
    /// The (global) consensus index. We assign one index to each sequenced certificate. this is
    /// helpful for clients.
    consensus_index: SequenceNumber,

    /// The consensus protocol to run.
    protocol: ConsensusProtocol,

    /// Metrics handler
    metrics: Arc<ConsensusMetrics>,
}

impl<PublicKey, Protocol> Consensus<PublicKey, Protocol>
where
    PublicKey: VerifyingKey,
    Protocol: ConsensusProtocol<PublicKey> + Send + 'static,
{
    pub fn spawn(
        committee: SharedCommittee<PublicKey>,
        store: Arc<ConsensusStore<PublicKey>>,
        rx_primary: Receiver<Certificate<PublicKey>>,
        tx_primary: Sender<ConsensusPrimaryMessage<PublicKey>>,
        tx_output: Sender<ConsensusOutput<PublicKey>>,
        protocol: Protocol,
        metrics: Arc<ConsensusMetrics>,
    ) -> JoinHandle<StoreResult<()>> {
        tokio::spawn(async move {
            let consensus_index = store.read_last_consensus_index()?;
            let genesis = Certificate::genesis(&committee);
            Self {
                rx_primary,
                tx_primary,
                tx_output,
                genesis,
                consensus_index,
                protocol,
                metrics,
            }
            .run()
            .await
        })
    }

    async fn run(&mut self) -> StoreResult<()> {
        // The consensus state (everything else is immutable).
        let mut state = ConsensusState::new(self.genesis.clone(), self.metrics.clone());

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_primary.recv().await {
            let sequence =
                self.protocol
                    .process_certificate(&mut state, self.consensus_index, certificate)?;

            // Update the consensus index.
            self.consensus_index += sequence.len() as u64;

            // Output the sequence in the right order.
            for output in sequence {
                let certificate = &output.certificate;
                #[cfg(not(feature = "benchmark"))]
                if output.consensus_index % 5_000 == 0 {
                    tracing::debug!("Committed {}", certificate.header);
                }

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    // NOTE: This log entry is used to compute performance.
                    tracing::info!("Committed {} -> {:?}", certificate.header, digest);
                }

                let message = ConsensusPrimaryMessage::Sequenced(certificate.clone());
                self.tx_primary
                    .send(message)
                    .await
                    .expect("Failed to send certificate to primary");

                if let Err(e) = self.tx_output.send(output).await {
                    tracing::warn!("Failed to output certificate: {e}");
                }
            }
        }
        Ok(())
    }
}
