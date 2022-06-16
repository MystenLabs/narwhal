// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::primary::PrimaryWorkerMessage;
use config::{Committee, SharedCommittee};
use crypto::traits::VerifyingKey;
use multiaddr::Multiaddr;
use network::PrimaryToWorkerNetwork;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::{mpsc::Receiver, watch};
use types::{Certificate, Round};

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector<PublicKey: VerifyingKey> {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: SharedCommittee<PublicKey>,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    rx_consensus: Receiver<Certificate<PublicKey>>,
    /// Channel to signal committee changes.
    tx_committee: watch::Sender<SharedCommittee<PublicKey>>,
    /// The latest round committed by consensus.
    last_committed_round: Round,
    /// A network sender to notify our workers of cleanup events.
    worker_network: PrimaryToWorkerNetwork,
}

impl<PublicKey: VerifyingKey> GarbageCollector<PublicKey> {
    pub fn spawn(
        name: PublicKey,
        committee: SharedCommittee<PublicKey>,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<Certificate<PublicKey>>,
        tx_committee: watch::Sender<SharedCommittee<PublicKey>>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                consensus_round,
                rx_consensus,
                tx_committee,
                last_committed_round: 0,
                worker_network: Default::default(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        while let Some(certificate) = self.rx_consensus.recv().await {
            // TODO [issue #9]: Re-include batch digests that have not been sequenced into our next block.

            let round = certificate.round();
            if round > self.last_committed_round {
                self.last_committed_round = round;

                // Trigger cleanup on the primary.
                self.consensus_round.store(round, Ordering::Relaxed);

                // Trigger cleanup on the workers..
                let addresses = self
                    .committee
                    .our_workers(&self.name)
                    .expect("Our public key or worker id is not in the committee")
                    .into_iter()
                    .map(|x| x.primary_to_worker)
                    .collect();
                let message = PrimaryWorkerMessage::<PublicKey>::Cleanup(round);
                self.worker_network.broadcast(addresses, &message).await;
            }
        }
    }
}
