// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::primary::PrimaryWorkerMessage;
use bytes::Bytes;
use config::Committee;
use crypto::traits::VerifyingKey;
use network::SimpleSender;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc::Receiver;
use types::Certificate;

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector<PublicKey: VerifyingKey> {
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    rx_consensus: Receiver<Certificate<PublicKey>>,
    /// Re-sent our own GC'ed digests to the proposed.
    tx_our_digests: Sender<(BatchDigest, WorkerId)>,
    /// The network addresses of our workers.
    addresses: Vec<SocketAddr>,
    /// A network sender to notify our workers of cleanup events.
    network: SimpleSender,
}

impl<PublicKey: VerifyingKey> GarbageCollector<PublicKey> {
    pub fn spawn(
        name: &PublicKey,
        committee: &Committee<PublicKey>,
        consensus_round: Arc<AtomicU64>,
        rx_consensus: Receiver<Certificate<PublicKey>>,
        tx_our_digests: Sender<(BatchDigest, WorkerId)>,
    ) {
        let addresses = committee
            .our_workers(name)
            .expect("Our public key or worker id is not in the committee")
            .iter()
            .map(|x| x.primary_to_worker)
            .collect();

        tokio::spawn(async move {
            Self {
                consensus_round,
                rx_consensus,
                tx_our_digests,
                addresses,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;
        while let Some(certificate) = self.rx_consensus.recv().await {
            let round = certificate.round();

            // Re-include batch digests that have not been sequenced into our next block.
            if certificate.author() == name {
                for payload in certificate.header.payload {
                    self.tx_our_digests
                        .send(payload)
                        .await
                        .expect("Failed to re-propose GC'ed digests");
                }
            }

            // Run the GC.
            if round > last_committed_round {
                last_committed_round = round;

                // Trigger cleanup on the primary.
                self.consensus_round.store(round, Ordering::Relaxed);

                // Trigger cleanup on the workers..
                let bytes = bincode::serialize(&PrimaryWorkerMessage::<PublicKey>::Cleanup(round))
                    .expect("Failed to serialize our own message");
                self.network
                    .broadcast(self.addresses.clone(), Bytes::from(bytes))
                    .await;
            }
        }
    }
}
