// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use config::{Epoch, SharedCommittee, WorkerId};
use crypto::{traits::VerifyingKey, Digest, Hash as _, SignatureService};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        watch,
    },
    time::{sleep, Duration, Instant},
};
use tracing::debug;
#[cfg(feature = "benchmark")]
use tracing::info;
use types::{BatchDigest, Certificate, CertificateDigest, Header, Round};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer<PublicKey: VerifyingKey> {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: SharedCommittee<PublicKey>,
    /// Service to sign headers.
    signature_service: SignatureService<PublicKey::Sig>,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: Duration,

    /// Watch channel to reconfigure the committee.
    rx_committee: watch::Receiver<SharedCommittee<PublicKey>>,
    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<(Vec<CertificateDigest>, Round, Epoch)>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(BatchDigest, WorkerId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header<PublicKey>>,

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<CertificateDigest>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(BatchDigest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
}

impl<PublicKey: VerifyingKey> Proposer<PublicKey> {
    pub fn spawn(
        name: PublicKey,
        committee: SharedCommittee<PublicKey>,
        signature_service: SignatureService<PublicKey::Sig>,
        header_size: usize,
        max_header_delay: Duration,
        rx_committee: watch::Receiver<SharedCommittee<PublicKey>>,
        rx_core: Receiver<(Vec<CertificateDigest>, Round, Epoch)>,
        rx_workers: Receiver<(BatchDigest, WorkerId)>,
        tx_core: Sender<Header<PublicKey>>,
    ) {
        let genesis = Certificate::genesis(&committee)
            .iter()
            .map(|x| x.digest())
            .collect();

        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                committee,
                header_size,
                max_header_delay,
                rx_committee,
                rx_core,
                rx_workers,
                tx_core,
                round: 1,
                last_parents: genesis,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        // Make a new header.
        let header = Header::new(
            self.name.clone(),
            self.round,
            self.committee.epoch(),
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    /// Update the committee and cleanup internal state.
    fn update_committee(&mut self, committee: SharedCommittee<PublicKey>) {
        self.committee = committee;
        self.round = 0;
        self.last_parents = Certificate::genesis(&self.committee)
            .iter()
            .map(|x| x.digest())
            .collect();
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);

        let timer = sleep(self.max_header_delay);
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();
            if (timer_expired || enough_digests) && enough_parents {
                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;

                // Reschedule the timer.
                let deadline = Instant::now() + self.max_header_delay;
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                // receive potential parents from the core.
                Some((parents, round, epoch)) = self.rx_core.recv() => {
                    // If the core already moved to the next epoch we should pull the next
                    // committee as well.
                    if epoch > self.committee.epoch() {
                        let new_committee = self.rx_committee.borrow_and_update().clone();
                        self.update_committee(new_committee);
                    }

                    // Ignore parents from lower rounds or epochs.
                    if round < self.round || epoch < self.committee.epoch() {
                        continue;
                    }

                    // Advance to the next round.
                    self.round = round + 1;
                    debug!("Dag moved to round {}", self.round);

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;
                }

                // Receive digests from our workers.
                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    self.payload_size += Digest::from(digest).size();
                    self.digests.push((digest, worker_id));
                }

                // Check whether the timer expired.
                () = &mut timer => {
                    // Nothing to do.
                }

                // Check whether the committee changed.
                result = self.rx_committee.changed() => {
                    result.expect("Committee channel dropped");
                    let new_committee = self.rx_committee.borrow().clone();
                    self.update_committee(new_committee);
                }
            }
        }
    }
}
