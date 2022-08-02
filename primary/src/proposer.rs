// Copyright(C) Facebook, Inc. and its affiliates.
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{metrics::PrimaryMetrics, NetworkModel};
use config::{Committee, Epoch, WorkerId};
use crypto::{Digest, Hash as _, PublicKey, Signature, SignatureService};
use std::{
    cmp::{max, Ordering},
    sync::Arc,
};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        watch,
    },
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};
use tracing::debug;
use types::{
    error::{DagError, DagResult},
    BatchDigest, Certificate, Header, ReconfigureNotification, Round,
};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq))]
pub enum ProposerMessage {
    NewParents(Vec<Certificate>, Round, Epoch),
    MeaningfulRound(Round),
}

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// Service to sign headers.
    signature_service: SignatureService<Signature>,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum message delay after GST (only meaningful in partial-synchrony).
    delta: Duration,
    /// The network model in which the node operates.
    network_model: NetworkModel,

    /// Watch channel to reconfigure the committee.
    rx_reconfigure: watch::Receiver<ReconfigureNotification>,
    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<ProposerMessage>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(BatchDigest, WorkerId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Certificate>,
    /// Holds the certificate of the last leader (if any).
    last_leader: Option<Certificate>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(BatchDigest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
    /// Whether we are on the happy path: i.e., we are after GST and the leader is not Byzantine.
    /// Note that `common_case` is only used in partial synchrony (it is alway set to `true` when
    /// running the DAG in asynchrony).
    common_case: bool,
    highest_useful_round: Round,

    /// Metrics handler
    metrics: Arc<PrimaryMetrics>,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService<Signature>,
        header_size: usize,
        max_header_delay: Duration,
        network_model: NetworkModel,
        rx_reconfigure: watch::Receiver<ReconfigureNotification>,
        rx_core: Receiver<ProposerMessage>,
        rx_workers: Receiver<(BatchDigest, WorkerId)>,
        tx_core: Sender<Header>,
        metrics: Arc<PrimaryMetrics>,
    ) -> JoinHandle<()> {
        let genesis = Certificate::genesis(&committee);
        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                header_size,
                delta: max_header_delay,
                network_model,
                rx_reconfigure,
                rx_core,
                rx_workers,
                tx_core,
                round: 0,
                last_parents: genesis,
                last_leader: None,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                common_case: true,
                highest_useful_round: 1,
                metrics,
            }
            .run()
            .await;
        })
    }

    async fn make_header(&mut self) -> DagResult<()> {
        // Make a new header.
        let header = Header::new(
            self.name.clone(),
            self.round,
            self.committee.epoch(),
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).map(|x| x.digest()).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {header:?}");

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            tracing::info!("Created {} -> {:?}", header, digest);
        }

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .map_err(|_| DagError::ShuttingDown)
    }

    /// Update the committee and cleanup internal state.
    fn update_committee(&mut self, committee: Committee) {
        self.committee = committee;
        self.round = 0;
        self.last_parents = Certificate::genesis(&self.committee);
        tracing::debug!("Committee updated to {}", self.committee);
    }

    // Main loop listening to incoming messages.
    /// Update the last leader certificate. This is only relevant in partial synchrony.
    fn update_leader(&mut self) -> bool {
        let leader_name = self.committee.leader(self.round as usize);
        self.last_leader = self
            .last_parents
            .iter()
            .find(|x| x.origin() == leader_name)
            .cloned();

        if let Some(leader) = self.last_leader.as_ref() {
            debug!("Got leader {} for round {}", leader.origin(), self.round);
        }

        self.last_leader.is_some()
    }

    /// Check whether if we have (i) 2f+1 votes for the leader, (ii) f+1 nodes not voting for the leader,
    /// or (iii) there is no leader to vote for. This is only relevant in partial synchrony.
    fn enough_votes(&self) -> bool {
        let leader = match &self.last_leader {
            Some(x) => x.digest(),
            None => return true,
        };

        let mut votes_for_leader = 0;
        let mut no_votes = 0;
        for certificate in &self.last_parents {
            let stake = self.committee.stake(&certificate.origin());
            if certificate.header.parents.contains(&leader) {
                votes_for_leader += stake;
            } else {
                no_votes += stake;
            }
        }

        let mut enough_votes = votes_for_leader >= self.committee.quorum_threshold();
        if enough_votes {
            if let Some(leader) = self.last_leader.as_ref() {
                debug!(
                    "Got enough support for leader {} at round {}",
                    leader.origin(),
                    self.round
                );
            }
        }
        enough_votes |= no_votes >= self.committee.validity_threshold();
        enough_votes
    }

    /// Whether we can advance the DAG or need to wait for the leader/more votes. This is only relevant in
    /// partial synchrony. Note that if we timeout, we ignore this check and advance anyway.
    fn is_common_case(&mut self) -> bool {
        match self.network_model {
            // In asynchrony we advance immediately.
            NetworkModel::Asynchronous => true,

            // In partial synchrony, we need to wait for the leader or for enough votes.
            NetworkModel::PartiallySynchronous => match self.round % 2 {
                0 => self.update_leader(),
                _ => self.enough_votes(),
            },
        }
    }

    fn update_state(&mut self, parents: Vec<Certificate>, round: Round) {
        // Compare the parents' round number with our current round.
        match round.cmp(&self.round) {
            Ordering::Greater => {
                // We accept round bigger than our current round to jump ahead in case we were
                // late (or just joined the network).
                self.round = round;
                self.last_parents = parents;
            }
            Ordering::Less => {
                // Ignore parents from older rounds.
                return;
            }
            Ordering::Equal => {
                // The core gives us the parents the first time they are enough to form a quorum.
                // Then it keeps giving us all the extra parents.
                self.last_parents.extend(parents)
            }
        }

        // Check whether we are on the happy path (we can vote for the leader or the leader has enough
        // votes to enable a commit).
        self.common_case = self.is_common_case();
    }

    /// Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);

        let timer = sleep(self.delta);
        tokio::pin!(timer);

        loop {
            // Check if we have enough parents; if we have enough digests; if other primaries already
            // made a non-certificate for this round (and need our certificate to make progress); and
            // if the timer expired (only meaningful in partial-synchrony).
            let enough_parents = !self.last_parents.is_empty();
            let we_have_enough_digests = self.payload_size >= self.header_size;
            let others_have_digests = self.round == self.highest_useful_round;
            let timer_expired = timer.is_elapsed();

            // Check whether advancing round allows the system to make meaningful progress. The system
            // makes meaningful progress if it advances with non-empty certificates.
            let meaningful_progress = we_have_enough_digests || others_have_digests;

            // Print a few debug logs.
            debug!("Do we have enough parents: {enough_parents}");
            debug!("Do we have enough digests: {we_have_enough_digests}");
            debug!("Have we learned about a non-empty certificate for this round: {others_have_digests}");
            debug!("Are we in the common case: {}", self.common_case);
            debug!("Did the timer expire: {timer_expired}");

            // Check whether we can move to the next round and proposer a new header.
            if (self.common_case || timer_expired) && enough_parents && meaningful_progress {
                if timer_expired && matches!(self.network_model, NetworkModel::PartiallySynchronous)
                {
                    debug!("Timer expired for round {}", self.round);
                }

                // Advance to the next round.
                self.round += 1;
                self.metrics
                    .current_round
                    .with_label_values(&[&self.committee.epoch.to_string()])
                    .set(self.round as i64);
                debug!("Dag moved to round {}", self.round);

                // Make a new header.
                match self.make_header().await {
                    Err(e @ DagError::ShuttingDown) => debug!("{e}"),
                    Err(e) => panic!("Unexpected error: {e}"),
                    Ok(()) => (),
                }
                self.payload_size = 0;

                // Reschedule the timer.
                let deadline = Instant::now() + self.delta;
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                Some(message) = self.rx_core.recv() => match message {
                    ProposerMessage::NewParents(parents, round, epoch) => {
                        // If the core already moved to the next epoch we should pull the next
                        // committee as well.
                        match epoch.cmp(&self.committee.epoch()) {
                            Ordering::Greater => {
                                let message = self.rx_reconfigure.borrow_and_update().clone();
                                match message {
                                    ReconfigureNotification::NewCommittee(new_committee) => {
                                        self.update_committee(new_committee);
                                    }
                                    ReconfigureNotification::Shutdown => return,
                                }
                            }
                            Ordering::Less => {
                                // We already updated the committee but the core is slow. Ignore
                                // the parents from older epochs.
                                continue;
                            }
                            Ordering::Equal => {
                                // Nothing to do, we can proceed.
                            }
                        }

                        // Update the internal state with the received information.
                        self.update_state(parents, round);

                    },
                    ProposerMessage::MeaningfulRound(round) => {
                        self.highest_useful_round = max(self.highest_useful_round, round);
                    },
                },

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
                result = self.rx_reconfigure.changed() => {
                    result.expect("Committee channel dropped");
                    let message = self.rx_reconfigure.borrow().clone();
                    match message {
                        ReconfigureNotification::NewCommittee(new_committee) => {
                            self.update_committee(new_committee);
                        },
                        ReconfigureNotification::Shutdown => return,
                    }
                }
            }
        }
    }
}
