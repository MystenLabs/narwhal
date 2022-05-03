// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    store::{ConsensusStore, StoreResult},
    ConsensusOutput, SequenceNumber,
};
use config::{Committee, Stake};
use crypto::{
    traits::{EncodeDecodeBase64, VerifyingKey},
    Hash,
};
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::{debug, warn};
use types::{Certificate, CertificateDigest, Round};

#[cfg(any(test, feature = "benchmark"))]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The representation of the DAG in memory.
type Dag<PublicKey> =
    HashMap<Round, HashMap<PublicKey, (CertificateDigest, Certificate<PublicKey>)>>;

/// The state that needs to be persisted for crash-recovery.
pub struct State<PublicKey: VerifyingKey> {
    /// The last committed round.
    last_committed_round: Round,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag<PublicKey>,
}

impl<PublicKey: VerifyingKey> State<PublicKey> {
    pub fn new(genesis: Vec<Certificate<PublicKey>>) -> Self {
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
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate<PublicKey>, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *std::iter::Iterator::max(self.last_committed.values()).unwrap();
        self.last_committed_round = last_committed_round;

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
    }
}

pub struct Consensus<PublicKey: VerifyingKey> {
    /// The committee information.
    committee: Committee<PublicKey>,
    /// Persistent storage to safe ensure crash-recovery.
    store: Arc<ConsensusStore<PublicKey>>,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_primary: Receiver<Certificate<PublicKey>>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_primary: Sender<Certificate<PublicKey>>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<ConsensusOutput<PublicKey>>,

    /// The genesis certificates.
    genesis: Vec<Certificate<PublicKey>>,
    /// The (global) consensus index. We assign one index to each sequenced certificate. this is
    /// helpful for clients.
    consensus_index: SequenceNumber,
}

impl<PublicKey: VerifyingKey> Consensus<PublicKey> {
    pub fn spawn(
        committee: Committee<PublicKey>,
        store: Arc<ConsensusStore<PublicKey>>,
        gc_depth: Round,
        rx_primary: Receiver<Certificate<PublicKey>>,
        tx_primary: Sender<Certificate<PublicKey>>,
        tx_output: Sender<ConsensusOutput<PublicKey>>,
    ) -> JoinHandle<StoreResult<()>> {
        tokio::spawn(async move {
            let consensus_index = store.read_last_consensus_index()?;
            Self {
                committee: committee.clone(),
                store,
                gc_depth,
                rx_primary,
                tx_primary,
                tx_output,
                genesis: Certificate::genesis(&committee),
                consensus_index,
            }
            .run()
            .await
        })
    }

    async fn run(&mut self) -> StoreResult<()> {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_primary.recv().await {
            let sequence = Consensus::process_certificate(
                &self.committee,
                &self.store,
                self.gc_depth,
                &mut state,
                self.consensus_index,
                certificate,
            )?;

            // Update the consensus index.
            self.consensus_index += sequence.len() as u64;

            // Output the sequence in the right order.
            for output in sequence {
                let certificate = &output.certificate;
                #[cfg(not(feature = "benchmark"))]
                if output.consensus_index % 5_000 == 0 {
                    debug!("Committed {}", certificate.header);
                }

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    // NOTE: This log entry is used to compute performance.
                    tracing::info!("Committed {} -> {:?}", certificate.header, digest);
                }

                self.tx_primary
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send certificate to primary");

                if let Err(e) = self.tx_output.send(output).await {
                    warn!("Failed to output certificate: {e}");
                }
            }
        }
        Ok(())
    }

    pub fn process_certificate(
        committee: &Committee<PublicKey>,
        store: &Arc<ConsensusStore<PublicKey>>,
        gc_depth: Round,
        state: &mut State<PublicKey>,
        consensus_index: SequenceNumber,
        certificate: Certificate<PublicKey>,
    ) -> StoreResult<Vec<ConsensusOutput<PublicKey>>> {
        debug!("Processing {:?}", certificate);
        let round = certificate.round();
        let mut consensus_index = consensus_index;

        // Add the new certificate to the local storage.
        state
            .dag
            .entry(round)
            .or_insert_with(HashMap::new)
            .insert(certificate.origin(), (certificate.digest(), certificate));

        // Try to order the dag to commit. Start from the highest round for which we have at least
        // 2f+1 certificates. This is because we need them to reveal the common coin.
        let r = round - 1;

        // We only elect leaders for even round numbers.
        if r % 2 != 0 || r < 4 {
            return Ok(Vec::new());
        }

        // Get the certificate's digest of the leader of round r-2. If we already ordered this leader,
        // there is nothing to do.
        let leader_round = r - 2;
        if leader_round <= state.last_committed_round {
            return Ok(Vec::new());
        }
        let (leader_digest, leader) = match Consensus::leader(committee, leader_round, &state.dag) {
            Some(x) => x,
            None => return Ok(Vec::new()),
        };

        // Check if the leader has f+1 support from its children (ie. round r-1).
        let stake: Stake = state
            .dag
            .get(&(r - 1))
            .expect("We should have the whole history by now")
            .values()
            .filter(|(_, x)| x.header.parents.contains(leader_digest))
            .map(|(_, x)| committee.stake(&x.origin()))
            .sum();

        // If it is the case, we can commit the leader. But first, we need to recursively go back to
        // the last committed leader, and commit all preceding leaders in the right order. Committing
        // a leader block means committing all its dependencies.
        if stake < committee.validity_threshold() {
            debug!("Leader {:?} does not have enough support", leader);
            return Ok(Vec::new());
        }

        // Get an ordered list of past leaders that are linked to the current leader.
        debug!("Leader {:?} has enough support", leader);
        let mut sequence = Vec::new();
        for leader in Consensus::order_leaders(committee, leader, state)
            .iter()
            .rev()
        {
            // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
            for x in Consensus::order_dag(gc_depth, leader, state) {
                let digest = x.digest();

                // Update and clean up internal state.
                state.update(&x, gc_depth);

                // Add the certificate to the sequence.
                sequence.push(ConsensusOutput {
                    certificate: x,
                    consensus_index,
                });

                // Increase the global consensus index.
                consensus_index += 1;

                // Persist the update.
                // TODO [issue #116]: Ensure this is not a performance bottleneck.
                store.write_consensus_state(&state.last_committed, &consensus_index, &digest)?;
            }
        }

        // Log the latest committed round of every authority (for debug).
        // Performance note: if tracing at the debug log level is disabled, this is cheap, see
        // https://github.com/tokio-rs/tracing/pull/326
        for (name, round) in &state.last_committed {
            debug!("Latest commit of {}: Round {}", name.encode_base64(), round);
        }

        Ok(sequence)
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    fn leader<'a>(
        committee: &Committee<PublicKey>,
        round: Round,
        dag: &'a Dag<PublicKey>,
    ) -> Option<&'a (CertificateDigest, Certificate<PublicKey>)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        #[cfg(test)]
        let coin = 0;
        #[cfg(not(test))]
        let coin = round;

        // Elect the leader.
        let mut keys: Vec<_> = committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader = &keys[coin as usize % committee.size()];

        // Return its certificate and the certificate's digest.
        dag.get(&round).and_then(|x| x.get(leader))
    }

    /// Order the past leaders that we didn't already commit.
    fn order_leaders(
        committee: &Committee<PublicKey>,
        leader: &Certificate<PublicKey>,
        state: &State<PublicKey>,
    ) -> Vec<Certificate<PublicKey>> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        for r in (state.last_committed_round + 2..leader.round())
            .rev()
            .step_by(2)
        {
            // Get the certificate proposed by the previous leader.
            let (_, prev_leader) = match Consensus::leader(committee, r, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            // Check whether there is a path between the last two leaders.
            if Consensus::linked(leader, prev_leader, &state.dag) {
                to_commit.push(prev_leader.clone());
                leader = prev_leader;
            }
        }
        to_commit
    }

    /// Checks if there is a path between two leaders.
    fn linked(
        leader: &Certificate<PublicKey>,
        prev_leader: &Certificate<PublicKey>,
        dag: &Dag<PublicKey>,
    ) -> bool {
        let mut parents = vec![leader];
        for r in (prev_leader.round()..leader.round()).rev() {
            parents = dag
                .get(&(r))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        parents.contains(&prev_leader)
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(
        gc_depth: Round,
        leader: &Certificate<PublicKey>,
        state: &State<PublicKey>,
    ) -> Vec<Certificate<PublicKey>> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .and_then(|x| x.values().find(|(x, _)| x == parent))
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + gc_depth >= state.last_committed_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}

#[cfg(test)]
mod tests {
    use super::{consensus_tests::*, *};
    use crypto::traits::KeyPair;
    use rand::Rng;
    use std::collections::BTreeSet;
    use types::Certificate;

    #[test]
    fn state_limits_test() {
        let gc_depth = 12;
        let rounds: Round = rand::thread_rng().gen_range(10, 100);

        // process certificates for rounds, check we don't grow the dag too much
        let keys: Vec<_> = test_utils::keys()
            .into_iter()
            .map(|kp| kp.public().clone())
            .collect();

        let genesis = Certificate::genesis(&mock_committee(&keys[..]))
            .iter()
            .map(|x| x.digest())
            .collect::<BTreeSet<_>>();
        let (certificates, _next_parents) = make_optimal_certificates(1, rounds, &genesis, &keys);
        let committee = mock_committee(&keys);

        let store_path = test_utils::temp_dir();
        let store = make_consensus_store(&store_path);

        let consensus_index = 0;
        let mut state = State::new(Certificate::genesis(&mock_committee(&keys[..])));
        for certificate in certificates {
            Consensus::process_certificate(
                &committee,
                &store,
                gc_depth,
                &mut state,
                consensus_index,
                certificate,
            )
            .unwrap();
        }
        // with "optimal" certificates (see `make_optimal_certificates`), and a round-robin between leaders,
        // we need at most 6 rounds lookbehind: we elect a leader at most at round r-2, and its round is
        // preceded by one round of history for each prior leader, which contains their latest commit at least.
        //
        // -- L1's latest
        // -- L2's latest
        // -- L3's latest
        // -- L4's latest
        // -- support level 1 (for L4)
        // -- support level 2 (for L4)
        //
        let n = state.dag.len();
        assert!(n <= 6, "DAG size: {}", n);
    }

    #[test]
    fn imperfect_state_limits_test() {
        let gc_depth = 12;
        let rounds: Round = rand::thread_rng().gen_range(10, 100);

        // process certificates for rounds, check we don't grow the dag too much
        let keys: Vec<_> = test_utils::keys()
            .into_iter()
            .map(|kp| kp.public().clone())
            .collect();

        let genesis = Certificate::genesis(&mock_committee(&keys[..]))
            .iter()
            .map(|x| x.digest())
            .collect::<BTreeSet<_>>();
        // TODO: evidence that this test fails when `failure_probability` parameter >= 1/3
        let (certificates, _next_parents) = make_certificates(1, rounds, &genesis, &keys, 0.333);
        let committee = mock_committee(&keys);

        let store_path = test_utils::temp_dir();
        let store = make_consensus_store(&store_path);

        let mut state = State::new(Certificate::genesis(&mock_committee(&keys[..])));
        let consensus_index = 0;
        for certificate in certificates {
            Consensus::process_certificate(
                &committee,
                &store,
                gc_depth,
                &mut state,
                consensus_index,
                certificate,
            )
            .unwrap();
        }
        // with "less optimal" certificates (see `make_certificates`), we should keep at most gc_depth rounds lookbehind
        let n = state.dag.len();
        assert!(n <= gc_depth as usize, "DAG size: {}", n);
    }
}
