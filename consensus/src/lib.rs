// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

// pub mod bullshark;
pub mod dag;
pub mod subscriber;
pub mod tusk;
mod utils;

pub use crate::{subscriber::SubscriberHandler, tusk::Consensus};
use crypto::{traits::VerifyingKey, Hash};
use serde::{Deserialize, Serialize};
use std::{cmp::max, collections::HashMap, ops::RangeInclusive};
use types::{Certificate, CertificateDigest, Round, SequenceNumber};

/// The default channel size used in the consensus and subscriber logic.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

/// The output format of the consensus.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(bound(deserialize = "PublicKey: VerifyingKey"))]
pub struct ConsensusOutput<PublicKey: VerifyingKey> {
    /// The sequenced certificate.
    pub certificate: Certificate<PublicKey>,
    /// The (global) index associated with this certificate.
    pub consensus_index: SequenceNumber,
}

/// The message sent by the client to sync missing chunks of the output sequence.
#[derive(Serialize, Deserialize, Debug)]
pub struct ConsensusSyncRequest {
    /// The sequence numbers of the missing consensus outputs.
    pub missing: RangeInclusive<SequenceNumber>,
}

/// The representation of the DAG in memory.
pub type Dag<PublicKey> =
    HashMap<Round, HashMap<PublicKey, (CertificateDigest, Certificate<PublicKey>)>>;

/// The state that needs to be persisted for crash-recovery.
pub struct ConsensusState<PublicKey: VerifyingKey> {
    /// The last committed round.
    last_committed_round: Round,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag<PublicKey>,
}

impl<PublicKey: VerifyingKey> ConsensusState<PublicKey> {
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
