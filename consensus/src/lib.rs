// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

pub mod bullshark;
pub mod consensus;
pub mod dag;
pub mod metrics;
pub mod tusk;
mod utils;

use std::sync::Arc;

pub use crate::consensus::Consensus;

use config::Committee;
use metrics::ConsensusMetrics;
use serde::{Deserialize, Serialize};
use storage::CertificateStore;
use types::{Certificate, ConsensusStore, SequenceNumber};

/// The default channel size used in the consensus and subscriber logic.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

/// The output format of the consensus.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConsensusOutput {
    /// The sequenced certificate.
    pub certificate: Certificate,
    /// The (global) index associated with this certificate.
    pub consensus_index: SequenceNumber,
}

/// Recovers the consensus state from disk
pub async fn recover_consensus_state(
    committee: &Committee,
    consensus_store: &ConsensusStore,
    certificate_store: CertificateStore,
    consensus_metrics: Arc<ConsensusMetrics>,
    gc_depth: u64,
) -> crate::consensus::ConsensusState {
    let genesis = Certificate::genesis(committee);
    #[allow(clippy::mutable_key_type)]
    let recover_last_committed = consensus_store.read_last_committed();
    crate::consensus::ConsensusState::new_from_store(
        genesis,
        consensus_metrics,
        recover_last_committed,
        certificate_store,
        gc_depth,
    )
    .await
}
