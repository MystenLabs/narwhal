// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use prometheus::{default_registry, register_int_gauge_vec_with_registry, IntGaugeVec, Registry};

#[derive(Clone)]
pub struct ConsensusMetrics {
    /// The current Narwhal round
    pub consensus_dag_size: IntGaugeVec,
    /// The last committed round from consensus
    pub last_committed_round: IntGaugeVec,
}

impl ConsensusMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            consensus_dag_size: register_int_gauge_vec_with_registry!(
                "consensus_dag_size",
                "The number of elements (certificates) in consensus dag",
                &[],
                registry
            )
            .unwrap(),
            last_committed_round: register_int_gauge_vec_with_registry!(
                "last_committed_round",
                "The most recent round that has been committed from consensus",
                &[],
                registry
            )
            .unwrap(),
        }
    }
}

impl Default for ConsensusMetrics {
    fn default() -> Self {
        Self::new(default_registry())
    }
}
