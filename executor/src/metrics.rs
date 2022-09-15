// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use prometheus::{
    default_registry, register_histogram_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry, Histogram, IntCounter, IntGauge, Registry,
};

#[derive(Clone, Debug)]
pub struct ExecutorMetrics {
    /// occupancy of the channel from the `Subscriber` to `Core`
    pub tx_executor: IntGauge,
    /// Number of elements in the waiting (ready-to-deliver) list of subscriber
    pub waiting_elements_subscriber: IntGauge,
    /// Number of times sends block command was successful
    pub send_block_command_successes: IntCounter,
    /// Number of times send block command resulted in an error response
    pub send_block_command_errors: IntCounter,
    /// Number of times send block command was retried
    pub send_block_command_retries: IntCounter,
    /// Latency spent waiting on certificate payload per certificate in ms
    pub fetch_payload_latency: Histogram,
    /// The number of retries per certificate when requesting the payload
    pub num_fetch_payload_regtries_per_certificate: Histogram,
}

impl ExecutorMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            tx_executor: register_int_gauge_with_registry!(
                "tx_executor",
                "occupancy of the channel from the `Subscriber` to `Core`",
                registry
            )
            .unwrap(),
            waiting_elements_subscriber: register_int_gauge_with_registry!(
                "waiting_elements_subscriber",
                "Number of waiting elements in the subscriber",
                registry
            )
            .unwrap(),
            send_block_command_successes: register_int_counter_with_registry!(
                "send_block_command_successes",
                "Number of times sends block command was successful",
                registry
            )
            .unwrap(),
            send_block_command_errors: register_int_counter_with_registry!(
                "send_block_command_errors",
                "Number of times send block command resulted in an error response",
                registry
            )
            .unwrap(),
            send_block_command_retries: register_int_counter_with_registry!(
                "send_block_command_retries",
                "Number of times send block command was retried",
                registry
            )
            .unwrap(),
            fetch_payload_latency: register_histogram_with_registry!(
                "fetch_payload_latency",
                "Latency spent waiting on certificate payload per certificate in ms",
                registry
            )
            .unwrap(),
            num_fetch_payload_retries_per_certificate: register_histogram_with_registry!(
                "num_fetch_payload_retries_per_certificate",
                "The number of retries per certificate when requesting the payload",
                registry
            )
            .unwrap(),
        }
    }
}

impl Default for ExecutorMetrics {
    fn default() -> Self {
        Self::new(default_registry())
    }
}
