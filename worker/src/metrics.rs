use mysten_network::metrics::MetricsCallbackProvider;
use std::time::Duration;
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use prometheus::{
    default_registry, register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_vec_with_registry, HistogramVec, IntCounterVec, IntGaugeVec, Registry,
};
use tonic::Code;

#[derive(Clone)]
pub struct Metrics {
    pub worker_metrics: Option<WorkerMetrics>,
    pub endpoint_metrics: Option<WorkerEndpointMetrics>,
}

/// Initialises the metrics. Should be called only once when the worker
/// node is initialised, otherwise it will lead to erroneously creating
/// multiple registries.
pub fn initialise_metrics(metrics_registry: &Registry) -> Metrics {
    // Essential/core metrics across the worker node
    let node_metrics = WorkerMetrics::new(metrics_registry);

    // Endpoint metrics
    let endpoint_metrics = WorkerEndpointMetrics::new(metrics_registry);

    Metrics {
        worker_metrics: Some(node_metrics),
        endpoint_metrics: Some(endpoint_metrics),
    }
}

#[derive(Clone)]
pub struct WorkerMetrics {
    /// Number of elements pending elements in the worker synchronizer
    pub pending_elements_worker_synchronizer: IntGaugeVec,
}

impl WorkerMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            pending_elements_worker_synchronizer: register_int_gauge_vec_with_registry!(
                "pending_elements_worker_synchronizer",
                "Number of pending elements in worker block synchronizer",
                &["epoch"],
                registry
            )
            .unwrap(),
        }
    }
}

impl Default for WorkerMetrics {
    fn default() -> Self {
        Self::new(default_registry())
    }
}

#[derive(Clone)]
pub struct WorkerEndpointMetrics {
    /// Counter of requests, route is a label (ie separate timeseries per route)
    requests_by_route: IntCounterVec,
    /// Request latency, route is a label
    req_latency_by_route: HistogramVec,
}

impl WorkerEndpointMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            requests_by_route: register_int_counter_vec_with_registry!(
                "worker_requests_by_route",
                "Number of requests by route",
                &["route", "status", "grpc_status_code"],
                registry
            )
            .unwrap(),
            req_latency_by_route: register_histogram_vec_with_registry!(
                "worker_req_latency_by_route",
                "Latency of a request by route",
                &["route", "status", "grpc_status_code"],
                registry
            )
            .unwrap(),
        }
    }
}

impl MetricsCallbackProvider for WorkerEndpointMetrics {
    fn on_request(&self, _path: String) {
        // For now we just do nothing
    }

    fn on_response(&self, path: String, latency: Duration, status: u16, grpc_status_code: Code) {
        let code: i32 = grpc_status_code.into();
        let labels = [path.as_str(), &status.to_string(), &code.to_string()];

        self.requests_by_route.with_label_values(&labels).inc();

        let req_latency_secs = latency.as_secs_f64();
        self.req_latency_by_route
            .with_label_values(&labels)
            .observe(req_latency_secs);
    }
}

impl Default for WorkerEndpointMetrics {
    fn default() -> Self {
        Self::new(default_registry())
    }
}
