// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use itertools::Itertools;
use std::{collections::HashMap, time::Duration};
use test_utils::cluster::Cluster;
use tracing::info;

/// Nodes will be started in a staggered fashion. This is simulating
/// a real world scenario where nodes across validators will not start
/// in the same time.
#[tokio::test]
async fn test_node_staggered_starts() {
    // Enabled debug tracing so we can easily observe the
    // nodes logs.
    setup_tracing();

    // 5 minutes
    let node_staggered_delay = Duration::from_secs(60 * 5);

    let mut cluster = Cluster::new(None, None);

    // Start the cluster in staggered fashion with some delay between the nodes start
    cluster
        .start(Some(4), Some(1), Some(node_staggered_delay))
        .await;

    let mut authorities_current_round = HashMap::new();

    for _ in 0..5 {
        for authority in cluster.authorities().await {
            let primary = authority.primary().await;
            if let Some(metric) = primary.metric("narwhal_primary_current_round") {
                let value = metric.get_gauge().get_value();

                authorities_current_round.insert(primary.id, value);

                info!(
                    "[Node {}] Metric narwhal_primary_current_round -> {value}",
                    primary.id
                );
            }
        }

        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    // we should have metrics from all the three nodes
    assert_eq!(authorities_current_round.len(), 4);

    // they should all be above 0
    assert!(authorities_current_round.values().all(|v| v > &0.0));

    // nodes shouldn't have round diff more than 2
    let (min, max) = authorities_current_round
        .values()
        .into_iter()
        .minmax()
        .into_option()
        .unwrap();
    assert!(max - min <= 2.0, "Nodes shouldn't be that behind");
}

fn setup_tracing() {
    // Setup tracing
    let tracing_level = "debug";
    let network_tracing_level = "info";

    let log_filter = format!("{tracing_level},h2={network_tracing_level},tower={network_tracing_level},hyper={network_tracing_level},tonic::transport={network_tracing_level}");

    let _guard = telemetry_subscribers::TelemetryConfig::new("narwhal")
        // load env variables
        .with_env()
        // load special log filter
        .with_log_level(&log_filter)
        .init();
}
