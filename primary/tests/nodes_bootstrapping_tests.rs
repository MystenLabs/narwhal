// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use itertools::Itertools;
use std::{collections::HashMap, time::Duration};
use telemetry_subscribers::TelemetryGuards;
use test_utils::cluster::Cluster;
use tracing::info;

/// Nodes will be started in a staggered fashion. This is simulating
/// a real world scenario where nodes across validators will not start
/// in the same time.
#[ignore]
#[tokio::test]
async fn test_node_staggered_starts() {
    // Enabled debug tracing so we can easily observe the
    // nodes logs.
    let _guard = setup_tracing();

    let node_staggered_delay = Duration::from_secs(60 * 5); // 5 minutes

    // A cluster of 4 nodes will be created
    let cluster = Cluster::new(None, None);

    // ==== Start first authority ====
    cluster.authority(0).start(false, Some(1)).await;

    tokio::time::sleep(node_staggered_delay).await;

    let rounds = authorities_current_round(&cluster).await;
    assert_eq!(
        rounds.len(),
        1,
        "Expected to have received metric only from one node"
    );
    assert!(rounds.values().all(|v| v == &1.0), "We have (f+2) unavailable nodes and expected all nodes to have made progress up to round 1 only");

    // ==== Start second authority ====
    cluster.authority(1).start(false, Some(1)).await;

    tokio::time::sleep(node_staggered_delay).await;

    let rounds = authorities_current_round(&cluster).await;
    assert_eq!(
        rounds.len(),
        2,
        "Expected to have received metrics from only two nodes"
    );
    assert!(rounds.values().all(|v| v == &1.0), "We have (f+1) unavailable nodes and expected all nodes to have made progress up to round 1 only");

    // ==== Start third authority ====
    // Now 2f + 1 nodes are becoming available and we expect all the nodes to
    // start making progress (advance in rounds).
    cluster.authority(2).start(false, Some(1)).await;

    tokio::time::sleep(node_staggered_delay).await;

    let rounds = authorities_current_round(&cluster).await;
    assert_eq!(
        rounds.len(),
        3,
        "Expected to have received metrics from only three nodes"
    );
    assert!(rounds.values().all(|v| v > &1.0), "We have only (f) unavailable nodes, so all should have made progress more than just the first round");

    // We expect all the nodes to have managed to catch up by now
    // and be pretty much in similar rounds. The threshold here is
    // not defined by some rule but more of an expectation.
    let (min, max) = rounds.values().into_iter().minmax().into_option().unwrap();
    assert!(max - min <= 2.0, "Nodes shouldn't be that behind");

    // ==== Start fourth authority ====
    // Now 3f + 1 nodes are becoming available (the whole network) and all the nodes
    // should make progress
    cluster.authority(3).start(false, Some(1)).await;

    tokio::time::sleep(node_staggered_delay).await;

    let rounds = authorities_current_round(&cluster).await;
    assert_eq!(
        rounds.len(),
        4,
        "Expected to have received metrics from all nodes"
    );
    assert!(rounds.values().all(|v| v > &1.0), "We have no unavailable nodes, so all should have made progress more than just the first round");

    // We expect all the nodes to have managed to catch up by now
    // and be pretty much in similar rounds. The threshold here is
    // not defined by some rule but more of an expectation.
    let (min, max) = rounds.values().into_iter().minmax().into_option().unwrap();
    assert!(max - min <= 2.0, "Nodes shouldn't be that behind");
}

async fn authorities_current_round(cluster: &Cluster) -> HashMap<usize, f64> {
    let mut authorities_current_round = HashMap::new();

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

    authorities_current_round
}

fn setup_tracing() -> TelemetryGuards {
    // Setup tracing
    let tracing_level = "debug";
    let network_tracing_level = "info";

    let log_filter = format!("{tracing_level},h2={network_tracing_level},tower={network_tracing_level},hyper={network_tracing_level},tonic::transport={network_tracing_level}");

    telemetry_subscribers::TelemetryConfig::new("narwhal")
        // load env variables
        .with_env()
        // load special log filter
        .with_log_level(&log_filter)
        .init()
        .0
}
