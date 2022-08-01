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
    let cluster = Cluster::new(None, None, true);

    // ==== Start first authority ====
    cluster.authority(0).start(false, Some(1)).await;

    tokio::time::sleep(node_staggered_delay).await;

    let rounds = authorities_latest_commit_round(&cluster).await;
    assert_eq!(
        rounds.len(),
        0,
        "No node should be able to commit, no reported round was expected"
    );

    // ==== Start second authority ====
    cluster.authority(1).start(false, Some(1)).await;

    tokio::time::sleep(node_staggered_delay).await;

    let rounds = authorities_latest_commit_round(&cluster).await;
    assert_eq!(
        rounds.len(),
        0,
        "No node should be able to commit, no reported round was expected"
    );

    // ==== Start third authority ====
    // Now 2f + 1 nodes are becoming available and we expect all the nodes to
    // start making progress (advance in rounds).
    cluster.authority(2).start(false, Some(1)).await;

    tokio::time::sleep(node_staggered_delay).await;

    let rounds = authorities_latest_commit_round(&cluster).await;
    assert_eq!(
        rounds.len(),
        3,
        "Expected to have received commit metrics from only three nodes"
    );
    assert!(rounds.values().all(|v| *v > 1f64), "We have only (f) unavailable nodes, so all should have made progress and committed at least after the first round");

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

    let rounds = authorities_latest_commit_round(&cluster).await;
    assert_eq!(
        rounds.len(),
        4,
        "Expected to have received commit metrics from all nodes"
    );
    assert!(rounds.values().all(|v| v > &1.0), "All nodes are available so all should have made progress and committed at least after the first round");

    // We expect all the nodes to have managed to catch up by now
    // and be pretty much in similar rounds. The threshold here is
    // not defined by some rule but more of an expectation.
    let (min, max) = rounds.values().into_iter().minmax().into_option().unwrap();
    assert!(max - min <= 2.0, "Nodes shouldn't be that behind");
}

#[ignore]
#[tokio::test]
async fn test_second_node_restart() {
    // Enabled debug tracing so we can easily observe the
    // nodes logs.
    let _guard = setup_tracing();

    let restart_delay = Duration::from_secs(120);
    let node_advance_delay = Duration::from_secs(60);

    // A cluster of 4 nodes will be created
    let mut cluster = Cluster::new(None, None, true);

    // ===== Start the cluster ====
    cluster.start(Some(4), Some(1), None).await;

    // Let the nodes advance a bit
    tokio::time::sleep(node_advance_delay).await;

    // Now restart node 2 with some delay between
    cluster.authority(2).restart(true, restart_delay).await;

    // now wait a bit to give the opportunity to recover
    tokio::time::sleep(node_advance_delay).await;

    // Ensure that nodes have made progress
    let rounds = authorities_latest_commit_round(&cluster).await;
    assert_eq!(
        rounds.len(),
        4,
        "Expected to have received commit metrics from all nodes"
    );
    assert!(rounds.values().all(|v| v > &1.0), "All nodes are available so all should have made progress and committed at least after the first round");

    // We expect all the nodes to have managed to catch up by now
    // and be pretty much in similar rounds. The threshold here is
    // not defined by some rule but more of an expectation.
    let (min, max) = rounds.values().into_iter().minmax().into_option().unwrap();
    assert!(max - min <= 2.0, "Nodes shouldn't be that behind");

    // Now restart node 3 with some delay between
    cluster.authority(3).restart(true, restart_delay).await;

    // now wait a bit to give the opportunity to recover
    tokio::time::sleep(node_advance_delay).await;

    // Ensure that nodes have made progress
    let rounds = authorities_latest_commit_round(&cluster).await;
    assert_eq!(
        rounds.len(),
        4,
        "Expected to have received commit metrics from all nodes"
    );
    assert!(rounds.values().all(|v| v > &1.0), "All nodes are available so all should have made progress and committed at least after the first round");

    // We expect all the nodes to have managed to catch up by now
    // and be pretty much in similar rounds. The threshold here is
    // not defined by some rule but more of an expectation.
    let (min, max) = rounds.values().into_iter().minmax().into_option().unwrap();
    assert!(max - min <= 2.0, "Nodes shouldn't be that behind");
}

#[ignore]
#[tokio::test]
/// We are testing the loss of liveness of a healthy cluster. While 3f+1 nodes run
/// we are shutting down f+1 nodes. Then we resume and ensure that system is able
/// to advance as normal.
async fn test_loss_of_liveness() {
    // Enabled debug tracing so we can easily observe the
    // nodes logs.
    let _guard = setup_tracing();

    let node_advance_delay = Duration::from_secs(120);

    // A cluster of 4 nodes will be created
    let mut cluster = Cluster::new(None, None, true);

    // ===== Start the cluster ====
    cluster.start(Some(4), Some(1), None).await;

    // Let the nodes advance a bit
    tokio::time::sleep(node_advance_delay).await;

    // Ensure that nodes have made progress
    cluster.assert_progress(4, 2).await;

    // Now stop node 2 & 3
    cluster.authority(2).stop_all().await;
    cluster.authority(3).stop_all().await;

    // wait and fetch the latest commit round
    tokio::time::sleep(node_advance_delay).await;
    let rounds_1 = cluster.assert_progress(2, 0).await;

    // wait and fetch again the rounds
    tokio::time::sleep(node_advance_delay).await;
    let rounds_2 = cluster.assert_progress(2, 0).await;

    // We assert that nodes haven't advanced at all
    assert_eq!(rounds_1, rounds_2);

    // Now bring up nodes
    cluster.authority(2).start(true, Some(1)).await;
    cluster.authority(3).start(true, Some(1)).await;

    // wait and fetch the latest commit round
    tokio::time::sleep(node_advance_delay).await;
    let rounds_3 = cluster.assert_progress(4, 0).await;

    let round_2_max = rounds_2.values().into_iter().max().unwrap();
    assert!(
        rounds_3.values().all(|v| v > round_2_max),
        "All the nodes should have advanced more from the previous round"
    );
}

async fn authorities_latest_commit_round(cluster: &Cluster) -> HashMap<usize, f64> {
    let mut authorities_latest_commit = HashMap::new();

    for authority in cluster.authorities().await {
        let primary = authority.primary().await;
        if let Some(metric) = primary.metric("narwhal_primary_last_committed_round") {
            let value = metric.get_gauge().get_value();

            authorities_latest_commit.insert(primary.id, value);

            info!(
                "[Node {}] Metric narwhal_primary_last_committed_round -> {value}",
                primary.id
            );
        }
    }

    authorities_latest_commit
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
