// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use bytes::Bytes;
use std::time::Duration;
use telemetry_subscribers::TelemetryGuards;
use test_utils::cluster::Cluster;
use tracing::info;
use types::{TransactionProto, TransactionsClient};

type StringTransaction = String;

#[ignore]
#[tokio::test]
async fn test_restore_from_disk() {
    // Enabled debug tracing so we can easily observe the
    // nodes logs.
    let _guard = setup_tracing();

    let mut cluster = Cluster::new(None, None);

    // start the cluster
    cluster.start(Some(4), Some(1), None).await;

    let id = 0;
    let name = cluster.authority(0).name;

    let committee = &cluster.committee_shared;
    let address = committee.load().worker(&name, &id).unwrap().transactions;
    let config = mysten_network::config::Config::new();
    let channel = config.connect_lazy(&address).unwrap();
    let mut client = TransactionsClient::new(channel);

    // Subscribe to the transaction confirmation channel
    let mut receiver = cluster
        .authority(0)
        .primary()
        .await
        .tx_transaction_confirmation
        .subscribe();

    // Create arbitrary transactions
    let mut batch_len = 3;
    for tx in [
        string_transaction(),
        string_transaction(),
        string_transaction(),
    ] {
        let tr = bincode::serialize(&tx).unwrap();
        let txn = TransactionProto {
            transaction: Bytes::from(tr),
        };
        client.submit_transaction(txn).await.unwrap();
    }

    // wait for transactions to complete
    loop {
        if let Ok(result) = receiver.recv().await {
            assert!(result.0.is_ok());
            batch_len -= 1;
            if batch_len < 1 {
                break;
            }
        }
    }

    // Now stop node 0
    cluster.stop_node(0).await;

    // Let other primaries advance
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Now start the node 0 again
    cluster.start_node(0, true, Some(1)).await;

    // Let the node recover
    tokio::time::sleep(Duration::from_secs(2)).await;

    let node = cluster.authority(0);

    // Check the metrics to ensure the node was recovered from disk
    let primary = node.primary().await;

    let node_recovered_state =
        if let Some(metric) = primary.metric("narwhal_primary_recovered_consensus_state") {
            let value = metric.get_counter().get_value();
            info!("Found metric for recovered consensus state.");

            value > 0.0
        } else {
            false
        };

    assert!(node_recovered_state, "Node did not recover state from disk");
}

fn string_transaction() -> StringTransaction {
    StringTransaction::from("test transaction")
}

#[ignore]
#[tokio::test]
async fn test_read_causal_signed_certificates() {
    const CURRENT_ROUND_METRIC: &str = "narwhal_primary_current_round";

    // Enabled debug tracing so we can easily observe the
    // nodes logs.
    let _guard = setup_tracing();

    let mut cluster = Cluster::new(None, None);

    // start the cluster
    cluster.start(Some(4), Some(1), None).await;

    // Let primaries advance little bit
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Ensure all nodes advanced
    for authority in cluster.authorities().await {
        if let Some(metric) = authority.primary().await.metric(CURRENT_ROUND_METRIC) {
            let value = metric.get_gauge().get_value();

            info!("Metric -> {:?}", value);

            // If the current round is increasing then it means that the
            // node starts catching up and is proposing.
            assert!(value > 1.0, "Node didn't progress further than the round 1");
        }
    }

    // Now stop node 0
    cluster.stop_node(0).await;

    // Let other primaries advance
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Now start the validator 0 again
    cluster.start_node(0, true, Some(1)).await;

    // Now check that the current round advances. Give the opportunity with a few
    // iterations. If metric hasn't picked up then we know that node can't make
    // progress.
    let mut node_made_progress = false;
    let node = cluster.authority(0).primary().await;

    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(1)).await;

        if let Some(metric) = node.metric(CURRENT_ROUND_METRIC) {
            let value = metric.get_gauge().get_value();
            info!("Metric -> {:?}", value);

            // If the current round is increasing then it means that the
            // node starts catching up and is proposing.
            if value > 1.0 {
                node_made_progress = true;
                break;
            }
        }
    }

    assert!(
        node_made_progress,
        "Node 0 didn't make progress - causal completion didn't succeed"
    );
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
