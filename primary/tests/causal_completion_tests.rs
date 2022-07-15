// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use bytes::{Bytes, BytesMut};
use std::time::Duration;
use test_utils::cluster::Cluster;
use test_utils::{batch, transaction};
use tracing::{info, subscriber::set_global_default};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use types::{Batch, TransactionProto, TransactionsClient};

#[ignore]
#[tokio::test]
async fn test_read_causal_signed_certificates() {
    const CURRENT_ROUND_METRIC: &str = "narwhal_primary_current_round";

    // Enabled debug tracing so we can easily observe the
    // nodes logs.
    setup_tracing();

    let mut cluster = Cluster::new(None);

    // start the cluster
    let nodes = cluster.start(4).await;

    // Let primaries advance little bit
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Ensure all nodes advanced
    for node in nodes {
        let metric_family = node.registry.gather();

        for metric in metric_family {
            if metric.get_name() == CURRENT_ROUND_METRIC {
                let value = metric.get_metric().first().unwrap().get_gauge().get_value();

                info!("Metrics name {} -> {:?}", metric.get_name(), value);

                // If the current round is increasing then it means that the
                // node starts catching up and is proposing.
                assert!(value > 1.0, "Node didn't progress further than the round 1");
            }
        }
    }

    // Now stop node 0
    cluster.stop_node(0);

    // Let other primaries advance
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Now start the validator 0 again
    let node = cluster.start_node(0, true, true).await.unwrap();

    // Now check that the current round advances. Give the opportunity with a few
    // iterations. If metric hasn't picked up then we know that node can't make
    // progress.
    let mut node_made_progress = false;
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let metric_family = node.registry.gather();

        for metric in metric_family {
            if metric.get_name() == CURRENT_ROUND_METRIC {
                let value = metric.get_metric().first().unwrap().get_gauge().get_value();

                info!("Metrics name {} -> {:?}", metric.get_name(), value);

                // If the current round is increasing then it means that the
                // node starts catching up and is proposing.
                if value > 1.0 {
                    node_made_progress = true;
                    break;
                }
            }
        }
    }

    assert!(
        node_made_progress,
        "Node 0 didn't make progress - causal completion didn't succeed"
    );
}

#[tokio::test]
async fn test_restore_consensus() {
    let mut cluster = Cluster::new(None);

    // start the cluster with worker and preserved store
    let (nodes, worker) = cluster.start_with_worker(1, true).await;
    let primary = nodes.first().unwrap();

    // submit transactions to the worker for consensus
    let addr = worker.transaction_addr.as_ref().unwrap().to_string();
    let mut client = TransactionsClient::connect(addr).await.unwrap();
    for tx in vec![transaction(), transaction(), transaction()] {
        let txn = TransactionProto {
            transaction: Bytes::from(tx.clone()),
        };
        client.submit_transaction(txn).await.unwrap();
    }

    // restart the nodes
    cluster.stop_node(primary.id);
    let _restarted = cluster.start_node(primary.id, true, true).await.unwrap();

    // listen on all primary tr_transaction_confirmation receivers for the output of the transactions
    // that we submitted to consensus
    let confirmation = _restarted.tr_transaction_confirmation.unwrap().recv().await;
}

fn setup_tracing() {
    // Setup tracing
    let tracing_level = "debug";
    let network_tracing_level = "info";

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .parse(format!(
            "{tracing_level},h2={network_tracing_level},tower={network_tracing_level},hyper={network_tracing_level},tonic::transport={network_tracing_level}"
        )).unwrap();
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(filter);
    let subscriber_builder =
        tracing_subscriber::fmt::Subscriber::builder().with_env_filter(env_filter);

    let subscriber = subscriber_builder.with_writer(std::io::stderr).finish();
    set_global_default(subscriber).expect("Failed to set subscriber");
}
