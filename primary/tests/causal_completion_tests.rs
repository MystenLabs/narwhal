// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use arc_swap::access::{Access, DynAccess};
use arc_swap::ArcSwap;
use bytes::Bytes;
use config::Committee;
use crypto::ed25519::Ed25519PublicKey;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use test_utils::cluster::Cluster;
use test_utils::{committee, transaction};
use tracing::{info, subscriber::set_global_default};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use types::{TransactionProto, TransactionsClient};

#[ignore]
#[tokio::test]
async fn test_read_causal_signed_certificates() {
    // Enabled debug tracing so we can easily observe the
    // nodes logs.
    setup_tracing();

    let mut cluster = Cluster::new(None, None);

    // start the cluster
    cluster.start(Some(4), Some(1)).await;

    let multiaddr = cluster.authorities()[0].worker(0).transactions_address;
    let addr = multiaddr.to_string();

    let committee = arc_swap::access::Access::load(&cluster.committee_shared).deref();

    let id = 0;
    let name = cluster.authority(0).name;
    let address = c.worker(&name, &id).unwrap().transactions;
    let config = mysten_network::config::Config::new();
    let channel = config.connect_lazy(&address).unwrap();
    let mut client = TransactionsClient::new(channel);

    for tx in vec![transaction(), transaction(), transaction()] {
        let txn = TransactionProto {
            transaction: Bytes::from(tx.clone()),
        };
        client.submit_transaction(txn).await.unwrap();
    }
    // Let transactions get submitted
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Now stop node 0
    cluster.stop_node(0);

    // Let other primaries advance
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Now start the validator 0 again
    cluster.start_node(0, true, Some(1)).await;

    // Now check that the current round advances. Give the opportunity with a few
    // iterations. If metric hasn't picked up then we know that node can't make
    // progress.
    let mut node_recovered_state = false;
    let node = cluster.authority(0);

    // let mut receiver = node.primary.tx_transaction_confirmation.subscribe();
    // loop {
    //     if let Ok(result) = receiver.recv().await {
    //         assert!(result.0.is_ok());
    //         break;
    //     }
    // }

    for _ in 0..10 {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let metric_family = node.primary.registry.gather();

        for metric in metric_family {
            if metric.get_name() == "recovered_consensus_state" {
                let value = metric.get_metric().first().unwrap().get_gauge().get_value();
                info!("Found metric for recovered consensus state.");
                if value > 0.0 {
                    node_recovered_state = true;
                    break;
                }
            }
        }
    }

    assert!(node_recovered_state, "Node recovered state");
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
