// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::{NetworkModel, Primary, CHANNEL_CAPACITY};
use arc_swap::ArcSwap;
use config::Parameters;
use consensus::{dag::Dag, metrics::ConsensusMetrics};
use fastcrypto::traits::KeyPair;
use node::NodeStorage;
use prometheus::Registry;
use std::{sync::Arc, time::Duration};
use test_utils::{temp_dir, CommitteeFixture};
use tokio::sync::watch;
use types::ReconfigureNotification;

#[tokio::test]
async fn get_network_peers_from_admin_server() {
    // telemetry_subscribers::init_for_testing();
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();
    let authority = fixture.authorities().next().unwrap();
    let name = authority.public_key();
    let signer = authority.keypair().copy();

    // Make the data store.
    let store = NodeStorage::reopen(temp_dir());

    let (tx_new_certificates, rx_new_certificates) =
        test_utils::test_new_certificates_channel!(CHANNEL_CAPACITY);
    let (tx_feedback, rx_feedback) =
        test_utils::test_committed_certificates_channel!(CHANNEL_CAPACITY);
    let (tx_get_block_commands, rx_get_block_commands) = test_utils::test_get_block_commands!(1);
    let initial_committee = ReconfigureNotification::NewEpoch(committee.clone());
    let (tx_reconfigure, _rx_reconfigure) = watch::channel(initial_committee);
    let consensus_metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));

    Primary::spawn(
        name.clone(),
        signer,
        authority.network_keypair().copy(),
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        parameters.clone(),
        store.header_store.clone(),
        store.certificate_store.clone(),
        store.payload_store.clone(),
        store.vote_digest_store.clone(),
        /* tx_consensus */ tx_new_certificates,
        /* rx_consensus */ rx_feedback,
        /* dag */
        tx_get_block_commands,
        rx_get_block_commands,
        Some(Arc::new(
            Dag::new(&committee, rx_new_certificates, consensus_metrics).1,
        )),
        NetworkModel::Asynchronous,
        tx_reconfigure,
        tx_feedback,
        &Registry::new(),
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test getting all known peers
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/known_peers",
        parameters.network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 7 peers (3 other primaries + 4 workers)
    println!("{:?}", resp);
    assert_eq!(7, resp.len());

    // Test getting all connected peers
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        parameters.network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 0 peers, no other primaries spawned
    println!("{:?}", resp);
    assert_eq!(0, resp.len());
}
