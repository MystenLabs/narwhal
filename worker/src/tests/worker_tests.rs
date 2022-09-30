// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::metrics::initialise_metrics;
use arc_swap::ArcSwap;
use bytes::Bytes;
use consensus::{dag::Dag, metrics::ConsensusMetrics};
use fastcrypto::Hash;
use node::NodeStorage;
use primary::{NetworkModel, Primary, CHANNEL_CAPACITY};
use prometheus::Registry;
use std::time::Duration;
use store::rocks;
use test_utils::{
    batch, temp_dir, CommitteeFixture, WorkerToPrimaryMockServer, WorkerToWorkerMockServer,
};
use types::{TransactionsClient, WorkerPrimaryMessage};

#[tokio::test]
async fn handle_clients_transactions() {
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();

    let worker_id = 0;
    let my_primary = fixture.authorities().next().unwrap();
    let myself = my_primary.worker(worker_id);
    let name = my_primary.public_key();

    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Create a new test store.
    let db = rocks::DBMap::<BatchDigest, Batch>::open(temp_dir(), None, Some("batches")).unwrap();
    let store = Store::new(db);

    let registry = Registry::new();
    let metrics = initialise_metrics(&registry);

    // Spawn a `Worker` instance.
    Worker::spawn(
        name.clone(),
        myself.keypair(),
        worker_id,
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        parameters,
        store,
        metrics,
    );

    // Spawn a network listener to receive our batch's digest.
    let batch = batch();
    let batch_digest = batch.digest();

    let primary_address = committee.primary(&name).unwrap();
    let expected = WorkerPrimaryMessage::OurBatch(batch_digest, worker_id);
    let (mut handle, _network) =
        WorkerToPrimaryMockServer::spawn(my_primary.network_keypair().copy(), primary_address);

    // Spawn enough workers' listeners to acknowledge our batches.
    let mut other_workers = Vec::new();
    for worker in fixture.authorities().skip(1).map(|a| a.worker(worker_id)) {
        let handle =
            WorkerToWorkerMockServer::spawn(worker.keypair(), worker.info().worker_address.clone());
        other_workers.push(handle);
    }

    // Wait till other services have been able to start up
    tokio::task::yield_now().await;
    // Send enough transactions to create a batch.
    let address = worker_cache
        .load()
        .worker(&name, &worker_id)
        .unwrap()
        .transactions;
    let config = mysten_network::config::Config::new();
    let channel = config.connect_lazy(&address).unwrap();
    let mut client = TransactionsClient::new(channel);
    for tx in batch.0 {
        let txn = TransactionProto {
            transaction: Bytes::from(tx.clone()),
        };
        client.submit_transaction(txn).await.unwrap();
    }

    // Ensure the primary received the batch's digest (ie. it did not panic).
    assert_eq!(handle.recv().await.unwrap(), expected);
}

#[tokio::test]
async fn get_network_peers_from_admin_server() {
    // telemetry_subscribers::init_for_testing();
    let primary_1_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let worker_cache = fixture.shared_worker_cache();
    let authority_1 = fixture.authorities().next().unwrap();
    let name_1 = authority_1.public_key();
    let signer_1 = authority_1.keypair().copy();

    let worker_id = 0;
    let worker_1_keypair = authority_1.worker(worker_id).keypair().copy();

    // Make the data store.
    let store = NodeStorage::reopen(temp_dir());

    let (tx_new_certificates, rx_new_certificates) =
        test_utils::test_new_certificates_channel!(CHANNEL_CAPACITY);
    let (tx_feedback, rx_feedback) = test_utils::test_channel!(CHANNEL_CAPACITY);
    let (tx_get_block_commands, rx_get_block_commands) = test_utils::test_get_block_commands!(1);
    let initial_committee = ReconfigureNotification::NewEpoch(committee.clone());
    let (tx_reconfigure, _rx_reconfigure) = watch::channel(initial_committee);
    let consensus_metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));

    // Spawn Primary 1
    Primary::spawn(
        name_1.clone(),
        signer_1,
        authority_1.network_keypair().copy(),
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        primary_1_parameters.clone(),
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
        None,
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let registry_1 = Registry::new();
    let metrics_1 = initialise_metrics(&registry_1);

    let worker_1_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Spawn a `Worker` instance for primary 1.
    Worker::spawn(
        name_1,
        worker_1_keypair,
        worker_id,
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        worker_1_parameters.clone(),
        store.batch_store.clone(),
        metrics_1.clone(),
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test getting all known peers for worker 1
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/known_peers",
        worker_1_parameters.network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 3 peers (1 primaries + 3 other workers)
    assert_eq!(4, resp.len());

    // Test getting all connected peers for worker 1 (worker at index 0 for primary 1)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        worker_1_parameters.network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 1 peers (only worker's primary spawned)
    assert_eq!(1, resp.len());

    let authority_2 = fixture.authorities().nth(1).unwrap();
    let name_2 = authority_2.public_key();
    let signer_2 = authority_2.keypair().copy();

    let worker_2_keypair = authority_2.worker(worker_id).keypair().copy();

    let primary_2_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    let (tx_new_certificates_2, rx_new_certificates_2) =
        test_utils::test_new_certificates_channel!(CHANNEL_CAPACITY);
    let (tx_feedback_2, rx_feedback_2) = test_utils::test_channel!(CHANNEL_CAPACITY);
    let (tx_get_block_commands_2, rx_get_block_commands_2) =
        test_utils::test_get_block_commands!(1);
    let initial_committee = ReconfigureNotification::NewEpoch(committee.clone());
    let (tx_reconfigure_2, _rx_reconfigure_2) = watch::channel(initial_committee);
    let consensus_metrics = Arc::new(ConsensusMetrics::new(&Registry::new()));

    // Spawn Primary 2
    Primary::spawn(
        name_2.clone(),
        signer_2,
        authority_2.network_keypair().copy(),
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        primary_2_parameters.clone(),
        store.header_store.clone(),
        store.certificate_store.clone(),
        store.payload_store.clone(),
        store.vote_digest_store.clone(),
        /* tx_consensus */ tx_new_certificates_2,
        /* rx_consensus */ rx_feedback_2,
        /* dag */
        tx_get_block_commands_2,
        rx_get_block_commands_2,
        Some(Arc::new(
            Dag::new(&committee, rx_new_certificates_2, consensus_metrics).1,
        )),
        NetworkModel::Asynchronous,
        tx_reconfigure_2,
        tx_feedback_2,
        &Registry::new(),
        None,
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    let registry_2 = Registry::new();
    let metrics_2 = initialise_metrics(&registry_2);

    let worker_2_parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Spawn a `Worker` instance for primary 2.
    Worker::spawn(
        name_2,
        worker_2_keypair,
        worker_id,
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        worker_2_parameters.clone(),
        store.batch_store,
        metrics_2.clone(),
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test getting all connected peers for worker 1 (worker at index 0 for primary 1)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        worker_1_parameters.network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 2 peers (1 primary spawned + 1 other worker spawned)
    assert_eq!(2, resp.len());

    // Test getting all connected peers for worker 2 (worker at index 0 for primary 2)
    let resp = reqwest::get(format!(
        "http://127.0.0.1:{}/peers",
        worker_2_parameters.network_admin_server_port
    ))
    .await
    .unwrap()
    .json::<Vec<String>>()
    .await
    .unwrap();

    // Assert we returned 2 peers (1 primary spawned + 1 other worker spawned)
    assert_eq!(2, resp.len());

    // Assert network connectivity metrics are also set as expected
    let mut m = std::collections::HashMap::new();
    m.insert("peer_id", resp.get(0).unwrap().as_str());
    assert_eq!(
        1,
        metrics_2
            .clone()
            .network_connection_metrics
            .unwrap()
            .network_peer_connected
            .get_metric_with(&m)
            .unwrap()
            .get()
    );

    m.insert("peer_id", resp.get(1).unwrap().as_str());
    assert_eq!(
        1,
        metrics_2
            .network_connection_metrics
            .unwrap()
            .network_peer_connected
            .get_metric_with(&m)
            .unwrap()
            .get()
    );
}
