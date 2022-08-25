// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::{NetworkModel, Primary, CHANNEL_CAPACITY};
use arc_swap::ArcSwap;
use config::Parameters;
use consensus::{dag::Dag, metrics::ConsensusMetrics};
use fastcrypto::traits::KeyPair;
use node::NodeStorage;
use prometheus::Registry;
use std::sync::Arc;
use test_utils::{keys, pure_committee_from_keys, shared_worker_cache_from_keys, temp_dir};
use tokio::sync::watch;
use types::{Empty, PublicToPrimaryClient, ReconfigureNotification, WorkerInfoResponse};

#[tokio::test]
async fn handle_worker_info_request() {
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };
    let mut k = keys(None);
    let committee = pure_committee_from_keys(&k);
    let worker_cache = shared_worker_cache_from_keys(&k);
    let keypair = k.pop().unwrap();
    let name = keypair.public().clone();
    let signer = keypair;

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
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        parameters.clone(),
        store.header_store.clone(),
        store.certificate_store.clone(),
        store.payload_store.clone(),
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

    // Spawn a client to ask for worker info and receive the reply.
    tokio::task::yield_now().await;
    let address = committee.primary(&name).unwrap().primary_to_primary.clone();
    let config = mysten_network::config::Config::new();
    let channel = config.connect_lazy(&address).unwrap();
    let mut client = PublicToPrimaryClient::new(channel);

    // Send worker info request.
    let response: WorkerInfoResponse = client
        .worker_info(Empty {})
        .await
        .unwrap()
        .into_inner()
        .deserialize()
        .unwrap();

    // Wait for the reply and ensure it is as expected.
    assert_eq!(
        response.workers,
        worker_cache.load().workers.get(&name).unwrap().0
    );
}
