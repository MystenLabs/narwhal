// Copyright(C) Facebook, Inc. and its affiliates.
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crypto::traits::KeyPair;
use prometheus::Registry;
use test_utils::{committee, keys};
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn propose_payload() {
    let kp = keys(None).pop().unwrap();
    let name = kp.public().clone();
    let signature_service = SignatureService::new(kp);

    let (_tx_reconfigure, rx_reconfigure) =
        watch::channel(ReconfigureNotification::NewEpoch(committee(None)));
    let (_tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));

    // Spawn the proposer.
    let _proposer_handle = Proposer::spawn(
        name.clone(),
        committee(None),
        signature_service,
        /* header_size */ 32,
        /* header_delay */
        Duration::from_secs(1_000), // Ensure it is not triggered.
        NetworkModel::PartiallySynchronous,
        rx_reconfigure,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
        metrics,
    );

    // Send enough digests for the header payload.
    let name_bytes: [u8; 32] = *name.0.as_bytes();
    let digest = BatchDigest(name_bytes);
    let worker_id = 0;
    tx_our_digests.send((digest, worker_id)).await.unwrap();

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee(None)).is_ok());
}

#[tokio::test]
async fn propose_empty() {
    let kp = keys(None).pop().unwrap();
    let name = kp.public().clone();
    let signature_service = SignatureService::new(kp);

    let header_delay = Duration::from_millis(5);

    let (_tx_reconfigure, rx_reconfigure) =
        watch::channel(ReconfigureNotification::NewEpoch(committee(None)));
    let (_tx_parents, rx_parents) = channel(1);
    let (tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));

    // Spawn the proposer.
    let _proposer_handle = Proposer::spawn(
        name.clone(),
        committee(None),
        signature_service,
        /* header_size */ 32,
        header_delay,
        NetworkModel::PartiallySynchronous,
        rx_reconfigure,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
        metrics,
    );

    // Ensure the proposer does not make an empty proposal
    tokio::select! {
        _result = rx_headers.recv() => panic!("Unexpected header proposal"),
        () = sleep(header_delay * 10) => ()
    }

    // Now send enough digests for the header payload.
    let name_bytes: [u8; 32] = *name.0.as_bytes();
    let digest = BatchDigest(name_bytes);
    let worker_id = 0;
    tx_our_digests.send((digest, worker_id)).await.unwrap();

    // Ensure the proposer makes a valid proposal
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee(None)).is_ok());
}

#[tokio::test]
async fn advance_to_help() {
    let kp = keys(None).pop().unwrap();
    let name = kp.public().clone();
    let signature_service = SignatureService::new(kp);

    let (_tx_reconfigure, rx_reconfigure) =
        watch::channel(ReconfigureNotification::NewEpoch(committee(None)));
    let (tx_parents, rx_parents) = channel(1);
    let (_tx_our_digests, rx_our_digests) = channel(1);
    let (tx_headers, mut rx_headers) = channel(1);

    let metrics = Arc::new(PrimaryMetrics::new(&Registry::new()));

    // Spawn the proposer.
    let _proposer_handle = Proposer::spawn(
        name.clone(),
        committee(None),
        signature_service,
        /* header_size */ 1_000,
        /* header_delay */
        Duration::from_secs(1_000), // Ensure it is not triggered.
        NetworkModel::PartiallySynchronous,
        rx_reconfigure,
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        /* tx_core */ tx_headers,
        metrics,
    );

    // Other primaries have enough batches to make progress, we should then create a empty
    // header to help them out.
    tx_parents
        .send(ProposerMessage::MeaningfulRound(1))
        .await
        .unwrap();

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round, 1);
    assert!(header.payload.is_empty());
    // assert_eq!(header.payload.get(&digest), Some(&worker_id));
    assert!(header.verify(&committee(None)).is_ok());
}
