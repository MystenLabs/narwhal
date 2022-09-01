// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use arc_swap::ArcSwap;
use fastcrypto::traits::KeyPair;
use prometheus::Registry;
use test_utils::{
    batch, batch_digest, batches, committee, keys, open_batch_store, pure_committee_from_keys,
    resolve_name_committee_and_worker_cache, serialize_batch_message,
    shared_worker_cache_from_keys, PublicToWorkerMockServer,
};
use tokio::time::timeout;
use types::serialized_batch_digest;

#[tokio::test]
async fn synchronize() {
    let (tx_message, rx_message) = test_utils::test_channel!(1);
    let (tx_primary, _) = test_utils::test_channel!(1);

    let mut keys = keys(None);
    let committee = pure_committee_from_keys(&keys);
    let worker_cache = shared_worker_cache_from_keys(&keys);
    let name = keys.pop().unwrap().public().clone();
    let id = 0;

    let (tx_reconfiguration, _rx_reconfiguration) =
        watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));

    // Create a new test store.
    let store = open_batch_store();

    let metrics = Arc::new(WorkerMetrics::new(&Registry::new()));

    // Spawn a `Synchronizer` instance.
    let _synchronizer_handle = Synchronizer::spawn(
        name.clone(),
        id,
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        store.clone(),
        /* gc_depth */ 50, // Not used in this test.
        /* sync_retry_delay */
        Duration::from_millis(1_000_000), // Ensure it is not triggered.
        /* sync_retry_nodes */ 3, // Not used in this test.
        rx_message,
        tx_reconfiguration,
        tx_primary,
        metrics,
        WorkerNetwork::default(),
    );

    // Spawn a listener to receive our batch requests.
    let target = keys.pop().unwrap().public().clone();
    let address = worker_cache
        .load()
        .worker(&target, &id)
        .unwrap()
        .public_to_worker;
    let missing = vec![batch_digest()];
    let message = WorkerMessage::BatchRequest(missing.clone(), name.clone());
    let serialized = bincode::serialize(&message).unwrap();
    let mut handle = PublicToWorkerMockServer::spawn(address);

    // Send a sync request.
    let message = PrimaryWorkerMessage::Synchronize(missing, target);
    tx_message.send(message).await.unwrap();

    // Ensure the target receives the sync request.
    assert_eq!(handle.recv().await.unwrap().payload, serialized);
}

#[tokio::test]
async fn synchronize_when_batch_exists() {
    let (tx_message, rx_message) = test_utils::test_channel!(1);
    let (tx_primary, mut rx_primary) = test_utils::test_channel!(1);

    let mut keys = keys(None);
    let worker_cache = shared_worker_cache_from_keys(&keys);
    let name = keys.pop().unwrap().public().clone();
    let id = 0;

    let committee = committee(None);
    let (tx_reconfiguration, _rx_reconfiguration) =
        watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));

    // Create a new test store.
    let store = open_batch_store();

    let metrics = Arc::new(WorkerMetrics::new(&Registry::new()));

    // Spawn a `Synchronizer` instance.
    let _synchronizer_handle = Synchronizer::spawn(
        name.clone(),
        id,
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache.clone(),
        store.clone(),
        /* gc_depth */ 50, // Not used in this test.
        /* sync_retry_delay */
        Duration::from_millis(1_000_000), // Ensure it is not triggered.
        /* sync_retry_nodes */ 3, // Not used in this test.
        rx_message,
        tx_reconfiguration,
        tx_primary,
        metrics,
        WorkerNetwork::default(),
    );

    // Spawn a listener to receive our batch requests.
    let target = keys.pop().unwrap().public().clone();
    let address = worker_cache
        .load()
        .worker(&target, &id)
        .unwrap()
        .public_to_worker;
    let batch_id = batch_digest();
    let missing = vec![batch_id];

    // now store the batch
    let payload = vec![10u8];
    store.write(batch_id, payload).await;

    let mut handle = PublicToWorkerMockServer::spawn(address);

    // Send a sync request.
    let message = PrimaryWorkerMessage::Synchronize(missing, target);
    tx_message.send(message).await.unwrap();

    // Ensure the target does NOT receive the sync request - we practically timeout waiting.
    let result = timeout(Duration::from_secs(1), handle.recv()).await;
    assert!(result.is_err());

    // Now ensure that the batch is forwarded directly to primary
    let result_batch_message: WorkerPrimaryMessage = rx_primary.recv().await.unwrap();

    match result_batch_message {
        WorkerPrimaryMessage::OthersBatch(result_digest, worker_id) => {
            assert_eq!(result_digest, batch_id, "Batch id mismatch");
            assert_eq!(worker_id, id, "Worker id mismatch");
        }
        _ => panic!("Unexpected message received!"),
    }
}

#[tokio::test]
async fn test_successful_request_batch() {
    let (tx_message, rx_message) = test_utils::test_channel!(1);
    let (tx_primary, mut rx_primary) = test_utils::test_channel!(1);

    let (name, committee, worker_cache) = resolve_name_committee_and_worker_cache();
    let id = 0;

    let (tx_reconfiguration, _rx_reconfiguration) =
        watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));

    // Create a new test store.
    let store = open_batch_store();

    let metrics = Arc::new(WorkerMetrics::new(&Registry::new()));

    // Spawn a `Synchronizer` instance.
    let _synchronizer_handle = Synchronizer::spawn(
        name.clone(),
        id,
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache,
        store.clone(),
        /* gc_depth */ 50, // Not used in this test.
        /* sync_retry_delay */
        Duration::from_millis(1_000_000), // Ensure it is not triggered.
        /* sync_retry_nodes */ 3, // Not used in this test.
        rx_message,
        tx_reconfiguration,
        tx_primary,
        metrics,
        WorkerNetwork::default(),
    );

    // Create a dummy batch and store
    let expected_batch = batch();
    let batch_serialised = serialize_batch_message(expected_batch.clone());
    let expected_digest = serialized_batch_digest(&batch_serialised.clone()).unwrap();
    store.write(expected_digest, batch_serialised.clone()).await;

    // WHEN we send a message to retrieve the batch
    let message = PrimaryWorkerMessage::RequestBatch(expected_digest);

    tx_message
        .send(message)
        .await
        .expect("Should be able to send message");

    // THEN we should receive batch the batch
    if let Ok(Some(message)) = timeout(Duration::from_secs(5), rx_primary.recv()).await {
        match message {
            WorkerPrimaryMessage::RequestedBatch(digest, batch) => {
                assert_eq!(batch, expected_batch);
                assert_eq!(digest, expected_digest)
            }
            _ => panic!("Unexpected message"),
        }
    } else {
        panic!("Expected to successfully received a request batch");
    }
}

#[tokio::test]
async fn test_request_batch_not_found() {
    let (tx_message, rx_message) = test_utils::test_channel!(1);
    let (tx_primary, mut rx_primary) = test_utils::test_channel!(1);

    let (name, committee, worker_cache) = resolve_name_committee_and_worker_cache();
    let id = 0;

    let (tx_reconfiguration, _rx_reconfiguration) =
        watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));

    // Create a new test store.
    let store = open_batch_store();

    let metrics = Arc::new(WorkerMetrics::new(&Registry::new()));

    // Spawn a `Synchronizer` instance.
    let _synchronizer_handle = Synchronizer::spawn(
        name.clone(),
        id,
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache,
        store.clone(),
        /* gc_depth */ 50, // Not used in this test.
        /* sync_retry_delay */
        Duration::from_millis(1_000_000), // Ensure it is not triggered.
        /* sync_retry_nodes */ 3, // Not used in this test.
        rx_message,
        tx_reconfiguration,
        tx_primary,
        metrics,
        WorkerNetwork::default(),
    );

    // The non existing batch id
    let expected_batch_id = BatchDigest::default();

    // WHEN we send a message to retrieve the batch that doesn't exist
    let message = PrimaryWorkerMessage::RequestBatch(expected_batch_id);

    tx_message
        .send(message)
        .await
        .expect("Should be able to send message");

    // THEN we should receive batch the batch
    if let Ok(Some(message)) = timeout(Duration::from_secs(5), rx_primary.recv()).await {
        match message {
            WorkerPrimaryMessage::Error(error) => {
                assert_eq!(
                    error,
                    WorkerPrimaryError::RequestedBatchNotFound(expected_batch_id)
                );
            }
            _ => panic!("Unexpected message"),
        }
    } else {
        panic!("Expected to successfully received a request batch");
    }
}

#[tokio::test]
async fn test_successful_batch_delete() {
    let (tx_message, rx_message) = test_utils::test_channel!(1);
    let (tx_primary, mut rx_primary) = test_utils::test_channel!(1);

    let (name, committee, worker_cache) = resolve_name_committee_and_worker_cache();
    let id = 0;

    let (tx_reconfiguration, _rx_reconfiguration) =
        watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));

    // Create a new test store.
    let store = open_batch_store();

    let metrics = Arc::new(WorkerMetrics::new(&Registry::new()));

    // Spawn a `Synchronizer` instance.
    let _synchronizer_handle = Synchronizer::spawn(
        name.clone(),
        id,
        Arc::new(ArcSwap::from_pointee(committee.clone())),
        worker_cache,
        store.clone(),
        /* gc_depth */ 50, // Not used in this test.
        /* sync_retry_delay */
        Duration::from_millis(1_000_000), // Ensure it is not triggered.
        /* sync_retry_nodes */ 3, // Not used in this test.
        rx_message,
        tx_reconfiguration,
        tx_primary,
        metrics,
        WorkerNetwork::default(),
    );

    // Create dummy batches and store them
    let expected_batches = batches(10);
    let mut batch_digests = Vec::new();

    for batch in expected_batches.clone() {
        let s = serialize_batch_message(batch);
        let digest = serialized_batch_digest(&s.clone()).unwrap();

        batch_digests.push(digest);

        store.write(digest, s.clone()).await;
    }

    // WHEN we send a message to delete batches
    let message = PrimaryWorkerMessage::DeleteBatches(batch_digests.clone());

    tx_message
        .send(message)
        .await
        .expect("Should be able to send message");

    // THEN we should receive the acknowledgement that the batches have been deleted
    if let Ok(Some(message)) = timeout(Duration::from_secs(5), rx_primary.recv()).await {
        match message {
            WorkerPrimaryMessage::DeletedBatches(digests) => {
                assert_eq!(digests, batch_digests);
            }
            _ => panic!("Unexpected message"),
        }
    } else {
        panic!("Expected to successfully receive a deleted batches request");
    }

    // AND batches should be deleted
    for batch in expected_batches {
        let s = serialize_batch_message(batch);
        let digest = serialized_batch_digest(&s.clone()).unwrap();

        let result = store.read(digest).await;
        assert!(result.as_ref().is_ok());
        assert!(result.unwrap().is_none());
    }
}
