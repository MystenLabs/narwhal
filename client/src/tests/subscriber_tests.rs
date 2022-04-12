// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    execution_state::{TestExecutionState, KILLER_TRANSACTION, MALFORMED_TRANSACTION},
    fixtures::{test_batch, test_certificate},
    sequencer::MockSequencer,
};
use crypto::ed25519::Ed25519PublicKey;
use primary::Certificate;
use std::{path::Path, sync::Arc};
use store::{
    reopen,
    rocks::{open_cf, DBMap},
};
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn handle_consensus_message() {
    let node_address = "127.0.0.1:13000".parse().unwrap();
    let (tx_sequence, rx_sequence) = channel(10);
    let (tx_batch_loader, mut rx_batch_loader) = channel(10);

    // Spawn a mock consensus
    MockSequencer::spawn(node_address, rx_sequence);
    tokio::task::yield_now().await;

    // Spawn a subscriber.
    let store_path = ".test_storage_handle_consensus_message";
    let _ = std::fs::remove_dir_all(store_path);
    const BATCHES_CF: &str = "batches";
    let rocksdb = open_cf(Path::new(store_path), None, &[BATCHES_CF]).unwrap();
    let batch_map = reopen!(&rocksdb, BATCHES_CF;<BatchDigest, SerializedBatchMessage>);
    let store = Store::new(batch_map);

    let execution_state = Arc::new(TestExecutionState::default());
    let subscriber = Subscriber::<TestExecutionState, Ed25519PublicKey>::new(
        node_address,
        store,
        execution_state,
        tx_batch_loader,
    )
    .await
    .unwrap();
    Subscriber::spawn(subscriber);

    // Feed certificates to the mock sequencer and ensure the batch loader receive the command to
    // download the corresponding transaction data.
    for _ in 0..2 {
        tx_sequence.send(Certificate::default()).await.unwrap();
    }
    for i in 0..2 {
        let output = rx_batch_loader.recv().await.unwrap();
        assert_eq!(output.consensus_index, i);
    }

    // Delete the test storage.
    std::fs::remove_dir_all(store_path).unwrap();
}

#[tokio::test]
async fn synchronize() {
    let node_address = "127.0.0.1:13001".parse().unwrap();
    let (tx_sequence, rx_sequence) = channel(10);
    let (tx_batch_loader, mut rx_batch_loader) = channel(10);

    // Spawn a mock consensus
    MockSequencer::spawn(node_address, rx_sequence);
    tokio::task::yield_now().await;

    // Send two certificates.
    for _ in 0..2 {
        tx_sequence.send(Certificate::default()).await.unwrap();
    }

    // Spawn a subscriber.
    let store_path = ".test_storage_synchronize";
    let _ = std::fs::remove_dir_all(store_path);
    const BATCHES_CF: &str = "batches";
    let rocksdb = open_cf(Path::new(store_path), None, &[BATCHES_CF]).unwrap();
    let batch_map = reopen!(&rocksdb, BATCHES_CF;<BatchDigest, SerializedBatchMessage>);
    let store = Store::new(batch_map);

    let execution_state = Arc::new(TestExecutionState::default());
    let subscriber = Subscriber::<TestExecutionState, Ed25519PublicKey>::new(
        node_address,
        store,
        execution_state,
        tx_batch_loader,
    )
    .await
    .unwrap();
    Subscriber::spawn(subscriber);

    // Send two extra certificates. The client needs to sync for the first two certificates.
    for _ in 0..2 {
        tx_sequence.send(Certificate::default()).await.unwrap();
    }

    // Ensure the client synchronizes the first twi certificates.
    for i in 0..4 {
        let output = rx_batch_loader.recv().await.unwrap();
        assert_eq!(output.consensus_index, i);
    }

    // Delete the test storage.
    std::fs::remove_dir_all(store_path).unwrap();
}

#[tokio::test]
async fn execute_transactions() {
    let node_address = "127.0.0.1:13002".parse().unwrap();
    let (tx_sequence, rx_sequence) = channel(10);
    let (tx_batch_loader, mut rx_batch_loader) = channel(10);

    // Spawn a mock consensus
    MockSequencer::spawn(node_address, rx_sequence);
    tokio::task::yield_now().await;

    // Spawn a subscriber.
    let store_path = ".test_storage_execute_transactions";
    let _ = std::fs::remove_dir_all(store_path);
    const BATCHES_CF: &str = "batches";
    let rocksdb = open_cf(Path::new(store_path), None, &[BATCHES_CF]).unwrap();
    let batch_map = reopen!(&rocksdb, BATCHES_CF;<BatchDigest, SerializedBatchMessage>);
    let store = Store::new(batch_map);

    let execution_state = Arc::new(TestExecutionState::default());
    let subscriber = Subscriber::<TestExecutionState, Ed25519PublicKey>::new(
        node_address,
        store.clone(),
        execution_state.clone(),
        tx_batch_loader,
    )
    .await
    .unwrap();
    Subscriber::spawn(subscriber);

    // Feed certificates to the mock sequencer and add the transaction data to storage (as if
    // the batch loader downloaded them).
    for i in 0..2 {
        let tx00 = i as u64;
        let tx01 = (i + 1000) as u64;
        let tx10 = (i + 2000) as u64;
        let tx11 = (i + 3000) as u64;

        let (digest_0, batch_0) = test_batch(vec![tx00, tx01]);
        let (digest_1, batch_1) = test_batch(vec![tx10, tx11]);

        store.write(digest_0, batch_0).await;
        store.write(digest_1, batch_1).await;

        let payload = [(digest_0, 0), (digest_1, 1)].iter().cloned().collect();
        let certificate = test_certificate(payload);

        tx_sequence.send(certificate).await.unwrap();
        let _ = rx_batch_loader.recv().await.unwrap();
    }

    // Ensure the execution state is updated accordingly.
    let expected = SubscriberState {
        next_certificate_index: 2,
        next_batch_index: 0,
        next_transaction_index: 0,
    };
    assert_eq!(execution_state.get_subscriber_state().await, expected);

    // Delete the test storage.
    std::fs::remove_dir_all(store_path).unwrap();
}

#[tokio::test]
async fn execute_empty_certificate() {
    let node_address = "127.0.0.1:13003".parse().unwrap();
    let (tx_sequence, rx_sequence) = channel(10);
    let (tx_batch_loader, mut rx_batch_loader) = channel(10);

    // Spawn a mock consensus
    MockSequencer::spawn(node_address, rx_sequence);
    tokio::task::yield_now().await;

    // Spawn a subscriber.
    let store_path = ".test_storage_execute_empty_certificate";
    let _ = std::fs::remove_dir_all(store_path);
    const BATCHES_CF: &str = "batches";
    let rocksdb = open_cf(Path::new(store_path), None, &[BATCHES_CF]).unwrap();
    let batch_map = reopen!(&rocksdb, BATCHES_CF;<BatchDigest, SerializedBatchMessage>);
    let store = Store::new(batch_map);

    let execution_state = Arc::new(TestExecutionState::default());
    let subscriber = Subscriber::<TestExecutionState, Ed25519PublicKey>::new(
        node_address,
        store.clone(),
        execution_state.clone(),
        tx_batch_loader,
    )
    .await
    .unwrap();
    Subscriber::spawn(subscriber);

    // Feed empty certificates to the mock sequencer.
    for _ in 0..2 {
        tx_sequence.send(Certificate::default()).await.unwrap();
        let _ = rx_batch_loader.recv().await.unwrap();
    }

    // Then feed one non-empty certificate and ensure the certificate index is updated.
    let (digest, batch) = test_batch(vec![10u64, 11u64]);
    store.write(digest, batch).await;
    let payload = [(digest, 0)].iter().cloned().collect();
    let certificate = test_certificate(payload);
    tx_sequence.send(certificate).await.unwrap();
    let _ = rx_batch_loader.recv().await.unwrap();

    let expected = SubscriberState {
        next_certificate_index: 3,
        next_batch_index: 0,
        next_transaction_index: 0,
    };
    assert_eq!(execution_state.get_subscriber_state().await, expected);

    // Delete the test storage.
    std::fs::remove_dir_all(store_path).unwrap();
}

#[tokio::test]
async fn execute_malformed_transactions() {
    let node_address = "127.0.0.1:13004".parse().unwrap();
    let (tx_sequence, rx_sequence) = channel(10);
    let (tx_batch_loader, mut rx_batch_loader) = channel(10);

    // Spawn a mock consensus
    MockSequencer::spawn(node_address, rx_sequence);
    tokio::task::yield_now().await;

    // Spawn a subscriber.
    let store_path = ".test_storage_execute_malformed_transactions";
    let _ = std::fs::remove_dir_all(store_path);
    const BATCHES_CF: &str = "batches";
    let rocksdb = open_cf(Path::new(store_path), None, &[BATCHES_CF]).unwrap();
    let batch_map = reopen!(&rocksdb, BATCHES_CF;<BatchDigest, SerializedBatchMessage>);
    let store = Store::new(batch_map);

    let execution_state = Arc::new(TestExecutionState::default());
    let subscriber = Subscriber::<TestExecutionState, Ed25519PublicKey>::new(
        node_address,
        store.clone(),
        execution_state.clone(),
        tx_batch_loader,
    )
    .await
    .unwrap();
    Subscriber::spawn(subscriber);

    // Feed a bad transaction to the mock sequencer
    let tx0 = MALFORMED_TRANSACTION;
    let tx1 = 10;
    let (digest, batch) = test_batch(vec![tx0, tx1]);

    store.write(digest, batch).await;

    let payload = [(digest, 0)].iter().cloned().collect();
    let certificate = test_certificate(payload);

    tx_sequence.send(certificate).await.unwrap();
    let _ = rx_batch_loader.recv().await.unwrap();

    // Feed two certificates with good transactions to the mock sequencer.
    for i in 0..2 {
        let tx00 = i as u64;
        let tx01 = (i + 1000) as u64;
        let tx10 = (i + 2000) as u64;
        let tx11 = (i + 3000) as u64;

        let (digest_0, batch_0) = test_batch(vec![tx00, tx01]);
        let (digest_1, batch_1) = test_batch(vec![tx10, tx11]);

        store.write(digest_0, batch_0).await;
        store.write(digest_1, batch_1).await;

        let payload = [(digest_0, 0), (digest_1, 1)].iter().cloned().collect();
        let certificate = test_certificate(payload);

        tx_sequence.send(certificate).await.unwrap();
        let _ = rx_batch_loader.recv().await.unwrap();
    }

    // Ensure the execution state is updated accordingly.
    let expected = SubscriberState {
        next_certificate_index: 3,
        next_batch_index: 0,
        next_transaction_index: 0,
    };
    assert_eq!(execution_state.get_subscriber_state().await, expected);

    // Delete the test storage.
    std::fs::remove_dir_all(store_path).unwrap();
}

#[tokio::test]
async fn internal_error_execution() {
    let node_address = "127.0.0.1:13005".parse().unwrap();
    let (tx_sequence, rx_sequence) = channel(10);
    let (tx_batch_loader, mut rx_batch_loader) = channel(10);

    // Spawn a mock consensus
    MockSequencer::spawn(node_address, rx_sequence);
    tokio::task::yield_now().await;

    // Spawn a subscriber.
    let store_path = ".test_storage_internal_error_execution";
    let _ = std::fs::remove_dir_all(store_path);
    const BATCHES_CF: &str = "batches";
    let rocksdb = open_cf(Path::new(store_path), None, &[BATCHES_CF]).unwrap();
    let batch_map = reopen!(&rocksdb, BATCHES_CF;<BatchDigest, SerializedBatchMessage>);
    let store = Store::new(batch_map);

    let execution_state = Arc::new(TestExecutionState::default());
    let subscriber = Subscriber::<TestExecutionState, Ed25519PublicKey>::new(
        node_address,
        store.clone(),
        execution_state.clone(),
        tx_batch_loader,
    )
    .await
    .unwrap();
    Subscriber::spawn(subscriber);

    // Feed a killer transaction to the mock sequencer
    let tx0 = KILLER_TRANSACTION;
    let tx1 = 10;
    let (digest, batch) = test_batch(vec![tx0, tx1]);

    store.write(digest, batch).await;

    let payload = [(digest, 0)].iter().cloned().collect();
    let certificate = test_certificate(payload);

    tx_sequence.send(certificate).await.unwrap();
    let _ = rx_batch_loader.recv().await.unwrap();

    // Ensure the execution state does not change.
    let expected = SubscriberState::default();
    assert_eq!(execution_state.get_subscriber_state().await, expected);

    // Delete the test storage.
    std::fs::remove_dir_all(store_path).unwrap();
}
