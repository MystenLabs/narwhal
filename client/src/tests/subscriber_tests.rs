// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{common::TestExecutionState, sequencer::MockSequencer};
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
        store.clone(),
        execution_state,
        tx_batch_loader,
    )
    .await
    .unwrap();
    Subscriber::spawn(subscriber);
    tokio::task::yield_now().await;

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
        store.clone(),
        execution_state,
        tx_batch_loader,
    )
    .await
    .unwrap();
    Subscriber::spawn(subscriber);
    tokio::task::yield_now().await;

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
