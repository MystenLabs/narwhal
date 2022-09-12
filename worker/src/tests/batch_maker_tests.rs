// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use prometheus::Registry;
use test_utils::{transaction, CommitteeFixture};

#[tokio::test]
async fn make_batch() {
    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();
    let (_tx_reconfiguration, rx_reconfiguration) =
        watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));
    let (tx_transaction, rx_transaction) = test_utils::test_channel!(1);
    let (tx_message, mut rx_message) = test_utils::test_channel!(1);
    let node_metrics = WorkerMetrics::new(&Registry::new());

    // Spawn a `BatchMaker` instance.
    let _batch_maker_handle = BatchMaker::spawn(
        committee,
        /* max_batch_size */ 200,
        /* max_batch_delay */
        Duration::from_millis(1_000_000), // Ensure the timer is not triggered.
        rx_reconfiguration,
        rx_transaction,
        tx_message,
        Arc::new(node_metrics),
    );

    // Send enough transactions to seal a batch.
    let tx = transaction();
    tx_transaction.send(tx.clone()).await.unwrap();
    tx_transaction.send(tx.clone()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = Batch(vec![tx.clone(), tx.clone()]);
    let batch = rx_message.recv().await.unwrap();
    assert_eq!(batch, expected_batch);
}

#[tokio::test]
async fn batch_timeout() {
    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();
    let (_tx_reconfiguration, rx_reconfiguration) =
        watch::channel(ReconfigureNotification::NewEpoch(committee.clone()));
    let (tx_transaction, rx_transaction) = test_utils::test_channel!(1);
    let (tx_message, mut rx_message) = test_utils::test_channel!(1);
    let node_metrics = WorkerMetrics::new(&Registry::new());

    // Spawn a `BatchMaker` instance.
    let _batch_maker_handle = BatchMaker::spawn(
        committee,
        /* max_batch_size */ 200,
        /* max_batch_delay */
        Duration::from_millis(50), // Ensure the timer is triggered.
        rx_reconfiguration,
        rx_transaction,
        tx_message,
        Arc::new(node_metrics),
    );

    // Do not send enough transactions to seal a batch.
    let tx = transaction();
    tx_transaction.send(tx.clone()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = Batch(vec![tx]);
    let batch = rx_message.recv().await.unwrap();
    assert_eq!(batch, expected_batch);
}
