// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use test_utils::{committee, keys, transaction};
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn make_batch() {
    let name = keys(None).pop().unwrap().public().clone();
    let committee = (&*committee(None)).clone();
    let (_tx_reconfiguration, rx_reconfiguration) =
        watch::channel(Reconfigure::NewCommittee(committee.clone()));
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        name,
        /* worker_id */ 0,
        committee,
        /* max_batch_size */ 200,
        /* max_batch_delay */
        Duration::from_millis(1_000_000), // Ensure the timer is not triggered.
        rx_reconfiguration,
        rx_transaction,
        tx_message,
    );

    // Send enough transactions to seal a batch.
    let tx = transaction();
    tx_transaction.send(tx.clone()).await.unwrap();
    tx_transaction.send(tx.clone()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = Batch(vec![tx.clone(), tx.clone()]);
    let QuorumWaiterMessage { batch, handlers: _ } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::<Ed25519PublicKey>::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}

#[tokio::test]
async fn batch_timeout() {
    let name = keys(None).pop().unwrap().public().clone();
    let committee = (&*committee(None)).clone();
    let (_tx_reconfiguration, rx_reconfiguration) =
        watch::channel(Reconfigure::NewCommittee(committee.clone()));
    let (tx_transaction, rx_transaction) = channel(1);
    let (tx_message, mut rx_message) = channel(1);

    // Spawn a `BatchMaker` instance.
    BatchMaker::spawn(
        name,
        /* worker_id */ 0,
        committee,
        /* max_batch_size */ 200,
        /* max_batch_delay */
        Duration::from_millis(50), // Ensure the timer is triggered.
        rx_reconfiguration,
        rx_transaction,
        tx_message,
    );

    // Do not send enough transactions to seal a batch..
    let tx = transaction();
    tx_transaction.send(tx.clone()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = Batch(vec![tx]);
    let QuorumWaiterMessage { batch, handlers: _ } = rx_message.recv().await.unwrap();
    match bincode::deserialize(&batch).unwrap() {
        WorkerMessage::<Ed25519PublicKey>::Batch(batch) => assert_eq!(batch, expected_batch),
        _ => panic!("Unexpected message"),
    }
}
