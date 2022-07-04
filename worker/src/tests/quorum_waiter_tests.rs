// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::worker::WorkerMessage;
use bytes::Bytes;
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use network::WorkerNetwork;
use test_utils::{batch, committee, keys, WorkerToWorkerMockServer};
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn wait_for_quorum() {
    let (tx_message, rx_message) = channel(1);
    let (tx_batch, mut rx_batch) = channel(1);
    let myself = keys(None).pop().unwrap().public().clone();
    let committee = committee(None);

    // Spawn a `QuorumWaiter` instance.
    QuorumWaiter::spawn(
        committee.clone(),
        /* stake */ 1,
        rx_message,
        tx_batch,
        /* sync_delay */ Duration::from_secs(100_000), // Never triggered.
    );

    // Make a batch.
    let message = WorkerMessage::<Ed25519PublicKey>::Batch(batch());
    let serialized = bincode::serialize(&message).unwrap();
    let expected = Bytes::from(serialized.clone());

    // Spawn enough listeners to acknowledge our batches.
    let mut names = Vec::new();
    let mut addresses = Vec::new();
    let mut listener_handles = Vec::new();
    for (name, address) in committee.others_workers(&myself, /* id */ &0) {
        let address = address.worker_to_worker;
        let handle = WorkerToWorkerMockServer::spawn(address.clone());
        names.push(name);
        addresses.push(address);
        listener_handles.push(handle);
    }

    // Broadcast the batch through the network.
    let handlers = WorkerNetwork::default()
        .broadcast(addresses, &message)
        .await;

    // Forward the batch along with the handlers to the `QuorumWaiter`.
    let message = QuorumWaiterMessage {
        batch: serialized.clone(),
        handlers: names.into_iter().zip(handlers.into_iter()).collect(),
    };
    tx_message.send(message).await.unwrap();

    // Wait for the `QuorumWaiter` to gather enough acknowledgements and output the batch.
    let output = rx_batch.recv().await.unwrap();
    assert_eq!(output, serialized);

    // Ensure the other listeners correctly received the batch.
    for mut handle in listener_handles {
        assert_eq!(handle.recv().await.unwrap().payload, expected);
    }
}
