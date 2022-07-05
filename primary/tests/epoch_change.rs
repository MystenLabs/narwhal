// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use arc_swap::ArcSwap;
use config::{Committee, Epoch, Parameters};
use crypto::traits::KeyPair;
use futures::future::join_all;
use node::NodeStorage;
use primary::{NetworkModel, Primary, CHANNEL_CAPACITY};
use prometheus::default_registry;
use std::{collections::BTreeMap, sync::Arc};
use test_utils::{keys, make_authority, pure_committee_from_keys, temp_dir};
use tokio::sync::mpsc::channel;
use types::ConsensusPrimaryMessage;

/// The epoch changes but the stake distribution and network addresses stay the same.
#[tokio::test]
async fn simple_epoch_change() {
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // The configuration of epoch 0.
    let keys_0 = keys(None);
    let committee_0 = pure_committee_from_keys(&keys_0);

    // Spawn the committee of epoch 0.
    let mut rx_channels = Vec::new();
    let mut tx_channels = Vec::new();
    for keypair in keys_0 {
        let name = keypair.public().clone();
        let signer = keypair;

        let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
        rx_channels.push(rx_new_certificates);
        let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
        tx_channels.push(tx_feedback.clone());

        let store = NodeStorage::reopen(temp_dir());

        Primary::spawn(
            name,
            signer,
            Arc::new(committee_0.clone()),
            parameters.clone(),
            store.header_store.clone(),
            store.certificate_store.clone(),
            store.payload_store.clone(),
            /* tx_consensus */ tx_new_certificates,
            /* rx_consensus */ rx_feedback,
            /* dag */ None,
            NetworkModel::Asynchronous,
            /* tx_committed_certificates */ tx_feedback,
            default_registry(),
        );
    }

    // Run for a while in epoch 0.
    for rx in rx_channels.iter_mut() {
        loop {
            let certificate = rx.recv().await.unwrap();
            assert_eq!(certificate.epoch(), 0);
            if certificate.round() == 10 {
                break;
            }
        }
    }

    // Move to the next epochs.
    for epoch in 1..=3 {
        // Move to the next epoch.
        let new_committee = committee_0.clone();
        new_committee.epoch.swap(Arc::new(epoch));
        for tx in &tx_channels {
            tx.send(ConsensusPrimaryMessage::Committee(new_committee.clone()))
                .await
                .unwrap();
        }

        // Run for a while.
        for rx in rx_channels.iter_mut() {
            loop {
                let certificate = rx.recv().await.unwrap();
                if certificate.epoch() == epoch && certificate.round() == 10 {
                    break;
                }
            }
        }
    }
}

#[tokio::test]
async fn partial_committee_change() {
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Make the committee of epoch 0.
    let keys_0 = keys(None);
    let authorities_0: Vec<_> = keys_0.iter().map(|_| make_authority()).collect();
    let committee_0 = Committee {
        epoch: ArcSwap::new(Arc::new(Epoch::default())),
        authorities: ArcSwap::from_pointee(
            keys_0
                .iter()
                .zip(authorities_0.clone().into_iter())
                .map(|(kp, authority)| (kp.public().clone(), authority))
                .collect(),
        ),
    };

    // Spawn the committee of epoch 0.
    let mut epoch_0_rx_channels = Vec::new();
    let mut epoch_0_tx_channels = Vec::new();
    for keypair in keys_0 {
        let name = keypair.public().clone();
        let signer = keypair;

        let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
        epoch_0_rx_channels.push(rx_new_certificates);
        let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
        epoch_0_tx_channels.push(tx_feedback.clone());

        let store = NodeStorage::reopen(temp_dir());

        Primary::spawn(
            name,
            signer,
            Arc::new(committee_0.clone()),
            parameters.clone(),
            store.header_store.clone(),
            store.certificate_store.clone(),
            store.payload_store.clone(),
            /* tx_consensus */ tx_new_certificates,
            /* rx_consensus */ rx_feedback,
            /* dag */ None,
            NetworkModel::Asynchronous,
            /* tx_committed_certificates */ tx_feedback,
            default_registry(),
        );
    }

    // Run for a while in epoch 0.
    for rx in epoch_0_rx_channels.iter_mut() {
        loop {
            let certificate = rx.recv().await.unwrap();
            assert_eq!(certificate.epoch(), 0);
            if certificate.round() == 10 {
                break;
            }
        }
    }

    // Make the committee of epoch 1.
    let mut to_spawn = Vec::new();

    let keys_0 = keys(None);
    let keys_1 = keys(Some(1));
    let mut total_stake = 0;
    let authorities_1: BTreeMap<_, _> = authorities_0
        .into_iter()
        .zip(keys_0.into_iter())
        .zip(keys_1.into_iter())
        .map(|((authority, key_0), key_1)| {
            let stake = authority.stake;
            let x = if total_stake < committee_0.validity_threshold() {
                let pk = key_0.public().clone();
                (pk, authority)
            } else {
                let new_authority = make_authority();
                let pk = key_1.public().clone();
                to_spawn.push(key_1);
                (pk, new_authority)
            };
            total_stake += stake;
            x
        })
        .collect();

    let committee_1 = Committee {
        epoch: ArcSwap::new(Arc::new(Epoch::default() + 1)),
        authorities: ArcSwap::from_pointee(authorities_1),
    };

    // Spawn the committee of epoch 1 (only the node not already booted).
    let mut epoch_1_rx_channels = Vec::new();
    let mut epoch_1_tx_channels = Vec::new();
    for keypair in to_spawn {
        let name = keypair.public().clone();
        let signer = keypair;

        let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
        epoch_1_rx_channels.push(rx_new_certificates);
        let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
        epoch_1_tx_channels.push(tx_feedback.clone());

        let store = NodeStorage::reopen(temp_dir());

        Primary::spawn(
            name,
            signer,
            Arc::new(committee_1.clone()),
            parameters.clone(),
            store.header_store.clone(),
            store.certificate_store.clone(),
            store.payload_store.clone(),
            /* tx_consensus */ tx_new_certificates,
            /* rx_consensus */ rx_feedback,
            /* dag */ None,
            NetworkModel::Asynchronous,
            /* tx_committed_certificates */ tx_feedback,
            default_registry(),
        );
    }

    // Tell the nodes of epoch 0 to transition to epoch 1.
    for tx in &epoch_0_tx_channels {
        tx.send(ConsensusPrimaryMessage::Committee(committee_1.clone()))
            .await
            .unwrap();
    }

    // Run for a while in epoch 1.
    for rx in epoch_1_rx_channels.iter_mut() {
        loop {
            let certificate = rx.recv().await.unwrap();
            if certificate.epoch() == 1 && certificate.round() == 10 {
                break;
            }
        }
    }
}

/// The epoch changes but the stake distribution and network addresses stay the same.
#[tokio::test]
async fn restart_with_new_committee() {
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // The configuration of epoch 0.
    let keys_0 = keys(None);
    let committee_0 = pure_committee_from_keys(&keys_0);

    // Spawn the committee of epoch 0.
    let mut rx_channels = Vec::new();
    let mut tx_channels = Vec::new();
    let mut handles = Vec::new();
    for keypair in keys_0 {
        let name = keypair.public().clone();
        let signer = keypair;

        let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
        rx_channels.push(rx_new_certificates);
        let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
        tx_channels.push(tx_feedback.clone());

        let store = NodeStorage::reopen(temp_dir());

        let primary_handles = Primary::spawn(
            name,
            signer,
            Arc::new(committee_0.clone()),
            parameters.clone(),
            store.header_store.clone(),
            store.certificate_store.clone(),
            store.payload_store.clone(),
            /* tx_consensus */ tx_new_certificates,
            /* rx_consensus */ rx_feedback,
            /* dag */ None,
            NetworkModel::Asynchronous,
            /* tx_committed_certificates */ tx_feedback,
            default_registry(),
        );
        handles.extend(primary_handles);
    }

    // Run for a while in epoch 0.
    for rx in rx_channels.iter_mut() {
        loop {
            let certificate = rx.recv().await.unwrap();
            assert_eq!(certificate.epoch(), 0);
            if certificate.round() == 10 {
                break;
            }
        }
    }

    // Shutdown the committee of the previous epoch;
    for tx in &tx_channels {
        let (token, mut rx) = channel(1);
        tx.send(ConsensusPrimaryMessage::Shutdown(token))
            .await
            .unwrap();
        rx.recv().await;
    }
    join_all(handles).await;

    // Move to the next epochs.
    for epoch in 1..=3 {
        let new_committee = committee_0.clone();
        new_committee.epoch.swap(Arc::new(epoch));

        let mut rx_channels = Vec::new();
        let mut tx_channels = Vec::new();
        let mut handles = Vec::new();
        for keypair in keys(None) {
            let name = keypair.public().clone();
            let signer = keypair;

            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
            rx_channels.push(rx_new_certificates);
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
            tx_channels.push(tx_feedback.clone());

            let store = NodeStorage::reopen(temp_dir());

            let primary_handles = Primary::spawn(
                name,
                signer,
                Arc::new(new_committee.clone()),
                parameters.clone(),
                store.header_store.clone(),
                store.certificate_store.clone(),
                store.payload_store.clone(),
                /* tx_consensus */ tx_new_certificates,
                /* rx_consensus */ rx_feedback,
                /* dag */ None,
                NetworkModel::Asynchronous,
                /* tx_committed_certificates */ tx_feedback,
                default_registry(),
            );
            handles.extend(primary_handles);
        }

        // Run for a while.
        for rx in rx_channels.iter_mut() {
            loop {
                let certificate = rx.recv().await.unwrap();
                if certificate.epoch() == epoch && certificate.round() == 10 {
                    break;
                }
            }
        }

        // Shutdown the committee of the previous epoch;
        for tx in &tx_channels {
            let (token, mut rx) = channel(1);
            tx.send(ConsensusPrimaryMessage::Shutdown(token))
                .await
                .unwrap();
            rx.recv().await;
        }
        join_all(handles).await;
    }
}
