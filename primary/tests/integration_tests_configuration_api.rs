// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use config::Parameters;
use consensus::dag::Dag;
use crypto::traits::KeyPair;
use node::NodeStorage;
use primary::{NetworkModel, Primary, CHANNEL_CAPACITY};
use std::{sync::Arc, time::Duration};
use test_utils::{committee, keys, temp_dir};
use tokio::sync::mpsc::channel;
use tonic::transport::Channel;
use types::{
    ConfigurationClient, ConsensusPrimaryMessage, MultiAddrProto, NewEpochRequest,
    NewNetworkInfoRequest, PublicKeyProto, ValidatorData,
};

#[tokio::test]
async fn test_new_epoch() {
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };
    let keypair = keys(None).pop().unwrap();
    let name = keypair.public().clone();
    let signer = keypair;
    let committee = committee(None);

    // Make the data store.
    let store = NodeStorage::reopen(temp_dir());

    let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
    let (_tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);

    Primary::spawn(
        name.clone(),
        signer,
        committee.clone(),
        parameters.clone(),
        store.header_store.clone(),
        store.certificate_store.clone(),
        store.payload_store.clone(),
        /* tx_consensus */ tx_new_certificates,
        /* rx_consensus */ rx_feedback,
        /* dag */ Some(Arc::new(Dag::new(&committee, rx_new_certificates).1)),
        NetworkModel::Asynchronous,
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test gRPC server with client call
    let mut client = connect_to_configuration_client(parameters.clone());

    let public_key = PublicKeyProto::from(name);
    let stake_weight = 1;
    let address = MultiAddrProto {
        address: "/ip4/127.0.0.1".to_string(),
    };

    let request = tonic::Request::new(NewEpochRequest {
        epoch_number: 0,
        validators: vec![ValidatorData {
            public_key: Some(public_key),
            stake_weight,
            address: Some(address),
        }],
    });

    let status = client.new_epoch(request).await.unwrap_err();

    println!("message: {:?}", status.message());

    // Not fully implemented but a 'Not Implemented!' message indicates no parsing errors.
    assert!(status.message().contains("Not Implemented!"));
}

#[tokio::test]
async fn test_new_network_info() {
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };
    let keypair = keys(None).pop().unwrap();
    let name = keypair.public().clone();
    let signer = keypair;
    let committee = committee(None);

    // Make the data store.
    let store = NodeStorage::reopen(temp_dir());

    let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
    let (_tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);

    Primary::spawn(
        name.clone(),
        signer,
        committee.clone(),
        parameters.clone(),
        store.header_store.clone(),
        store.certificate_store.clone(),
        store.payload_store.clone(),
        /* tx_consensus */ tx_new_certificates,
        /* rx_consensus */ rx_feedback,
        /* dag */ Some(Arc::new(Dag::new(&committee, rx_new_certificates).1)),
        NetworkModel::Asynchronous,
    );

    // Wait for tasks to start
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Test gRPC server with client call
    let mut client = connect_to_configuration_client(parameters.clone());

    let public_key = PublicKeyProto::from(name);
    let stake_weight = 1;
    let address = MultiAddrProto {
        address: "/ip4/127.0.0.1".to_string(),
    };

    let request = tonic::Request::new(NewNetworkInfoRequest {
        epoch_number: 0,
        validators: vec![ValidatorData {
            public_key: Some(public_key),
            stake_weight,
            address: Some(address),
        }],
    });

    let status = client.new_network_info(request).await.unwrap_err();

    println!("message: {:?}", status.message());

    // Not fully implemented but a 'Not Implemented!' message indicates no parsing errors.
    assert!(status.message().contains("Not Implemented!"));
}

fn connect_to_configuration_client(parameters: Parameters) -> ConfigurationClient<Channel> {
    let config = mysten_network::config::Config::new();
    let channel = config
        .connect_lazy(&parameters.consensus_api_grpc.socket_addr)
        .unwrap();
    ConfigurationClient::new(channel)
}

#[tokio::test]
async fn test_reconfigure() {
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Make the first committee.
    let committee = committee(None);

    // Spawn the committee.
    let mut rx_channels = Vec::new();
    let mut tx_channels = Vec::new();
    for keypair in keys(None) {
        let name = keypair.public().clone();
        let signer = keypair;

        let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
        rx_channels.push(rx_new_certificates);
        let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
        tx_channels.push(tx_feedback);

        let store = NodeStorage::reopen(temp_dir());

        Primary::spawn(
            name,
            signer,
            committee.clone(),
            parameters.clone(),
            store.header_store.clone(),
            store.certificate_store.clone(),
            store.payload_store.clone(),
            /* tx_consensus */ tx_new_certificates,
            /* rx_consensus */ rx_feedback,
            /* dag */ None,
            NetworkModel::Asynchronous,
        );
    }

    // Wait for tasks to start.
    // tokio::time::sleep(Duration::from_secs(1)).await;

    // for rx in rx_channels.iter_mut() {
    let rx = &mut rx_channels[0];
    loop {
        let certificate = rx.recv().await.unwrap();
        assert_eq!(certificate.epoch(), 0);
        println!("{certificate:?}");
        if certificate.round() == 10 {
            break;
        }
    }
    // }

    println!("\n\n");

    // Make a new committee.
    let new_committee = test_utils::pure_committee_from_keys(&keys(None));
    new_committee.epoch.swap(Arc::new(1));

    let mut addresses = new_committee
        .authorities
        .load()
        .iter()
        .map(|(_, authority)| authority.primary.primary_to_primary.clone())
        .collect::<Vec<_>>();
    addresses.sort();
    println!("New Committee: {:?}", addresses);

    // Change the committee to epoch 1.
    for tx in &tx_channels {
        tx.send(ConsensusPrimaryMessage::Committee(new_committee.clone()))
            .await
            .unwrap();
    }

    println!("\n\n");

    // for rx in rx_channels.iter_mut() {
    let rx = &mut rx_channels[0];
    loop {
        let certificate = rx.recv().await.unwrap();
        println!("{certificate:?}");
        if certificate.epoch() == 1 && certificate.round() == 10 {
            break;
        }
    }
    // }
}
