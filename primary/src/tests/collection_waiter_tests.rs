// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    collection_waiter::{
        BatchMessage, CollectionCommand, CollectionErrorType, CollectionResult, CollectionWaiter,
        GetCollectionResponse,
    },
    common,
    common::{certificate, committee_with_base_port, create_db_stores, keys},
    PrimaryWorkerMessage,
};
use bincode::deserialize;
use config::{Committee};
use crypto::{
    ed25519::Ed25519PublicKey,
    traits::{KeyPair, VerifyingKey},
    Digest, Hash,
};
use ed25519_dalek::{Digest as _, Sha512};
use futures::StreamExt;
use network::SimpleSender;
use std::{
    collections::{HashMap},
    net::SocketAddr,
};
use tokio::{
    net::TcpListener,
    sync::mpsc::{channel, Sender},
    task::JoinHandle,
    time::{sleep, timeout, Duration},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::test]
async fn test_successfully_retrieve_collection() {
    // GIVEN
    let (_, certificate_store, _) = create_db_stores();

    // AND the necessary keys
    let (name, committee) = resolve_name_and_committee();

    // AND store certificate
    let header = common::fixture_header_with_payload(2);
    let certificate = certificate(&header);
    certificate_store
        .write(certificate.digest(), certificate.clone())
        .await;

    let collection_id = certificate.digest();

    // AND spawn a new collections waiter
    let (tx_commands, rx_commands) = channel(1);
    let (tx_get_collection, mut rx_get_collection) = channel(1);
    let (tx_batch_messages, rx_batch_messages) = channel(10);

    CollectionWaiter::spawn(
        name.clone(),
        committee.clone(),
        certificate_store.clone(),
        rx_commands,
        rx_batch_messages,
    );

    // AND "mock" the batch responses
    let mut expected_batch_messages = HashMap::new();
    for (batch_id, _) in header.payload {
        expected_batch_messages.insert(
            batch_id.clone(),
            BatchMessage {
                id: batch_id,
                transactions: vec![vec![10u8, 5u8, 2u8], vec![8u8, 2u8, 3u8]],
            },
        );
    }

    // AND spin up a worker node
    let worker_id = 0;
    let worker_address = committee
        .worker(&name, &worker_id)
        .unwrap()
        .primary_to_worker;

    let handle = worker_listener::<Ed25519PublicKey>(
        worker_address,
        expected_batch_messages.clone(),
        tx_batch_messages,
    );

    // WHEN we send a request to get a collection
    //send_get_collection(tx_commands.clone(), collection_id.clone()).await;
    tx_commands
        .send(CollectionCommand::GetCollection {
            id: collection_id.clone(),
            sender: tx_get_collection,
        })
        .await;

    // Wait for the worker server to complete before continue.
    // Then we'll be confident that the expected batch responses
    // have been sent (via the tx_batch_messages channel though)
    match timeout(Duration::from_millis(4_000), handle).await {
        Err(_) => panic!("worker hasn't received expected batch requests"),
        _ => {}
    }

    // THEN we should expect to get back the result
    let timer = sleep(Duration::from_millis(5_000));
    tokio::pin!(timer);

    tokio::select! {
        Some(result) = rx_get_collection.recv() => {
            assert!(result.is_ok(), "Expected to receive a successful result, instead got error: {}", result.err().unwrap());

            let collection = result.unwrap();

            assert_eq!(collection.batches.len(), expected_batch_messages.len());
            assert_eq!(collection.id, collection_id.clone());

            for (_, batch) in expected_batch_messages {
                assert_eq!(batch.transactions.len(), 2);
            }
        },
        () = &mut timer => {
            panic!("Timeout, no result has been received in time")
        }
    }
}

#[tokio::test]
async fn test_one_pending_request_for_collection_at_time() {
    // GIVEN
    let (_, certificate_store, _) = create_db_stores();

    // AND the necessary keys
    let (name, committee) = resolve_name_and_committee();

    // AND store certificate
    let header = common::fixture_header_with_payload(2);
    let certificate = certificate(&header);
    certificate_store
        .write(certificate.digest(), certificate.clone())
        .await;

    let collection_id = certificate.digest();

    // AND spawn a new collections waiter
    let (_, rx_commands) = channel(1);
    let (_, rx_batch_messages) = channel(1);

    let mut waiter = CollectionWaiter {
        name: name.clone(),
        committee: committee.clone(),
        certificate_store: certificate_store.clone(),
        rx_commands,
        pending_get_collection: HashMap::new(),
        network: SimpleSender::new(),
        rx_batch_receiver: rx_batch_messages,
        tx_pending_batch: HashMap::new(),
        tx_get_collection_map: HashMap::new(),
    };

    let get_mock_sender = || {
        let (tx, _) = channel(1);
        return tx;
    };

    // WHEN we send GetCollection command
    let result_some = waiter
        .handle_command(CollectionCommand::GetCollection {
            id: collection_id.clone(),
            sender: get_mock_sender(),
        })
        .await;

    // AND we send more GetCollection commands
    let mut results_none = Vec::new();
    for _ in 0..3 {
        results_none.push(
            waiter
                .handle_command(CollectionCommand::GetCollection {
                    id: collection_id.clone(),
                    sender: get_mock_sender(),
                })
                .await,
        );
    }

    // THEN
    assert!(
        result_some.is_some(),
        "Expected to have a future to do some further work"
    );

    for result in results_none {
        assert!(
            result.is_none(),
            "Expected to not get a future for further work"
        );
    }
}

#[tokio::test]
async fn test_unlocking_pending_get_collection_request_after_response() {
    // GIVEN
    let (_, certificate_store, _) = create_db_stores();

    // AND the necessary keys
    let (name, committee) = resolve_name_and_committee();

    // AND store certificate
    let header = common::fixture_header_with_payload(2);
    let certificate = certificate(&header);
    certificate_store
        .write(certificate.digest(), certificate.clone())
        .await;

    let collection_id = certificate.digest();

    // AND spawn a new collections waiter
    let (_, rx_commands) = channel(1);
    let (_, rx_batch_messages) = channel(1);

    let mut waiter = CollectionWaiter {
        name: name.clone(),
        committee: committee.clone(),
        certificate_store: certificate_store.clone(),
        rx_commands,
        pending_get_collection: HashMap::new(),
        network: SimpleSender::new(),
        rx_batch_receiver: rx_batch_messages,
        tx_pending_batch: HashMap::new(),
        tx_get_collection_map: HashMap::new(),
    };

    let get_mock_sender = || {
        let (tx, _) = channel(1);
        return tx;
    };

    // AND we send GetCollection commands
    for _ in 0..3 {
        waiter
            .handle_command(CollectionCommand::GetCollection {
                id: collection_id.clone(),
                sender: get_mock_sender(),
            })
            .await;
    }

    // WHEN
    let result = CollectionResult::Ok(GetCollectionResponse {
        id: collection_id.clone(),
        batches: vec![],
    });

    waiter.handle_batch_waiting_result(result).await;

    // THEN
    assert_eq!(
        waiter.pending_get_collection.contains_key(&collection_id),
        false
    );
    assert_eq!(
        waiter.tx_get_collection_map.contains_key(&collection_id),
        false
    );
}

#[tokio::test]
async fn test_batch_timeout() {
    // GIVEN
    let (_, certificate_store, _) = create_db_stores();

    // AND the necessary keys
    let (name, committee) = resolve_name_and_committee();

    // AND store certificate
    let header = common::fixture_header_with_payload(2);
    let certificate = certificate(&header);
    certificate_store
        .write(certificate.digest(), certificate.clone())
        .await;

    let collection_id = certificate.digest();

    // AND spawn a new collections waiter
    let (tx_commands, rx_commands) = channel(1);
    let (tx_get_collection, mut rx_get_collection) = channel(1);
    let (_, rx_batch_messages) = channel(10);

    CollectionWaiter::spawn(
        name.clone(),
        committee.clone(),
        certificate_store.clone(),
        rx_commands,
        rx_batch_messages,
    );

    // WHEN we send a request to get a collection
    //send_get_collection(tx_commands.clone(), collection_id.clone()).await;
    tx_commands
        .send(CollectionCommand::GetCollection {
            id: collection_id.clone(),
            sender: tx_get_collection,
        })
        .await;

    // THEN we should expect to get back the result
    let timer = sleep(Duration::from_millis(5_000));
    tokio::pin!(timer);

    tokio::select! {
        Some(result) = rx_get_collection.recv() => {
            assert!(result.is_err(), "Expected to receive an error result");

            let collection_error = result.err().unwrap();

            assert_eq!(collection_error.id, collection_id.clone());
            assert_eq!(collection_error.error, CollectionErrorType::BatchTimeout);
        },
        () = &mut timer => {
            panic!("Timeout, no result has been received in time")
        }
    }
}

// helper method to get a name and a committee
fn resolve_name_and_committee() -> (Ed25519PublicKey, Committee<Ed25519PublicKey>) {
    let mut keys = keys();
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let kp = keys.pop().unwrap();
    let name = kp.public().clone();
    let committee = committee_with_base_port(13_000);

    (name, committee)
}

// worker_listener listens to TCP requests. The worker responds to the
// RequestBatch requests for the provided expected_batches.
pub fn worker_listener<PublicKey: VerifyingKey>(
    address: SocketAddr,
    expected_batches: HashMap<Digest, BatchMessage>,
    tx_batch_messages: Sender<BatchMessage>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());

        println!("Start listening server");

        let (_, mut reader) = transport.split();
        let mut counter = 0;
        loop {
            match reader.next().await {
                Some(Ok(received)) => {
                    let message = received.freeze();
                    match deserialize(&message) {
                        Ok(PrimaryWorkerMessage::<PublicKey>::RequestBatch(id)) => {
                            if expected_batches.contains_key(&id) {
                                tx_batch_messages
                                    .send(expected_batches.get(&id).cloned().unwrap())
                                    .await;

                                counter += 1;

                                // Once all the expected requests have been received, break the loop
                                // of the server.
                                if counter == expected_batches.len() {
                                    break;
                                }
                            }
                        }
                        _ => panic!("Unexpected request received"),
                    };
                }
                _ => panic!("Failed to receive network message"),
            }
        }
    })
}
