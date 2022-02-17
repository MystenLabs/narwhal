use crate::collection_waiter::{BatchMessage, CollectionCommand, CollectionWaiter};
use crate::common::{committee, committee_with_base_port, create_db_stores, keys};
use crate::{Certificate, Header, PrimaryWorkerMessage};
use bincode::deserialize;
use config::WorkerId;
use crypto::traits::VerifyingKey;
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use crypto::{Digest, Hash};
use ed25519_dalek::{Digest as _, Sha512, Signer};
use futures::{StreamExt};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::test]
async fn test_successfully_retrieve_collection() {
    // GIVEN
    let (header_store, _, _) = create_db_stores();

    // AND the necessary keys
    let mut keys = keys();
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let kp = keys.pop().unwrap();
    let name = kp.public().clone();
    let committee = committee_with_base_port(13_000);

    // AND store header
    let header = fixture_header_with_payload();
    header_store.write(header.clone().id, header.clone()).await;

    // AND spawn a new collections waiter
    let (tx_commands, rx_commands) = channel(1);
    let (tx_get_collection, mut rx_get_collection) = channel(1);
    let (tx_batch_messages, rx_batch_messages) = channel(10);

    CollectionWaiter::spawn(
        name.clone(),
        committee.clone(),
        header_store.clone(),
        rx_commands,
        tx_get_collection,
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
    send_get_collection(tx_commands.clone(), header.id.clone()).await;
    //send_get_collection(tx_commands.clone(), header.id.clone()).await;

    // Wait for the worker server to complete before continue.
    // Then we'll be confident that the expected batch responses
    // have been sent (via the tx_batch_messages channel though)
    handle.await;

    // THEN we should expect to get back the result
    let timer = sleep(Duration::from_millis(5_000));
    tokio::pin!(timer);

    tokio::select! {
        Some(result) = rx_get_collection.recv() => {
            assert!(result.is_ok(), "Expected to receive a successful result, instead got error: {}", result.err().unwrap());

            let collection = result.unwrap();

            assert_eq!(collection.batches.len(), expected_batch_messages.len());
            assert_eq!(collection.id, header.id.clone());
        },
        () = &mut timer => {
            panic!("Timeout, no result has been received in time")
        }
    }
}

#[tokio::test]
async fn test_one_pending_request_for_collection_at_time() {}

async fn send_get_collection(sender: Sender<CollectionCommand>, collection_id: Digest) {
    sender
        .send(CollectionCommand::GetCollection { id: collection_id })
        .await;
}

pub fn fixture_header_with_payload() -> Header<Ed25519PublicKey> {
    let kp = keys().pop().unwrap();
    let mut payload: BTreeMap<Digest, WorkerId> = BTreeMap::new();

    let batch_digest_1 = Digest::new(
        Sha512::digest(vec![10u8, 5u8, 8u8, 20u8].as_slice()).as_slice()[..32]
            .try_into()
            .unwrap(),
    );
    let batch_digest_2 = Digest::new(
        Sha512::digest(vec![14u8, 2u8, 7u8, 10u8].as_slice()).as_slice()[..32]
            .try_into()
            .unwrap(),
    );

    payload.insert(batch_digest_1, 0);
    payload.insert(batch_digest_2, 0);

    let header = Header {
        author: kp.public().clone(),
        round: 1,
        parents: Certificate::genesis(&committee())
            .iter()
            .map(|x| x.digest())
            .collect(),
        payload,
        ..Header::default()
    };

    Header {
        id: header.digest(),
        signature: kp.sign(header.digest().as_ref()),
        ..header
    }
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
