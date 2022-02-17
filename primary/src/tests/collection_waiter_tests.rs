use crate::collection_waiter::{BatchMessage, CollectionCommand, CollectionWaiter};
use crate::common::{committee, committee_with_base_port, create_db_stores, keys};
use crate::{Certificate, Header};
use bytes::Bytes;
use config::WorkerId;
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use crypto::{Digest, Hash};
use ed25519_dalek::{Digest as _, Sha512, Signer};
use futures::{SinkExt, StreamExt};
use std::collections::BTreeMap;
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
        name,
        committee,
        header_store.clone(),
        rx_commands,
        tx_get_collection,
        rx_batch_messages,
    );

    // AND spin up a worker node
    /*
    let worker_id = 0;
    let (target, _) = keys.pop().unwrap();
    let worker_address = committee.worker(&target, &worker_id).unwrap().worker_to_worker;
    */

    // let handle = listener(worker_address, Some(Bytes::from(serialized)));

    // WHEN we send a request to get a collection
    send_get_collection(tx_commands.clone(), header.id.clone()).await;

    // AND "mock" the batch responses
    for (batch_id, worker_id) in header.payload {
        tx_batch_messages
            .send(BatchMessage {
                id: batch_id,
                transactions: vec![vec![10u8, 5u8, 2u8], vec![8u8, 2u8, 3u8]],
            })
            .await;
    }

    // THEN we should expect to get back the result
    let timer = sleep(Duration::from_millis(5_000));
    tokio::pin!(timer);

    tokio::select! {
        Some(result) = rx_get_collection.recv() => {
            assert!(result.is_ok(), "Expected to receive a successful result, instead got error: {}", result.err().unwrap());

            let collection = result.unwrap();

            assert!(!collection.batches.is_empty());
            assert_eq!(collection.id, header.id.clone());
        },
        () = &mut timer => {
            panic!("Timeout, no result has been received in time")
        }
    }
}

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

// worker_listener listens to TCP requests
pub fn worker_listener(address: SocketAddr, expected: Option<Bytes>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());

        let (mut writer, mut reader) = transport.split();
        match reader.next().await {
            Some(Ok(received)) => {
                writer.send(Bytes::from("Ack")).await.unwrap();
                if let Some(expected) = expected {
                    assert_eq!(received.freeze(), expected);
                }
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}
