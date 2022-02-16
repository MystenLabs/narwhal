use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use bytes::Bytes;
use ed25519_dalek::{Digest as _, Sha512, Signer};
use futures::{SinkExt, StreamExt};
use store::reopen;
use tokio::net::TcpListener;
use crypto::{Digest, Hash};
use crate::collection_waiter::{GetCollectionResult, CollectionWaiter, CollectionCommand, BatchMessage};
use crate::common::{
    certificate, committee, committee_with_base_port, create_db_stores, header, headers, keys,
    listener,
};
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use tokio::sync::mpsc::channel;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use config::WorkerId;
use crate::error::DagResult;
use crate::{Certificate, Header};

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

    // AND store headers and batches
    let header = header_with_payload();
    header_store.write(header.clone().id, header.clone()).await;

    // AND spawn a new collections waiter
    let (tx_commands, rx_commands) = channel(1);
    let (tx_get_collection, mut rx_get_collection) = channel(1);
    let (tx_batch_messages, rx_batch_messages) = channel(2);

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

    // AND send a get collection command
    match tx_commands.send(CollectionCommand::GetCollection { id: header.id.clone() }).await {
        Result::Ok(_) => {

        },
        Result::Err(err) => {
            panic!("{}", err);
        }
    }

    /*
    tokio::spawn( async move {

    });*/
    // "mock" the batch responses
    for (batch_id, worker_id) in header.payload {
        tx_batch_messages.send(
            BatchMessage{
                id: batch_id,
                transactions: vec![
                    vec![10u8, 5u8, 2u8], vec![8u8, 2u8, 3u8]
                ]
            }
        ).await;
    }

    // Wait to receive back a response or timeout
    let timer = sleep(Duration::from_millis(4_000));
    tokio::pin!(timer);

    tokio::select! {
        Some(result) = rx_get_collection.recv() => {
            match result {
                Ok(r) => {
                    assert!(!r.transactions.is_empty());
                    assert_eq!(r.id, header.id.clone());
                },
                Err(err) => {
                    panic!("error retrieving result: {}", err);
                }
            }
        },
        () = &mut timer => {
            println!("Timeout ended");
            assert!(false);
        }
    }
}

pub fn header_with_payload() -> Header<Ed25519PublicKey> {
    let kp = keys().pop().unwrap();
    let mut payload: BTreeMap<Digest, WorkerId> = BTreeMap::new();

    let batch_digest_1 = Digest::new(Sha512::digest(vec![10u8, 5u8, 8u8, 20u8].as_slice()).as_slice()[..32].try_into().unwrap());
    let batch_digest_2 = Digest::new(Sha512::digest(vec![14u8, 2u8, 7u8, 10u8].as_slice()).as_slice()[..32].try_into().unwrap());

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