// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use blake2::digest::Update;
use bytes::Bytes;
use config::{Authority, Committee, PrimaryAddresses, WorkerAddresses, WorkerId};
use crypto::{
    ed25519::{Ed25519KeyPair, Ed25519PublicKey, Ed25519Signature},
    traits::{KeyPair, Signer},
    Digest, Hash as _,
};
use futures::{sink::SinkExt as _, stream::StreamExt as _};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{collections::BTreeMap, net::SocketAddr};
use store::{rocks, Store};
use tokio::{
    net::TcpListener,
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tonic::Response;
use types::{
    Batch, BatchDigest, BincodeEncodedPayload, Certificate, Empty, Header, PrimaryToPrimary,
    PrimaryToPrimaryServer, PrimaryToWorker, PrimaryToWorkerServer, Transaction, Vote,
    WorkerToPrimary, WorkerToPrimaryServer,
};

pub const HEADERS_CF: &str = "headers";
pub const CERTIFICATES_CF: &str = "certificates";
pub const PAYLOAD_CF: &str = "payload";

pub fn temp_dir() -> std::path::PathBuf {
    tempfile::tempdir()
        .expect("Failed to open temporary directory")
        .into_path()
}

////////////////////////////////////////////////////////////////
/// Keys, Committee
////////////////////////////////////////////////////////////////

// Fixture
pub fn keys() -> Vec<Ed25519KeyPair> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| Ed25519KeyPair::generate(&mut rng)).collect()
}

// Fixture
pub fn committee() -> Committee<Ed25519PublicKey> {
    Committee {
        authorities: keys()
            .iter()
            .enumerate()
            .map(|(i, kp)| {
                let id = kp.public();
                let primary = PrimaryAddresses {
                    primary_to_primary: format!("127.0.0.1:{}", 100 + i * 10).parse().unwrap(),
                    worker_to_primary: format!("127.0.0.1:{}", 200 + i * 10).parse().unwrap(),
                };
                let workers = vec![
                    (
                        0,
                        WorkerAddresses {
                            primary_to_worker: format!("127.0.0.1:{}", 300 + i * 10)
                                .parse()
                                .unwrap(),
                            transactions: format!("127.0.0.1:{}", 400 + i * 10).parse().unwrap(),
                            worker_to_worker: format!("127.0.0.1:{}", 500 + i * 10)
                                .parse()
                                .unwrap(),
                        },
                    ),
                    (
                        1,
                        WorkerAddresses {
                            primary_to_worker: format!("127.0.0.1:{}", 300 + i * 10 + 1)
                                .parse()
                                .unwrap(),
                            transactions: format!("127.0.0.1:{}", 400 + i * 10 + 1)
                                .parse()
                                .unwrap(),
                            worker_to_worker: format!("127.0.0.1:{}", 500 + i * 10 + 1)
                                .parse()
                                .unwrap(),
                        },
                    ),
                    (
                        2,
                        WorkerAddresses {
                            primary_to_worker: format!("127.0.0.1:{}", 300 + i * 10 + 2)
                                .parse()
                                .unwrap(),
                            transactions: format!("127.0.0.1:{}", 400 + i * 10 + 2)
                                .parse()
                                .unwrap(),
                            worker_to_worker: format!("127.0.0.1:{}", 500 + i * 10 + 2)
                                .parse()
                                .unwrap(),
                        },
                    ),
                    (
                        3,
                        WorkerAddresses {
                            primary_to_worker: format!("127.0.0.1:{}", 300 + i * 10 + 3)
                                .parse()
                                .unwrap(),
                            transactions: format!("127.0.0.1:{}", 400 + i * 10 + 3)
                                .parse()
                                .unwrap(),
                            worker_to_worker: format!("127.0.0.1:{}", 500 + i * 10 + 3)
                                .parse()
                                .unwrap(),
                        },
                    ),
                ]
                .iter()
                .cloned()
                .collect();
                (
                    id.clone(),
                    Authority {
                        stake: 1,
                        primary,
                        workers,
                    },
                )
            })
            .collect(),
    }
}

// Fixture.
pub fn committee_with_base_port(base_port: u16) -> Committee<Ed25519PublicKey> {
    let mut committee = committee();
    for authority in committee.authorities.values_mut() {
        let primary = &mut authority.primary;

        let port = primary.primary_to_primary.port();
        primary.primary_to_primary.set_port(base_port + port);

        let port = primary.worker_to_primary.port();
        primary.worker_to_primary.set_port(base_port + port);

        for worker in authority.workers.values_mut() {
            let port = worker.primary_to_worker.port();
            worker.primary_to_worker.set_port(base_port + port);

            let port = worker.transactions.port();
            worker.transactions.set_port(base_port + port);

            let port = worker.worker_to_worker.port();
            worker.worker_to_worker.set_port(base_port + port);
        }
    }
    committee
}

////////////////////////////////////////////////////////////////
/// Headers, Votes, Certificates
////////////////////////////////////////////////////////////////

// Fixture
pub fn header() -> Header<Ed25519PublicKey> {
    let kp = keys().pop().unwrap();
    let header = Header {
        author: kp.public().clone(),
        round: 1,
        parents: Certificate::genesis(&committee())
            .iter()
            .map(|x| x.digest())
            .collect(),
        ..Header::default()
    };

    let header_digest = header.digest();
    Header {
        id: header_digest,
        signature: kp.sign(Digest::from(header_digest).as_ref()),
        ..header
    }
}

// Fixture
pub fn headers() -> Vec<Header<Ed25519PublicKey>> {
    keys()
        .into_iter()
        .map(|kp| {
            let header = Header {
                author: kp.public().clone(),
                round: 1,
                parents: Certificate::genesis(&committee())
                    .iter()
                    .map(|x| x.digest())
                    .collect(),
                ..Header::default()
            };
            let header_digest = header.digest();
            Header {
                id: header_digest,
                signature: kp.sign(Digest::from(header_digest).as_ref()),
                ..header
            }
        })
        .collect()
}

#[allow(dead_code)]
pub fn fixture_header() -> Header<Ed25519PublicKey> {
    let kp = keys().pop().unwrap();

    fixture_header_builder().build(|payload| kp.sign(payload))
}

pub fn fixture_header_builder() -> types::HeaderBuilder<Ed25519PublicKey> {
    let kp = keys().pop().unwrap();

    let builder = types::HeaderBuilder::<Ed25519PublicKey>::default();
    builder.author(kp.public().clone()).round(1).parents(
        Certificate::genesis(&committee())
            .iter()
            .map(|x| x.digest())
            .collect(),
    )
}

pub fn fixture_header_with_payload(number_of_batches: u8) -> Header<Ed25519PublicKey> {
    let kp = keys().pop().unwrap();
    let mut payload: BTreeMap<BatchDigest, WorkerId> = BTreeMap::new();

    for i in 0..number_of_batches {
        let batch_digest = BatchDigest(crypto::blake2b_256(|hasher| {
            hasher.update(vec![10u8, 5u8, 8u8, 20u8, i])
        }));
        payload.insert(batch_digest, 0);
    }

    let builder = fixture_header_builder();
    builder.payload(payload).build(|payload| kp.sign(payload))
}

// will create a batch with randomly formed transactions
// dictated by the parameter number_of_transactions
pub fn fixture_batch_with_transactions(number_of_transactions: u32) -> Batch {
    let transactions = (0..number_of_transactions)
        .map(|_v| transaction())
        .collect();

    Batch(transactions)
}

// Fixture
pub fn transaction() -> Transaction {
    // generate random value transactions, but the length will be always 100 bytes
    (0..100).map(|_v| rand::random::<u8>()).collect()
}

// Fixture
pub fn votes(header: &Header<Ed25519PublicKey>) -> Vec<Vote<Ed25519PublicKey>> {
    keys()
        .into_iter()
        .map(|kp| {
            let vote = Vote {
                id: header.id,
                round: header.round,
                origin: header.author.clone(),
                author: kp.public().clone(),
                signature: Ed25519Signature::default(),
            };
            Vote {
                signature: kp.sign(Digest::from(vote.digest()).as_ref()),
                ..vote
            }
        })
        .collect()
}

// Fixture
pub fn certificate(header: &Header<Ed25519PublicKey>) -> Certificate<Ed25519PublicKey> {
    Certificate {
        header: header.clone(),
        votes: votes(header)
            .into_iter()
            .map(|x| (x.author, x.signature))
            .collect(),
    }
}

// Fixture
pub fn listener(address: SocketAddr) -> JoinHandle<Bytes> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        match reader.next().await {
            Some(Ok(received)) => {
                writer.send(Bytes::from("Ack")).await.unwrap();
                received.freeze()
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}

// Fixture
pub fn expecting_listener(address: SocketAddr, expected: Option<Bytes>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        match reader.next().await {
            Some(Ok(received)) => {
                writer.send(Bytes::from("Ack")).await.unwrap();
                if let Some(expected) = expected {
                    let rcvd = received.freeze();
                    assert_eq!(
                        rcvd.clone(),
                        expected.clone(),
                        "expected {} received {}",
                        hex::encode(expected),
                        hex::encode(rcvd)
                    );
                }
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}

pub struct PrimaryToPrimaryMockServer {
    sender: Sender<BincodeEncodedPayload>,
}

impl PrimaryToPrimaryMockServer {
    pub fn spawn(address: SocketAddr) -> Receiver<BincodeEncodedPayload> {
        let (sender, receiver) = channel(1);
        let mock = Self { sender };
        let service = tonic::transport::Server::builder()
            .add_service(PrimaryToPrimaryServer::new(mock))
            .serve(address);
        tokio::spawn(service);
        receiver
    }
}

#[tonic::async_trait]
impl PrimaryToPrimary for PrimaryToPrimaryMockServer {
    async fn send_message(
        &self,
        request: tonic::Request<BincodeEncodedPayload>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        self.sender.send(request.into_inner()).await.unwrap();
        Ok(Response::new(Empty {}))
    }
}

pub struct WorkerToPrimaryMockServer {
    sender: Sender<BincodeEncodedPayload>,
}

impl WorkerToPrimaryMockServer {
    pub fn spawn(address: SocketAddr) -> Receiver<BincodeEncodedPayload> {
        let (sender, receiver) = channel(1);
        let mock = Self { sender };
        let service = tonic::transport::Server::builder()
            .add_service(WorkerToPrimaryServer::new(mock))
            .serve(address);
        tokio::spawn(service);
        receiver
    }
}

#[tonic::async_trait]
impl WorkerToPrimary for WorkerToPrimaryMockServer {
    async fn send_message(
        &self,
        request: tonic::Request<BincodeEncodedPayload>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        self.sender.send(request.into_inner()).await.unwrap();
        Ok(Response::new(Empty {}))
    }
}

pub struct PrimaryToWorkerMockServer {
    sender: Sender<BincodeEncodedPayload>,
}

impl PrimaryToWorkerMockServer {
    pub fn spawn(address: SocketAddr) -> Receiver<BincodeEncodedPayload> {
        let (sender, receiver) = channel(1);
        let mock = Self { sender };
        let service = tonic::transport::Server::builder()
            .add_service(PrimaryToWorkerServer::new(mock))
            .serve(address);
        tokio::spawn(service);
        receiver
    }
}

#[tonic::async_trait]
impl PrimaryToWorker for PrimaryToWorkerMockServer {
    async fn send_message(
        &self,
        request: tonic::Request<BincodeEncodedPayload>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        self.sender.send(request.into_inner()).await.unwrap();
        Ok(Response::new(Empty {}))
    }
}

// helper method to get a name and a committee. Special care should be given on
// the base_port parameter to not collide between tests. It is advisable to
// provide unique base_port numbers across the tests.
pub fn resolve_name_and_committee(
    base_port: u16,
) -> (Ed25519PublicKey, Committee<Ed25519PublicKey>) {
    let mut keys = keys();
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let kp = keys.pop().unwrap();
    let name = kp.public().clone();
    let committee = committee_with_base_port(base_port);

    (name, committee)
}

////////////////////////////////////////////////////////////////
/// Batches
////////////////////////////////////////////////////////////////

// Fixture
pub fn batch() -> Batch {
    Batch(vec![transaction(), transaction()])
}

// Fixture
pub fn batch_digest() -> BatchDigest {
    resolve_batch_digest(serialized_batch())
}

pub fn digest_batch(batch: Batch) -> BatchDigest {
    let serialized_batch = serialize_batch_message(batch);
    resolve_batch_digest(serialized_batch)
}

// Fixture
pub fn resolve_batch_digest(batch_serialised: Vec<u8>) -> BatchDigest {
    BatchDigest::new(crypto::blake2b_256(|hasher| {
        hasher.update(&batch_serialised)
    }))
}

// Fixture
pub fn serialized_batch() -> Vec<u8> {
    serialize_batch_message(batch())
}

pub fn serialize_batch_message(batch: Batch) -> Vec<u8> {
    let message = worker::WorkerMessage::<Ed25519PublicKey>::Batch(batch);
    bincode::serialize(&message).unwrap()
}

/// generate multiple fixture batches. The number of generated batches
/// are dictated by the parameter num_of_batches.
pub fn batches(num_of_batches: usize) -> Vec<Batch> {
    let mut batches = Vec::new();

    for i in 1..num_of_batches + 1 {
        batches.push(batch_with_transactions(i));
    }

    batches
}

pub fn batch_with_transactions(num_of_transactions: usize) -> Batch {
    let mut transactions = Vec::new();

    for _ in 0..num_of_transactions {
        transactions.push(transaction());
    }

    Batch(transactions)
}

const BATCHES_CF: &str = "batches";

pub fn open_batch_store() -> Store<BatchDigest, worker::SerializedBatchMessage> {
    let db = rocks::DBMap::<BatchDigest, worker::SerializedBatchMessage>::open(
        temp_dir(),
        None,
        Some(BATCHES_CF),
    )
    .unwrap();
    Store::new(db)
}
