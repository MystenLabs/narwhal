// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use blake2::digest::Update;
use config::{
    utils::get_available_port, Authority, Committee, PrimaryAddresses, WorkerAddresses, WorkerId,
};
use crypto::{
    ed25519::{Ed25519KeyPair, Ed25519PublicKey, Ed25519Signature},
    traits::{KeyPair, Signer},
    Digest, Hash as _,
};
use futures::Stream;
use multiaddr::Multiaddr;
use rand::{rngs::StdRng, SeedableRng as _};
use std::{collections::BTreeMap, pin::Pin};
use store::{rocks, Store};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tonic::Response;
use types::{
    Batch, BatchDigest, BincodeEncodedPayload, Certificate, Empty, Header, PrimaryToPrimary,
    PrimaryToPrimaryServer, PrimaryToWorker, PrimaryToWorkerServer, Transaction, Vote,
    WorkerToPrimary, WorkerToPrimaryServer, WorkerToWorker, WorkerToWorkerServer,
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
pub fn keys(rng_seed: impl Into<Option<u64>>) -> Vec<Ed25519KeyPair> {
    let seed = rng_seed.into().unwrap_or(0u64).to_le_bytes();
    let mut rng_arg = [0u8; 32];
    for i in 0..4 {
        rng_arg[i * 8..(i + 1) * 8].copy_from_slice(&seed[..]);
    }

    let mut rng = StdRng::from_seed(rng_arg);
    (0..4).map(|_| Ed25519KeyPair::generate(&mut rng)).collect()
}

// Fixture
pub fn committee(rng_seed: impl Into<Option<u64>>) -> Committee<Ed25519PublicKey> {
    Committee {
        authorities: keys(rng_seed)
            .iter()
            .map(|kp| {
                let id = kp.public();
                let primary = PrimaryAddresses {
                    primary_to_primary: format!("/ip4/127.0.0.1/tcp/{}/http", get_available_port())
                        .parse()
                        .unwrap(),
                    worker_to_primary: format!("/ip4/127.0.0.1/tcp/{}/http", get_available_port())
                        .parse()
                        .unwrap(),
                };
                let workers = vec![
                    (
                        0,
                        WorkerAddresses {
                            primary_to_worker: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                            transactions: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                            worker_to_worker: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                        },
                    ),
                    (
                        1,
                        WorkerAddresses {
                            primary_to_worker: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                            transactions: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                            worker_to_worker: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                        },
                    ),
                    (
                        2,
                        WorkerAddresses {
                            primary_to_worker: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                            transactions: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                            worker_to_worker: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                        },
                    ),
                    (
                        3,
                        WorkerAddresses {
                            primary_to_worker: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                            transactions: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
                            .parse()
                            .unwrap(),
                            worker_to_worker: format!(
                                "/ip4/127.0.0.1/tcp/{}/http",
                                get_available_port()
                            )
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

////////////////////////////////////////////////////////////////
/// Headers, Votes, Certificates
////////////////////////////////////////////////////////////////

// Fixture
pub fn header() -> Header<Ed25519PublicKey> {
    let kp = keys(None).pop().unwrap();
    let header = Header {
        author: kp.public().clone(),
        round: 1,
        parents: Certificate::genesis(&committee(None))
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
    keys(None)
        .into_iter()
        .map(|kp| {
            let header = Header {
                author: kp.public().clone(),
                round: 1,
                parents: Certificate::genesis(&committee(None))
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
    let kp = keys(None).pop().unwrap();

    fixture_header_builder().build(|payload| kp.sign(payload))
}

pub fn fixture_header_builder() -> types::HeaderBuilder<Ed25519PublicKey> {
    let kp = keys(None).pop().unwrap();

    let builder = types::HeaderBuilder::<Ed25519PublicKey>::default();
    builder.author(kp.public().clone()).round(1).parents(
        Certificate::genesis(&committee(None))
            .iter()
            .map(|x| x.digest())
            .collect(),
    )
}

pub fn fixture_header_with_payload(number_of_batches: u8) -> Header<Ed25519PublicKey> {
    let kp = keys(None).pop().unwrap();
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
    keys(None)
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

pub struct PrimaryToPrimaryMockServer {
    sender: Sender<BincodeEncodedPayload>,
}

impl PrimaryToPrimaryMockServer {
    pub fn spawn(address: Multiaddr) -> Receiver<BincodeEncodedPayload> {
        let (sender, receiver) = channel(1);
        tokio::spawn(async move {
            let config = mysten_network::config::Config::new();
            let mock = Self { sender };
            config
                .server_builder()
                .add_service(PrimaryToPrimaryServer::new(mock))
                .bind(&address)
                .await
                .unwrap()
                .serve()
                .await
        });
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
    pub fn spawn(address: Multiaddr) -> Receiver<BincodeEncodedPayload> {
        let (sender, receiver) = channel(1);
        tokio::spawn(async move {
            let config = mysten_network::config::Config::new();
            let mock = Self { sender };
            config
                .server_builder()
                .add_service(WorkerToPrimaryServer::new(mock))
                .bind(&address)
                .await
                .unwrap()
                .serve()
                .await
        });
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
    pub fn spawn(address: Multiaddr) -> Receiver<BincodeEncodedPayload> {
        let (sender, receiver) = channel(1);
        tokio::spawn(async move {
            let config = mysten_network::config::Config::new();
            let mock = Self { sender };
            config
                .server_builder()
                .add_service(PrimaryToWorkerServer::new(mock))
                .bind(&address)
                .await
                .unwrap()
                .serve()
                .await
        });
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

pub struct WorkerToWorkerMockServer {
    sender: Sender<BincodeEncodedPayload>,
}

impl WorkerToWorkerMockServer {
    pub fn spawn(address: Multiaddr) -> Receiver<BincodeEncodedPayload> {
        let (sender, receiver) = channel(1);
        tokio::spawn(async move {
            let config = mysten_network::config::Config::new();
            let mock = Self { sender };
            config
                .server_builder()
                .add_service(WorkerToWorkerServer::new(mock))
                .bind(&address)
                .await
                .unwrap()
                .serve()
                .await
        });
        receiver
    }
}

#[tonic::async_trait]
impl WorkerToWorker for WorkerToWorkerMockServer {
    async fn send_message(
        &self,
        request: tonic::Request<BincodeEncodedPayload>,
    ) -> Result<tonic::Response<Empty>, tonic::Status> {
        self.sender.send(request.into_inner()).await.unwrap();
        Ok(Response::new(Empty {}))
    }

    type ClientBatchRequestStream =
        Pin<Box<dyn Stream<Item = Result<BincodeEncodedPayload, tonic::Status>> + Send>>;

    async fn client_batch_request(
        &self,
        _request: tonic::Request<BincodeEncodedPayload>,
    ) -> Result<tonic::Response<Self::ClientBatchRequestStream>, tonic::Status> {
        todo!()
    }
}

// helper method to get a name and a committee.
pub fn resolve_name_and_committee() -> (Ed25519PublicKey, Committee<Ed25519PublicKey>) {
    let mut keys = keys(None);
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let kp = keys.pop().unwrap();
    let name = kp.public().clone();
    let committee = committee(None);

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
