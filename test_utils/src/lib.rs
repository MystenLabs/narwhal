// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use config::{
    utils::get_available_port, Authority, Committee, Epoch, PrimaryAddresses, WorkerAddresses,
    WorkerId,
};
use crypto::{
    ed25519::{Ed25519KeyPair, Ed25519PublicKey, Ed25519Signature},
    traits::{KeyPair, Signer},
    Digest, Hash as _,
};
use futures::Stream;
use multiaddr::Multiaddr;
use rand::{rngs::StdRng, Rng, SeedableRng as _};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    ops::RangeInclusive,
    pin::Pin,
    sync::Arc,
};
use store::{reopen, rocks, rocks::DBMap, Store};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tonic::Response;
use types::{
    serialized_batch_digest, Batch, BatchDigest, BincodeEncodedPayload, Certificate,
    CertificateDigest, ConsensusStore, Empty, Header, HeaderBuilder, PrimaryToPrimary,
    PrimaryToPrimaryServer, PrimaryToWorker, PrimaryToWorkerServer, Round, SequenceNumber,
    Transaction, Vote, WorkerToPrimary, WorkerToPrimaryServer, WorkerToWorker,
    WorkerToWorkerServer,
};

pub mod cluster;

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
    committee_from_keys(&keys(rng_seed))
}
pub fn committee_from_keys(keys: &[Ed25519KeyPair]) -> Committee<Ed25519PublicKey> {
    pure_committee_from_keys(keys)
}

pub fn make_authority_with_port_getter<F: FnMut() -> u16>(mut get_port: F) -> Authority {
    let primary = PrimaryAddresses {
        primary_to_primary: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
            .parse()
            .unwrap(),
        worker_to_primary: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
            .parse()
            .unwrap(),
    };
    let workers = vec![
        (
            0,
            WorkerAddresses {
                primary_to_worker: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
                transactions: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
                worker_to_worker: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
            },
        ),
        (
            1,
            WorkerAddresses {
                primary_to_worker: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
                transactions: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
                worker_to_worker: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
            },
        ),
        (
            2,
            WorkerAddresses {
                primary_to_worker: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
                transactions: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
                worker_to_worker: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
            },
        ),
        (
            3,
            WorkerAddresses {
                primary_to_worker: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
                transactions: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
                worker_to_worker: format!("/ip4/127.0.0.1/tcp/{}/http", get_port())
                    .parse()
                    .unwrap(),
            },
        ),
    ]
    .iter()
    .cloned()
    .collect();

    Authority {
        stake: 1,
        primary,
        workers,
    }
}

pub fn make_authority() -> Authority {
    make_authority_with_port_getter(get_available_port)
}

pub fn pure_committee_from_keys(keys: &[Ed25519KeyPair]) -> Committee<Ed25519PublicKey> {
    Committee {
        epoch: Epoch::default(),
        authorities: keys
            .iter()
            .map(|kp| (kp.public().clone(), make_authority()))
            .collect(),
    }
}

////////////////////////////////////////////////////////////////
/// Headers, Votes, Certificates
////////////////////////////////////////////////////////////////

// Fixture
pub fn mock_committee(keys: &[Ed25519PublicKey]) -> Committee<Ed25519PublicKey> {
    Committee {
        epoch: Epoch::default(),
        authorities: keys
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: "/ip4/0.0.0.0/tcp/0/http".parse().unwrap(),
                            worker_to_primary: "/ip4/0.0.0.0/tcp/0/http".parse().unwrap(),
                        },
                        workers: HashMap::default(),
                    },
                )
            })
            .collect(),
    }
}

pub fn make_consensus_store(store_path: &std::path::Path) -> Arc<ConsensusStore<Ed25519PublicKey>> {
    const LAST_COMMITTED_CF: &str = "last_committed";
    const SEQUENCE_CF: &str = "sequence";

    let rocksdb = rocks::open_cf(store_path, None, &[LAST_COMMITTED_CF, SEQUENCE_CF])
        .expect("Failed creating database");

    let (last_committed_map, sequence_map) = reopen!(&rocksdb,
        LAST_COMMITTED_CF;<Ed25519PublicKey, Round>,
        SEQUENCE_CF;<SequenceNumber, CertificateDigest>
    );

    Arc::new(ConsensusStore::new(last_committed_map, sequence_map))
}

// Fixture
pub fn header() -> Header<Ed25519PublicKey> {
    header_with_epoch(&committee(None))
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

// Fixture
pub fn header_with_epoch(committee: &Committee<Ed25519PublicKey>) -> Header<Ed25519PublicKey> {
    let kp = keys(None).pop().unwrap();
    let header = Header {
        author: kp.public().clone(),
        round: 1,
        epoch: committee.epoch(),
        parents: Certificate::genesis(committee)
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

#[allow(dead_code)]
pub fn fixture_header() -> Header<Ed25519PublicKey> {
    let kp = keys(None).pop().unwrap();

    fixture_header_builder().build(&kp).unwrap()
}

pub fn fixture_header_builder() -> types::HeaderBuilder<Ed25519PublicKey> {
    let kp = keys(None).pop().unwrap();

    let builder = types::HeaderBuilder::<Ed25519PublicKey>::default();
    builder
        .author(kp.public().clone())
        .round(1)
        .epoch(0)
        .parents(
            Certificate::genesis(&committee(None))
                .iter()
                .map(|x| x.digest())
                .collect(),
        )
}

pub fn fixture_headers_round(
    prior_round: Round,
    parents: &BTreeSet<CertificateDigest>,
) -> (Round, Vec<Header<Ed25519PublicKey>>) {
    let round = prior_round + 1;
    let next_headers: Vec<_> = keys(None)
        .into_iter()
        .map(|kp| {
            let builder = types::HeaderBuilder::<Ed25519PublicKey>::default();
            builder
                .author(kp.public().clone())
                .round(round)
                .epoch(0)
                .parents(parents.clone())
                .with_payload_batch(fixture_batch_with_transactions(10), 0)
                .build(&kp)
                .unwrap()
        })
        .collect();
    (round, next_headers)
}

pub fn fixture_payload(number_of_batches: u8) -> BTreeMap<BatchDigest, WorkerId> {
    let mut payload: BTreeMap<BatchDigest, WorkerId> = BTreeMap::new();

    for i in 0..number_of_batches {
        let dummy_serialized_batch = vec![
            0u8, 0u8, 0u8, 0u8, // enum variant prefix
            1u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, // num txes
            5u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, // tx length
            10u8, 5u8, 8u8, 20u8, i, //tx
        ];
        let batch_digest = serialized_batch_digest(&dummy_serialized_batch).unwrap();

        payload.insert(batch_digest, 0);
    }

    payload
}

pub fn fixture_header_with_payload(number_of_batches: u8) -> Header<Ed25519PublicKey> {
    let kp = keys(None).pop().unwrap();
    let payload: BTreeMap<BatchDigest, WorkerId> = fixture_payload(number_of_batches);

    let builder = fixture_header_builder();
    builder.payload(payload).build(&kp).unwrap()
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
        .flat_map(|kp| {
            // we should not re-sign using the key of the authority
            // that produced the header
            if kp.public() == &header.author {
                None
            } else {
                let vote = Vote {
                    id: header.id,
                    round: header.round,
                    epoch: header.epoch,
                    origin: header.author.clone(),
                    author: kp.public().clone(),
                    signature: Ed25519Signature::default(),
                };
                Some(Vote {
                    signature: kp.sign(Digest::from(vote.digest()).as_ref()),
                    ..vote
                })
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
    serialized_batch_digest(&serialized_batch()).unwrap()
}

pub fn digest_batch(batch: Batch) -> BatchDigest {
    let serialized_batch = serialize_batch_message(batch);
    serialized_batch_digest(&serialized_batch).unwrap()
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

pub fn open_batch_store() -> Store<BatchDigest, types::SerializedBatchMessage> {
    let db = rocks::DBMap::<BatchDigest, types::SerializedBatchMessage>::open(
        temp_dir(),
        None,
        Some(BATCHES_CF),
    )
    .unwrap();
    Store::new(db)
}

// Creates one certificate per authority starting and finishing at the specified rounds (inclusive).
// Outputs a VecDeque of certificates (the certificate with higher round is on the front) and a set
// of digests to be used as parents for the certificates of the next round.
// Note : the certificates are unsigned
pub fn make_optimal_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[Ed25519PublicKey],
) -> (
    VecDeque<Certificate<Ed25519PublicKey>>,
    BTreeSet<CertificateDigest>,
) {
    make_certificates(range, initial_parents, keys, 0.0)
}

// Outputs rounds worth of certificates with optimal parents, signed
pub fn make_optimal_signed_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[Ed25519KeyPair],
) -> (
    VecDeque<Certificate<Ed25519PublicKey>>,
    BTreeSet<CertificateDigest>,
) {
    make_signed_certificates(range, initial_parents, keys, 0.0)
}

// Bernoulli-samples from a set of ancestors passed as a argument,
fn this_cert_parents(
    ancestors: &BTreeSet<CertificateDigest>,
    failure_prob: f64,
) -> BTreeSet<CertificateDigest> {
    std::iter::from_fn(|| {
        let f: f64 = rand::thread_rng().gen();
        Some(f > failure_prob)
    })
    .take(ancestors.len())
    .zip(ancestors)
    .flat_map(|(parenthood, parent)| parenthood.then(|| *parent))
    .collect::<BTreeSet<_>>()
}

// Utility for making several rounds worth of certificates through iterated parenthood sampling.
// The making of individial certificates once parents are figured out is delegated to the `make_one_certificate` argument
fn rounds_of_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[Ed25519PublicKey],
    failure_probability: f64,
    make_one_certificate: impl Fn(
        Ed25519PublicKey,
        Round,
        BTreeSet<CertificateDigest>,
    ) -> (CertificateDigest, Certificate<Ed25519PublicKey>),
) -> (
    VecDeque<Certificate<Ed25519PublicKey>>,
    BTreeSet<CertificateDigest>,
) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in range {
        next_parents.clear();
        for name in keys {
            let this_cert_parents = this_cert_parents(&parents, failure_probability);

            let (digest, certificate) =
                make_one_certificate(name.clone(), round, this_cert_parents);
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents = next_parents.clone();
    }
    (certificates, next_parents)
}

// make rounds worth of unsigned certificates with the sampled number of parents
pub fn make_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[Ed25519PublicKey],
    failure_probability: f64,
) -> (
    VecDeque<Certificate<Ed25519PublicKey>>,
    BTreeSet<CertificateDigest>,
) {
    rounds_of_certificates(
        range,
        initial_parents,
        keys,
        failure_probability,
        mock_certificate,
    )
}

// make rounds worth of unsigned certificates with the sampled number of parents
pub fn make_certificates_with_epoch(
    range: RangeInclusive<Round>,
    epoch: Epoch,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[Ed25519PublicKey],
) -> (
    VecDeque<Certificate<Ed25519PublicKey>>,
    BTreeSet<CertificateDigest>,
) {
    let mut certificates = VecDeque::new();
    let mut parents = initial_parents.iter().cloned().collect::<BTreeSet<_>>();
    let mut next_parents = BTreeSet::new();

    for round in range {
        next_parents.clear();
        for name in keys {
            let (digest, certificate) =
                mock_certificate_with_epoch(name.clone(), round, epoch, parents.clone());
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents = next_parents.clone();
    }
    (certificates, next_parents)
}

// make rounds worth of signed certificates with the sampled number of parents
pub fn make_signed_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[Ed25519KeyPair],
    failure_probability: f64,
) -> (
    VecDeque<Certificate<Ed25519PublicKey>>,
    BTreeSet<CertificateDigest>,
) {
    let public_keys = keys.iter().map(|k| k.public().clone()).collect::<Vec<_>>();
    let generator = |pk, round, parents| mock_signed_certificate(keys, pk, round, parents);

    rounds_of_certificates(
        range,
        initial_parents,
        &public_keys[..],
        failure_probability,
        generator,
    )
}

// Creates an unsigned certificate from its given round, origin and parents,
// Note: the certificate is unsigned
pub fn mock_certificate(
    origin: Ed25519PublicKey,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate<Ed25519PublicKey>) {
    let certificate = Certificate {
        header: Header {
            author: origin,
            round,
            parents,
            payload: fixture_payload(1),
            ..Header::default()
        },
        ..Certificate::default()
    };
    (certificate.digest(), certificate)
}

// Creates an unsigned certificate from its given round, epoch, origin, and parents,
// Note: the certificate is unsigned
pub fn mock_certificate_with_epoch(
    origin: Ed25519PublicKey,
    round: Round,
    epoch: Epoch,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate<Ed25519PublicKey>) {
    let certificate = Certificate {
        header: Header {
            author: origin,
            round,
            epoch,
            parents,
            payload: fixture_payload(1),
            ..Header::default()
        },
        ..Certificate::default()
    };
    (certificate.digest(), certificate)
}

// Creates one signed certificate from a set of signers - the signers must include the origin
pub fn mock_signed_certificate(
    signers: &[Ed25519KeyPair],
    origin: Ed25519PublicKey,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate<Ed25519PublicKey>) {
    let author = signers.iter().find(|kp| *kp.public() == origin).unwrap();
    let header_builder = HeaderBuilder::default()
        .author(origin.clone())
        .payload(fixture_payload(1))
        .round(round)
        .epoch(0)
        .parents(parents);
    let header = header_builder.build(author).unwrap();
    let mut cert = Certificate {
        header,
        ..Certificate::default()
    };

    for signer in signers {
        let pk = signer.public();
        let sig = signer
            .try_sign(Digest::from(cert.digest()).as_ref())
            .unwrap();
        cert.votes.push((pk.clone(), sig))
    }
    (cert.digest(), cert)
}
