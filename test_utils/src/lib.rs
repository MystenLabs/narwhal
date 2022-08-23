use arc_swap::ArcSwap;
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use config::{
    utils::get_available_port, Authority, Committee, Epoch, PrimaryAddresses, SharedWorkerCache,
    WorkerCache, WorkerId, WorkerIndex, WorkerInfo,
};
use crypto::PublicKey;
use crypto::{KeyPair, Signature};
use fastcrypto::{
    traits::{KeyPair as _, Signer as _},
    Digest, Hash as _,
};
use futures::Stream;
use indexmap::IndexMap;
use multiaddr::Multiaddr;
use rand::{rngs::StdRng, Rng, SeedableRng as _};
use std::{
    collections::{BTreeSet, VecDeque},
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

#[macro_export]
macro_rules! test_channel {
    ($e:expr) => {
        types::metered_channel::channel(
            $e,
            &prometheus::IntGauge::new("TEST_COUNTER", "test counter").unwrap(),
        );
    };
}

// Note: use the following macros to initialize your Primary / Consensus channels
// if your test is spawning a primary and you encounter an `AllReg` error.
//
// Rationale:
// The primary initialization will try to edit a specific metric in its registry
// for its new_certificates and committeed_certificates channel. The gauge situated
// in the channel you're passing as an argument to the primary initialization is
// the replacement. If that gauge is a dummy gauge, such as the one above, the
// initialization of the primary will panic (to protect the production code against
// an erroneous mistake in editing this bootstrap logic).
#[macro_export]
macro_rules! test_committed_certificates_channel {
    ($e:expr) => {
        types::metered_channel::channel(
            $e,
            &prometheus::IntGauge::new(
                primary::PrimaryChannelMetrics::NAME_COMMITTED_CERTS,
                primary::PrimaryChannelMetrics::DESC_COMMITTED_CERTS,
            )
            .unwrap(),
        );
    };
}

#[macro_export]
macro_rules! test_new_certificates_channel {
    ($e:expr) => {
        types::metered_channel::channel(
            $e,
            &prometheus::IntGauge::new(
                primary::PrimaryChannelMetrics::NAME_NEW_CERTS,
                primary::PrimaryChannelMetrics::DESC_NEW_CERTS,
            )
            .unwrap(),
        );
    };
}

#[macro_export]
macro_rules! test_get_block_commands {
    ($e:expr) => {
        types::metered_channel::channel(
            $e,
            &prometheus::IntGauge::new(
                primary::PrimaryChannelMetrics::NAME_GET_BLOCK_COMMANDS,
                primary::PrimaryChannelMetrics::DESC_GET_BLOCK_COMMANDS,
            )
            .unwrap(),
        );
    };
}

////////////////////////////////////////////////////////////////
/// Keys, Committee
////////////////////////////////////////////////////////////////

// Fixture
pub fn keys(rng_seed: impl Into<Option<u64>>) -> Vec<KeyPair> {
    let seed = rng_seed.into().unwrap_or(0u64).to_le_bytes();
    let mut rng_arg = [0u8; 32];
    for i in 0..4 {
        rng_arg[i * 8..(i + 1) * 8].copy_from_slice(&seed[..]);
    }

    let mut rng = StdRng::from_seed(rng_arg);
    (0..4).map(|_| KeyPair::generate(&mut rng)).collect()
}

// Fixture
pub fn committee(rng_seed: impl Into<Option<u64>>) -> Committee {
    pure_committee_from_keys(&keys(rng_seed))
}

pub fn pure_committee_from_keys(keys: &[KeyPair]) -> Committee {
    Committee {
        epoch: Epoch::default(),
        authorities: keys
            .iter()
            .map(|kp| (kp.public().clone(), make_authority()))
            .collect(),
    }
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

    Authority { stake: 1, primary }
}

pub fn make_authority() -> Authority {
    make_authority_with_port_getter(get_available_port)
}

// Fixture
pub fn shared_worker_cache(rng_seed: impl Into<Option<u64>>) -> SharedWorkerCache {
    shared_worker_cache_from_keys(&keys(rng_seed))
}

pub fn shared_worker_cache_from_keys(keys: &[KeyPair]) -> SharedWorkerCache {
    Arc::new(ArcSwap::from_pointee(worker_cache_from_keys(keys)))
}

pub fn worker_cache_from_keys(keys: &[KeyPair]) -> WorkerCache {
    WorkerCache {
        epoch: Epoch::default(),
        workers: keys
            .iter()
            .map(|kp| (kp.public().clone(), make_worker_index()))
            .collect(),
    }
}

pub fn make_worker_index() -> WorkerIndex {
    initialize_worker_index_with_port_getter(get_available_port)
}

pub fn initialize_worker_index_with_port_getter<F: FnMut() -> u16>(mut get_port: F) -> WorkerIndex {
    let workers = vec![
        (
            0,
            WorkerInfo {
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
            WorkerInfo {
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
            WorkerInfo {
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
            WorkerInfo {
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

    WorkerIndex(workers)
}

////////////////////////////////////////////////////////////////
/// Headers, Votes, Certificates
////////////////////////////////////////////////////////////////

// Fixture
pub fn mock_committee(keys: &[PublicKey]) -> Committee {
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
                    },
                )
            })
            .collect(),
    }
}

// Fixture
pub fn mock_worker_cache(keys: &[PublicKey]) -> WorkerCache {
    WorkerCache {
        epoch: Epoch::default(),
        workers: keys
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    WorkerIndex(
                        vec![(
                            0,
                            WorkerInfo {
                                primary_to_worker: "/ip4/0.0.0.0/tcp/0/http".parse().unwrap(),
                                transactions: "/ip4/0.0.0.0/tcp/0/http".parse().unwrap(),
                                worker_to_worker: "/ip4/0.0.0.0/tcp/0/http".parse().unwrap(),
                            },
                        )]
                        .iter()
                        .cloned()
                        .collect(),
                    ),
                )
            })
            .collect(),
    }
}

pub fn make_consensus_store(store_path: &std::path::Path) -> Arc<ConsensusStore> {
    const LAST_COMMITTED_CF: &str = "last_committed";
    const SEQUENCE_CF: &str = "sequence";

    let rocksdb = rocks::open_cf(store_path, None, &[LAST_COMMITTED_CF, SEQUENCE_CF])
        .expect("Failed creating database");

    let (last_committed_map, sequence_map) = reopen!(&rocksdb,
        LAST_COMMITTED_CF;<PublicKey, Round>,
        SEQUENCE_CF;<SequenceNumber, CertificateDigest>
    );

    Arc::new(ConsensusStore::new(last_committed_map, sequence_map))
}

// Fixture
pub fn header() -> Header {
    header_with_epoch(&committee(None))
}

// Fixture
pub fn headers() -> Vec<Header> {
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
pub fn header_with_epoch(committee: &Committee) -> Header {
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
pub fn fixture_header() -> Header {
    let kp = keys(None).pop().unwrap();

    fixture_header_builder().build(&kp).unwrap()
}

pub fn fixture_header_builder() -> types::HeaderBuilder {
    let kp = keys(None).pop().unwrap();

    let builder = types::HeaderBuilder::default();
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
) -> (Round, Vec<Header>) {
    let round = prior_round + 1;
    let next_headers: Vec<_> = keys(None)
        .into_iter()
        .map(|kp| {
            let builder = types::HeaderBuilder::default();
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

pub fn fixture_payload(number_of_batches: u8) -> IndexMap<BatchDigest, WorkerId> {
    let mut payload: IndexMap<BatchDigest, WorkerId> = IndexMap::new();

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

pub fn fixture_header_with_payload(number_of_batches: u8) -> Header {
    let kp = keys(None).pop().unwrap();
    let payload = fixture_payload(number_of_batches);

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
pub fn votes(header: &Header) -> Vec<Vote> {
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
                    signature: Signature::default(),
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
pub fn certificate(header: &Header) -> Certificate {
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

    async fn worker_info(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<BincodeEncodedPayload>, tonic::Status> {
        unimplemented!()
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

// helper method to get a name and a committee + worker_cache.
pub fn resolve_name_committee_and_worker_cache() -> (PublicKey, Committee, SharedWorkerCache) {
    let mut keys = keys(None);
    let committee = pure_committee_from_keys(&keys);
    let worker_cache = shared_worker_cache_from_keys(&keys);
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let kp = keys.pop().unwrap();
    let name = kp.public().clone();

    (name, committee, worker_cache)
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
    let message = worker::WorkerMessage::Batch(batch);
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
    keys: &[PublicKey],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
    make_certificates(range, initial_parents, keys, 0.0)
}

// Outputs rounds worth of certificates with optimal parents, signed
pub fn make_optimal_signed_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[KeyPair],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
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
    .flat_map(|(parenthood, parent)| parenthood.then_some(*parent))
    .collect::<BTreeSet<_>>()
}

// Utility for making several rounds worth of certificates through iterated parenthood sampling.
// The making of individual certificates once parents are figured out is delegated to the `make_one_certificate` argument
fn rounds_of_certificates(
    range: RangeInclusive<Round>,
    initial_parents: &BTreeSet<CertificateDigest>,
    keys: &[PublicKey],
    failure_probability: f64,
    make_one_certificate: impl Fn(
        PublicKey,
        Round,
        BTreeSet<CertificateDigest>,
    ) -> (CertificateDigest, Certificate),
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
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
    keys: &[PublicKey],
    failure_probability: f64,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
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
    keys: &[PublicKey],
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
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
    keys: &[KeyPair],
    failure_probability: f64,
) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>) {
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
    origin: PublicKey,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate) {
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
    origin: PublicKey,
    round: Round,
    epoch: Epoch,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate) {
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
    signers: &[KeyPair],
    origin: PublicKey,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate) {
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
