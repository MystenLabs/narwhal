// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

use crypto::traits::{EncodeDecodeBase64, VerifyingKey};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, OpenOptions},
    io::{BufWriter, Write as _},
    net::SocketAddr,
    ops::Deref,
    time::Duration,
};
use thiserror::Error;
use tracing::info;

mod duration_format;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(String),

    #[error("Unknown worker id {0}")]
    UnknownWorker(WorkerId),

    #[error("Failed to read config file '{file}': {message}")]
    ImportError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    ExportError { file: String, message: String },
}

pub trait Import: DeserializeOwned {
    fn import(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ImportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

impl<D: DeserializeOwned> Import for D {}

pub trait Export: Serialize {
    fn export(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::ExportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

impl<S: Serialize> Export for S {}

pub type Stake = u32;
pub type WorkerId = u32;

#[derive(Deserialize, Clone)]
/// Holds all the node properties. An example is provided to
/// showcase the usage and deserialization from a json file.
/// To define a Duration on the property file can use either
/// miliseconds or seconds (e.x 5s, 10ms , 2000ms).
pub struct Parameters {
    /// The preferred header size. The primary creates a new header when it has enough parents and
    /// enough batches' digests to reach `header_size`. Denominated in bytes.
    pub header_size: usize,
    /// The maximum delay that the primary waits between generating two headers, even if the header
    /// did not reach `max_header_size`.
    #[serde(with = "duration_format")]
    pub max_header_delay: Duration,
    /// The depth of the garbage collection (Denominated in number of rounds).
    pub gc_depth: u64,
    /// The delay after which the synchronizer retries to send sync requests. Denominated in ms.
    #[serde(with = "duration_format")]
    pub sync_retry_delay: Duration,
    /// Determine with how many nodes to sync when re-trying to send sync-request. These nodes
    /// are picked at random from the committee.
    pub sync_retry_nodes: usize,
    /// The preferred batch size. The workers seal a batch of transactions when it reaches this size.
    /// Denominated in bytes.
    pub batch_size: usize,
    /// The delay after which the workers seal a batch of transactions, even if `max_batch_size`
    /// is not reached.
    #[serde(with = "duration_format")]
    pub max_batch_delay: Duration,
    /// The parameters for the block synchronizer
    pub block_synchronizer: BlockSynchronizerParameters,
    /// The parameters for gRPC server
    pub grpc_server: GrpcServerParameters,
    /// The maximum number of concurrent requests for messages accepted from an un-trusted entity
    pub max_concurrent_requests: usize,
}

#[derive(Deserialize, Clone)]
pub struct GrpcServerParameters {
    /// Socket address the server should be listening to.
    pub socket_addr: String,
    /// The timeout configuration when requesting batches from workers.
    #[serde(with = "duration_format")]
    pub get_collections_timeout: Duration,
}

impl Default for GrpcServerParameters {
    fn default() -> Self {
        Self {
            socket_addr: "127.0.0.1:50052".to_string(),
            get_collections_timeout: Duration::from_millis(5_000),
        }
    }
}

#[derive(Deserialize, Clone)]
pub struct BlockSynchronizerParameters {
    /// The timeout configuration when requesting certificates from peers.
    #[serde(with = "duration_format")]
    pub certificates_synchronize_timeout: Duration,
    /// Timeout when has requested the payload for a certificate and is
    /// waiting to receive them.
    #[serde(with = "duration_format")]
    pub payload_synchronize_timeout: Duration,
    /// The timeout configuration when for when we ask the other peers to
    /// discover who has the payload available for the dictated certificates.
    #[serde(with = "duration_format")]
    pub payload_availability_timeout: Duration,
}

impl Default for BlockSynchronizerParameters {
    fn default() -> Self {
        Self {
            certificates_synchronize_timeout: Duration::from_millis(2_000),
            payload_synchronize_timeout: Duration::from_millis(2_000),
            payload_availability_timeout: Duration::from_millis(2_000),
        }
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            header_size: 1_000,
            max_header_delay: Duration::from_millis(100),
            gc_depth: 50,
            sync_retry_delay: Duration::from_millis(5_000),
            sync_retry_nodes: 3,
            batch_size: 500_000,
            max_batch_delay: Duration::from_millis(100),
            block_synchronizer: BlockSynchronizerParameters::default(),
            grpc_server: GrpcServerParameters::default(),
            max_concurrent_requests: 500_000,
        }
    }
}

impl Parameters {
    pub fn tracing(&self) {
        info!("Header size set to {} B", self.header_size);
        info!(
            "Max header delay set to {} ms",
            self.max_header_delay.as_millis()
        );
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!(
            "Sync retry delay set to {} ms",
            self.sync_retry_delay.as_millis()
        );
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Batch size set to {} B", self.batch_size);
        info!(
            "Max batch delay set to {} ms",
            self.max_batch_delay.as_millis()
        );
        info!(
            "Synchronize certificates timeout set to {} ms",
            self.block_synchronizer
                .certificates_synchronize_timeout
                .as_millis()
        );
        info!(
            "Payload (batches) availability timeout set to {} ms",
            self.block_synchronizer
                .payload_availability_timeout
                .as_millis()
        );
        info!(
            "Synchronize payload (batches) timeout set to {} ms",
            self.block_synchronizer
                .payload_synchronize_timeout
                .as_millis()
        );
        info!("gRPC Server listening on {}", self.grpc_server.socket_addr);
        info!(
            "Get collections timeout set to {} ms",
            self.grpc_server.get_collections_timeout.as_millis()
        );
        info!(
            "Max concurrent requests set to {}",
            self.max_concurrent_requests
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PrimaryAddresses {
    /// Address to receive messages from other primaries (WAN).
    pub primary_to_primary: SocketAddr,
    /// Address to receive messages from our workers (LAN).
    pub worker_to_primary: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize, Eq, Hash, PartialEq)]
pub struct WorkerAddresses {
    /// Address to receive client transactions (WAN).
    pub transactions: SocketAddr,
    /// Address to receive messages from other workers (WAN).
    pub worker_to_worker: SocketAddr,
    /// Address to receive messages from our primary (LAN).
    pub primary_to_worker: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Authority {
    /// The voting power of this authority.
    pub stake: Stake,
    /// The network addresses of the primary.
    pub primary: PrimaryAddresses,
    /// Map of workers' id and their network addresses.
    pub workers: HashMap<WorkerId, WorkerAddresses>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(bound(deserialize = "PublicKey: DeserializeOwned"))]
pub struct Committee<PublicKey: VerifyingKey> {
    pub authorities: BTreeMap<PublicKey, Authority>,
}

impl<PublicKey: VerifyingKey> Committee<PublicKey> {
    /// Returns the number of authorities.
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    /// Return the stake of a specific authority.
    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities
            .get(&name.clone())
            .map_or_else(|| 0, |x| x.stake)
    }

    /// Returns the stake of all authorities except `myself`.
    pub fn others_stake(&self, myself: &PublicKey) -> Vec<(PublicKey, Stake)> {
        self.authorities
            .iter()
            .filter(|(name, _)| *name != myself)
            .map(|(name, authority)| (name.deref().clone(), authority.stake))
            .collect()
    }

    /// Returns the stake required to reach a quorum (2f+1).
    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        (total_votes + 2) / 3
    }

    /// Returns the primary addresses of the target primary.
    pub fn primary(&self, to: &PublicKey) -> Result<PrimaryAddresses, ConfigError> {
        self.authorities
            .get(&to.clone())
            .map(|x| x.primary.clone())
            .ok_or_else(|| ConfigError::NotInCommittee((*to).encode_base64()))
    }

    /// Returns the addresses of all primaries except `myself`.
    pub fn others_primaries(&self, myself: &PublicKey) -> Vec<(PublicKey, PrimaryAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| *name != myself)
            .map(|(name, authority)| (name.deref().clone(), authority.primary.clone()))
            .collect()
    }

    /// Returns the addresses of a specific worker (`id`) of a specific authority (`to`).
    pub fn worker(&self, to: &PublicKey, id: &WorkerId) -> Result<WorkerAddresses, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| *name == to)
            .map(|(_, authority)| authority)
            .ok_or_else(|| {
                ConfigError::NotInCommittee(ToString::to_string(&(*to).encode_base64()))
            })?
            .workers
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or_else(|| ConfigError::NotInCommittee((*to).encode_base64()))
    }

    /// Returns the addresses of all our workers.
    pub fn our_workers(&self, myself: &PublicKey) -> Result<Vec<WorkerAddresses>, ConfigError> {
        let res = self
            .authorities
            .iter()
            .find(|(name, _)| *name == myself)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee((*myself).encode_base64()))?
            .workers
            .values()
            .cloned()
            .collect();
        Ok(res)
    }

    /// Returns the addresses of all workers with a specific id except the ones of the authority
    /// specified by `myself`.
    pub fn others_workers(
        &self,
        myself: &PublicKey,
        id: &WorkerId,
    ) -> Vec<(PublicKey, WorkerAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| *name != myself)
            .filter_map(|(name, authority)| {
                authority
                    .workers
                    .iter()
                    .find(|(worker_id, _)| worker_id == &id)
                    .map(|(_, addresses)| (name.deref().clone(), addresses.clone()))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::{Import, Parameters};
    use std::{fs::File, io::Write};
    use tempfile::tempdir;
    use tracing_test::traced_test;

    #[test]
    #[traced_test]
    fn parse_properties() {
        // GIVEN
        let input = r#"{
             "header_size": 1000,
             "max_header_delay": "100ms",
             "gc_depth": 50,
             "sync_retry_delay": "5s",
             "sync_retry_nodes": 3,
             "batch_size": 500000,
             "max_batch_delay": "100ms",
             "block_synchronizer": {
                 "certificates_synchronize_timeout": "2s",
                 "payload_synchronize_timeout": "3_000ms",
                 "payload_availability_timeout": "4_000ms"
             },
             "grpc_server": {
                 "socket_addr": "127.0.0.1:50052",
                 "get_collections_timeout": "5_000ms"
             },
             "max_concurrent_requests": 500000
          }"#;

        // AND temporary file
        let dir = tempdir().expect("Couldn't create tempdir");

        let file_path = dir.path().join("temp-properties.json");
        let mut file = File::create(file_path.clone()).expect("Couldn't create temp file");

        // AND write the json context
        writeln!(file, "{input}").expect("Couldn't write to file");

        // WHEN
        let params = Parameters::import(file_path.to_str().unwrap()).expect("Error raised");

        // THEN
        assert_eq!(params.sync_retry_delay.as_millis(), 5_000);
        assert_eq!(
            params
                .block_synchronizer
                .certificates_synchronize_timeout
                .as_millis(),
            2_000
        );
        assert_eq!(
            params
                .block_synchronizer
                .payload_synchronize_timeout
                .as_millis(),
            3_000
        );
        assert_eq!(
            params
                .block_synchronizer
                .payload_availability_timeout
                .as_millis(),
            4_000
        );
        assert_eq!(params.grpc_server.socket_addr, "127.0.0.1:50052");
        assert_eq!(
            params.grpc_server.get_collections_timeout.as_millis(),
            5_000
        );
    }

    #[test]
    #[traced_test]
    fn tracing_should_print_parameters() {
        // GIVEN
        let parameters = Parameters::default();

        // WHEN
        parameters.tracing();

        // THEN
        assert!(logs_contain("Header size set to 1000 B"));
        assert!(logs_contain("Max header delay set to 100 ms"));
        assert!(logs_contain("Garbage collection depth set to 50 rounds"));
        assert!(logs_contain("Sync retry delay set to 5000 ms"));
        assert!(logs_contain("Sync retry nodes set to 3 nodes"));
        assert!(logs_contain("Batch size set to 500000 B"));
        assert!(logs_contain("Max batch delay set to 100 ms"));
        assert!(logs_contain(
            "Synchronize certificates timeout set to 2000 ms"
        ));
        assert!(logs_contain(
            "Payload (batches) availability timeout set to 2000 ms"
        ));
        assert!(logs_contain(
            "Synchronize payload (batches) timeout set to 2000 ms"
        ));
        assert!(logs_contain("gRPC Server listening on 127.0.0.1:50052"));
        assert!(logs_contain("Get collections timeout set to 5000 ms"));
        assert!(logs_contain("Max concurrent requests set to 500000"))
    }
}
