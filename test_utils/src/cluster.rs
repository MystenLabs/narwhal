// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{committee, keys, temp_dir};
use arc_swap::ArcSwap;
use config::{Committee, Parameters, SharedCommittee, WorkerId};
use crypto::{
    ed25519::{Ed25519KeyPair, Ed25519PrivateKey, Ed25519PublicKey},
    traits::{KeyPair, ToFromBytes},
};
use executor::{SerializedTransaction, SubscriberResult, DEFAULT_CHANNEL_SIZE};
use multiaddr::Multiaddr;
use node::{
    execution_state::SimpleExecutionState,
    metrics::{primary_metrics_registry, worker_metrics_registry},
    Node, NodeStorage,
};
use prometheus::Registry;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{broadcast::Sender, mpsc::channel},
    task::JoinHandle,
};
use tracing::info;

#[cfg(test)]
#[path = "tests/cluster_tests.rs"]
pub mod cluster_tests;

pub struct Cluster {
    authorities: HashMap<usize, AuthorityDetails>,
    committee_shared: Arc<ArcSwap<Committee<Ed25519PublicKey>>>,
    #[allow(dead_code)]
    parameters: Parameters,
}

impl Cluster {
    /// Initialises a new cluster by the provided parameters. The cluster will
    /// create all the authorities (primaries & workers) that are defined under
    /// the committee structure, but none of them will be started.
    /// If a committee is provided then this will be used, otherwise the default
    /// will be used instead.
    pub fn new(
        parameters: Option<Parameters>,
        input_committee: Option<Committee<Ed25519PublicKey>>,
    ) -> Self {
        let c = input_committee.unwrap_or_else(|| committee(None));
        let shared_committee = Arc::new(ArcSwap::from_pointee(c));
        let params = parameters.unwrap_or_else(Self::parameters);

        info!("###### Creating new cluster ######");
        info!("Validator keys:");
        let k = keys(None);
        let mut nodes = HashMap::new();

        for (id, key_pair) in k.into_iter().enumerate() {
            info!("Key {id} -> {}", key_pair.public().clone());

            let authority =
                AuthorityDetails::new(id, key_pair, params.clone(), shared_committee.clone());
            nodes.insert(id, authority);
        }

        Self {
            authorities: nodes,
            committee_shared: shared_committee,
            parameters: params,
        }
    }

    /// Starts a cluster by the defined number of authorities. The authorities
    /// will be started sequentially started from the one with id zero up to
    /// the provided number `authorities_number`. If none number is provided, then
    /// the maximum number of authorities will be started.
    /// If a number higher than the available ones in the committee is provided then
    /// the method will panic.
    /// The workers_per_authority dictates how many workers per authority should
    /// also be started (the same number will be started for each authority). If none
    /// is provided then the maximum number of workers will be started.
    pub async fn start(
        &mut self,
        authorities_number: Option<usize>,
        workers_per_authority: Option<usize>,
    ) {
        let max_authorities = self.committee_shared.load().authorities.len();
        let authorities = authorities_number.unwrap_or(max_authorities);

        if authorities > max_authorities {
            panic!("Provided nodes number is greater than the maximum allowed");
        }

        for id in 0..authorities {
            info!("Spinning up node: {id}");
            self.start_node(id, false, workers_per_authority).await;
        }
    }

    /// Starts the authority node by the defined id - if not already running - and
    /// the details are returned. If the node is already running then a panic
    /// is thrown instead.
    /// When the preserve_store is true, then the started authority will use the
    /// same path that has been used the last time when started (both the primary
    /// and the workers).
    /// This is basically a way to use the same storage between node restarts.
    /// When the preserve_store is false, then authority will start with an empty
    /// storage.
    /// If the `workers_per_authority` is provided then the corresponding number of
    /// workers will be started per authority. Otherwise if not provided, then maximum
    /// number of workers will be started per authority.
    pub async fn start_node(
        &mut self,
        id: usize,
        preserve_store: bool,
        workers_per_authority: Option<usize>,
    ) {
        let authority = self
            .authorities
            .get_mut(&id)
            .unwrap_or_else(|| panic!("Authority with id {} not found", id));

        // start the primary
        authority.start_primary(preserve_store).await;

        // start the workers
        if let Some(workers) = workers_per_authority {
            for worker_id in 0..workers {
                authority
                    .start_worker(worker_id as WorkerId, preserve_store)
                    .await;
            }
        } else {
            authority.start_all_workers(preserve_store).await;
        }
    }

    /// This method stops the authority (both the primary and the worker nodes)
    /// with the provided id.
    pub fn stop_node(&mut self, id: usize) {
        if let Some(node) = self.authorities.get_mut(&id) {
            node.stop_all();
            info!("Aborted node for id {id}");
        } else {
            info!("Node with {id} not found - nothing to stop");
        }
    }

    /// Returns all the running authorities. Any authority that:
    /// * has been started ever
    /// * or has been stopped
    /// will not be returned by this method.
    pub fn authorities(&mut self) -> Vec<AuthorityDetails> {
        self.authorities
            .iter()
            .filter(|(_, authority)| authority.is_running())
            .map(|(_, authority)| authority.clone())
            .collect()
    }

    /// Returns the authority identified by the provided id.
    /// Will panic if the authority with the id is not found.
    pub fn authority(&mut self, id: usize) -> AuthorityDetails {
        self.authorities
            .get(&id)
            .unwrap_or_else(|| panic!("Authority with id {} not found", id))
            .clone()
    }

    fn parameters() -> Parameters {
        Parameters {
            batch_size: 200,
            max_header_delay: Duration::from_secs(2),
            ..Parameters::default()
        }
    }
}

#[derive(Clone)]
pub struct PrimaryNodeDetails {
    pub id: usize,
    pub key_pair: Arc<Ed25519KeyPair>,
    pub store_path: PathBuf,
    pub registry: Registry,
    pub tx_transaction_confirmation: Sender<(SubscriberResult<Vec<u8>>, SerializedTransaction)>,
    committee: SharedCommittee<Ed25519PublicKey>,
    parameters: Parameters,
    handlers: Arc<ArcSwap<Vec<JoinHandle<()>>>>,
}

impl PrimaryNodeDetails {
    fn new(
        id: usize,
        key_pair: Ed25519KeyPair,
        parameters: Parameters,
        committee: SharedCommittee<Ed25519PublicKey>,
    ) -> Self {
        // used just to initialise the struct value
        let (tx, _) = tokio::sync::broadcast::channel(1);

        Self {
            id,
            key_pair: Arc::new(key_pair),
            registry: Registry::new(),
            store_path: temp_dir(),
            tx_transaction_confirmation: tx,
            committee,
            parameters,
            handlers: Arc::new(ArcSwap::from_pointee(Vec::new())),
        }
    }

    pub async fn start(&mut self, preserve_store: bool) {
        if self.is_running() {
            panic!("Tried to start a node that is already running");
        }

        let registry = primary_metrics_registry(self.key_pair.public().clone());

        // Make the data store.
        let store_path = if preserve_store {
            self.store_path.clone()
        } else {
            temp_dir()
        };

        info!(
            "Primary Node {} will use path {:?}",
            self.id,
            store_path.clone()
        );

        // The channel returning the result for each transaction's execution.
        let (tx_transaction_confirmation, mut rx_transaction_confirmation) =
            channel(Node::CHANNEL_CAPACITY);

        // KeyPair is not clonable - hackish way to reconstruct a new one.
        let pub_key = Ed25519PublicKey::from_bytes(self.key_pair.name.0.as_bytes()).unwrap();
        let private_key = Ed25519PrivateKey::from_bytes(self.key_pair.secret.0.as_bytes()).unwrap();

        let key_pair = Ed25519KeyPair {
            name: pub_key,
            secret: private_key,
        };

        // Primary node
        let primary_store: NodeStorage<Ed25519PublicKey> = NodeStorage::reopen(store_path.clone());
        let mut primary_handlers = Node::spawn_primary(
            key_pair,
            self.committee.clone(),
            &primary_store,
            self.parameters.clone(),
            /* consensus */ true,
            /* execution_state */ Arc::new(SimpleExecutionState),
            tx_transaction_confirmation,
            &registry,
        )
        .await
        .unwrap();

        let (tx, _) = tokio::sync::broadcast::channel(DEFAULT_CHANNEL_SIZE);
        let transactions_sender = tx.clone();
        // spawn a task to listen on the committed transactions
        // and translate to a mpmc channel
        let h = tokio::spawn(async move {
            while let Some(t) = rx_transaction_confirmation.recv().await {
                // send the transaction to the mpmc channel
                transactions_sender
                    .send(t)
                    .expect("Couldn't send message to broadcast channel");
            }
        });

        // add the tasks's handle to the primary's handle so can be shutdown
        // with the others.
        primary_handlers.push(h);

        self.handlers.swap(Arc::new(primary_handlers));
        self.store_path = store_path;
        self.registry = registry;
        self.tx_transaction_confirmation = tx;
    }

    pub fn stop(&self) {
        self.handlers.load().iter().for_each(|h| h.abort());
        info!("Aborted primary node for id {}", self.id);
    }

    /// This method returns whether the node is still running or not. We
    /// iterate over all the handlers and check whether there is still any
    /// that is not finished. If we find at least one, then we report the
    /// node as still running.
    fn is_running(&self) -> bool {
        if self.handlers.load().is_empty() {
            return false;
        }

        self.handlers.load().iter().any(|h| !h.is_finished())
    }
}

#[derive(Clone)]
pub struct WorkerNodeDetails {
    pub id: WorkerId,
    pub transactions_address: Multiaddr,
    pub registry: Registry,
    name: Ed25519PublicKey,
    committee: SharedCommittee<Ed25519PublicKey>,
    parameters: Parameters,
    store_path: PathBuf,
    handlers: Arc<ArcSwap<Vec<JoinHandle<()>>>>,
}

impl WorkerNodeDetails {
    fn new(
        id: WorkerId,
        name: Ed25519PublicKey,
        parameters: Parameters,
        transactions_address: Multiaddr,
        committee: SharedCommittee<Ed25519PublicKey>,
    ) -> Self {
        Self {
            id,
            name,
            registry: Registry::new(),
            store_path: temp_dir(),
            transactions_address,
            committee,
            parameters,
            handlers: Arc::new(ArcSwap::from_pointee(Vec::new())),
        }
    }

    /// Starts the node. When preserve_store is true then the last used
    pub async fn start(&mut self, preserve_store: bool) {
        if self.is_running() {
            panic!(
                "Worker with id {} is already running, can't start again",
                self.id
            );
        }

        let registry = worker_metrics_registry(self.id, self.name.clone());

        // Make the data store.
        let store_path = if preserve_store {
            self.store_path.clone()
        } else {
            temp_dir()
        };

        let worker_store = NodeStorage::reopen(store_path.clone());
        let worker_handlers = Node::spawn_workers(
            self.name.clone(),
            vec![self.id],
            self.committee.clone(),
            &worker_store,
            self.parameters.clone(),
            &registry,
        );

        self.handlers.swap(Arc::new(worker_handlers));
        self.store_path = store_path;
        self.registry = registry;
    }

    pub fn stop(&self) {
        self.handlers.load().iter().for_each(|h| h.abort());
        info!("Aborted worker node for id {}", self.id);
    }

    /// This method returns whether the node is still running or not. We
    /// iterate over all the handlers and check whether there is still any
    /// that is not finished. If we find at least one, then we report the
    /// node as still running.
    fn is_running(&self) -> bool {
        self.handlers.load().iter().any(|h| !h.is_finished())
    }
}

/// The authority details hold all the necessary structs and details
/// to identify and manage a specific authority. An authority is
/// composed of its primary node and the worker nodes. Via this struct
/// we can manage the nodes one by one or in batch fashion (ex stop_all).
#[allow(dead_code)]
#[derive(Clone)]
pub struct AuthorityDetails {
    pub id: usize,
    pub name: Ed25519PublicKey,
    pub registry: Registry,
    pub primary: PrimaryNodeDetails,
    workers: HashMap<WorkerId, WorkerNodeDetails>,
}

impl AuthorityDetails {
    pub fn new(
        id: usize,
        key_pair: Ed25519KeyPair,
        parameters: Parameters,
        committee: SharedCommittee<Ed25519PublicKey>,
    ) -> Self {
        // Create all the nodes we have in the committee
        let name = key_pair.public().clone();
        let primary = PrimaryNodeDetails::new(id, key_pair, parameters.clone(), committee.clone());

        // Create all the workers - even if we don't intend to start them all. Those
        // act as place holder setups. That gives us the power in a clear way manage
        // the nodes independently.
        let mut workers = HashMap::new();
        for (worker_id, addresses) in committee
            .load()
            .authorities
            .get(&name)
            .unwrap()
            .workers
            .clone()
        {
            let worker = WorkerNodeDetails::new(
                worker_id,
                name.clone(),
                parameters.clone(),
                addresses.transactions.clone(),
                committee.clone(),
            );
            workers.insert(worker_id, worker);
        }

        Self {
            id,
            name,
            registry: Registry::new(),
            primary,
            workers,
        }
    }

    /// This method will return true either when the primary or any of
    /// the workers is running. In order to make sure that we don't end up
    /// in intermediate states we want to make sure that everything has
    /// stopped before we report something as not running (in case we want
    /// to start them again).
    fn is_running(&self) -> bool {
        if self.primary.is_running() {
            return true;
        }

        for (_, worker) in self.workers.iter() {
            if worker.is_running() {
                return true;
            }
        }
        false
    }

    pub async fn start_primary(&mut self, preserve_store: bool) {
        self.primary.start(preserve_store).await;
    }

    pub fn stop_primary(&self) {
        self.primary.stop();
    }

    pub async fn start_all_workers(&mut self, preserve_store: bool) {
        for (_, worker) in self.workers.iter_mut() {
            worker.start(preserve_store).await;
        }
    }

    pub async fn start_worker(&mut self, id: WorkerId, preserve_store: bool) {
        self.workers
            .get_mut(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id))
            .start(preserve_store)
            .await;
    }

    pub fn stop_worker(&self, id: WorkerId) {
        self.workers
            .get(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id))
            .stop();
    }

    /// Stops all the nodes (primary & workers)
    pub fn stop_all(&self) {
        self.primary.stop();

        for (_, worker) in self.workers.iter() {
            worker.stop();
        }
    }

    /// Returns the worker with the provided id. If not found then a panic
    /// is raised instead.
    pub fn worker(&self, id: WorkerId) -> WorkerNodeDetails {
        self.workers
            .get(&id)
            .unwrap_or_else(|| panic!("Worker with id {} not found ", id))
            .clone()
    }

    /// Helper method to return transaction addresses of
    /// all the worker nodes.
    /// Important: only the addresses of the running workers will
    /// be returned.
    pub fn worker_transaction_addresses(&self) -> Vec<Multiaddr> {
        self.workers
            .iter()
            .filter_map(|(_, worker)| {
                if worker.is_running() {
                    Some(worker.transactions_address.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}
