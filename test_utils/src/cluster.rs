// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{committee, keys, temp_dir};
use arc_swap::ArcSwap;
use config::{Committee, Parameters, SharedCommittee, WorkerId};
use crypto::{
    ed25519::{Ed25519KeyPair, Ed25519PrivateKey, Ed25519PublicKey},
    traits::{KeyPair, ToFromBytes},
};
use multiaddr::Multiaddr;
use node::{
    execution_state::SimpleExecutionState,
    metrics::{primary_metrics_registry, worker_metrics_registry},
    Node, NodeStorage,
};
use prometheus::Registry;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{sync::mpsc::channel, task::JoinHandle};
use tracing::info;

#[derive(Clone)]
pub struct PrimaryNodeDetails {
    pub id: usize,
    pub key_pair: Arc<Ed25519KeyPair>,
    pub store_path: PathBuf,
    pub registry: Registry,
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
        Self {
            id,
            key_pair: Arc::new(key_pair),
            registry: Registry::new(),
            store_path: temp_dir(),
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
        let (tx_transaction_confirmation, _) = channel(Node::CHANNEL_CAPACITY);

        let pub_key = Ed25519PublicKey::from_bytes(self.key_pair.name.0.as_bytes()).unwrap();
        let private_key = Ed25519PrivateKey::from_bytes(self.key_pair.secret.0.as_bytes()).unwrap();

        let key_pair = Ed25519KeyPair {
            name: pub_key,
            secret: private_key,
        };

        // Primary node
        let primary_store: NodeStorage<Ed25519PublicKey> = NodeStorage::reopen(store_path.clone());
        let primary_handlers = Node::spawn_primary(
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

        self.handlers.swap(Arc::new(primary_handlers));
        self.store_path = store_path;
        self.registry = registry;
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

#[allow(dead_code)]
#[derive(Clone)]
pub struct AuthorityDetails {
    pub id: usize,
    pub name: Ed25519PublicKey,
    pub registry: Registry,
    pub primary: PrimaryNodeDetails,
    pub workers: HashMap<WorkerId, WorkerNodeDetails>,
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

        // Create all the workers - even if we don't intend to start them all
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

    pub fn stop_all(&self) {
        self.primary.stop();

        for (_, worker) in self.workers.iter() {
            worker.stop();
        }
    }
}

pub struct Cluster {
    authorities: HashMap<usize, AuthorityDetails>,
    committee_shared: Arc<ArcSwap<Committee<Ed25519PublicKey>>>,
    #[allow(dead_code)]
    parameters: Parameters,
}

impl Cluster {
    pub fn new(parameters: Option<Parameters>) -> Self {
        let committee = committee(None);
        let shared_committee = Arc::new(ArcSwap::from_pointee(committee));
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

    /// Returns the running authorities
    pub fn authorities(&mut self) -> Vec<AuthorityDetails> {
        self.authorities
            .iter()
            .filter(|(_, authority)| authority.is_running())
            .map(|(_, authority)| authority.clone())
            .collect()
    }

    pub fn authority(&mut self, id: usize) -> AuthorityDetails {
        self.authorities
            .get(&id)
            .unwrap_or_else(|| panic!("Authority with id {} not found", id))
            .clone()
    }

    /// Starts a cluster by the defined number of validators.
    /// For each validator one primary and one worker node are
    /// started.
    /// Returns a tuple for each spin up node with their id and
    /// the corresponding Registry
    pub async fn start(&mut self, nodes_number: usize) {
        if nodes_number > self.committee_shared.load().authorities.len() {
            panic!("Provided nodes number is greater than the maximum allowed");
        }

        for id in 0..nodes_number {
            info!("Spinning up node: {id}");
            self.start_node(id, false).await;
        }
    }

    /// Starts a node by the defined id - if not already running - and
    /// the details are returned. If the node is already running then an
    /// error is returned instead.
    /// When the preserve_store is true, then the started node will use the
    /// same path that has been used the last time when the node was started.
    /// This is basically a way to use the same storage between node restarts.
    /// When the preserve_store is false, then node will start with an empty
    /// storage.
    pub async fn start_node(&mut self, id: usize, preserve_store: bool) {
        let authority = self
            .authorities
            .get_mut(&id)
            .unwrap_or_else(|| panic!("Authority with id {} not found", id));

        // start the primary
        authority.start_primary(preserve_store).await;

        // start only the worker with id 0
        authority.start_worker(0, preserve_store).await;
    }

    /// This method stops a node by the provided id. The method will return
    /// either when the node has been successfully stopped or even when
    /// the node doesn't exist.
    pub fn stop_node(&mut self, id: usize) {
        if let Some(node) = self.authorities.get_mut(&id) {
            node.stop_all();
            info!("Aborted node for id {id}");
        } else {
            info!("Node with {id} not found - nothing to stop");
        }
    }

    fn parameters() -> Parameters {
        Parameters {
            batch_size: 200,
            max_header_delay: Duration::from_secs(2),
            ..Parameters::default()
        }
    }
}
