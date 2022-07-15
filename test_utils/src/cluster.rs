// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{committee, keys, temp_dir};
use arc_swap::ArcSwap;
use config::{Committee, Parameters};
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use executor::{SerializedTransaction, SubscriberResult};
use multiaddr::Multiaddr;
use node::{
    execution_state::SimpleExecutionState, metrics::primary_metrics_registry, Node, NodeStorage,
};
use prometheus::Registry;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::mpsc::Receiver;
use tokio::{sync::mpsc::channel, task::JoinHandle};
use tracing::info;

#[allow(dead_code)]
pub struct NodeDetails {
    pub id: usize,
    pub name: Ed25519PublicKey,
    pub registry: Registry,
    pub store_path: PathBuf,
    handlers: Vec<JoinHandle<()>>,
    pub is_primary: bool,
    pub tr_transaction_confirmation:
        Option<Receiver<(SubscriberResult<Vec<u8>>, SerializedTransaction)>>,
    pub transaction_addr: Option<Multiaddr>,
}

impl NodeDetails {
    /// This method returns whether the node is still running or not. We
    /// iterate over all the handlers and check whether there is still any
    /// that is not finished. If we find at least one, then we report the
    /// node as still running.
    fn is_running(&self) -> bool {
        self.handlers.iter().any(|h| !h.is_finished())
    }
}

pub struct Cluster {
    nodes: HashMap<usize, Arc<NodeDetails>>,
    committee_shared: Arc<ArcSwap<Committee<Ed25519PublicKey>>>,
    parameters: Parameters,
}

impl Cluster {
    pub fn new(parameters: Option<Parameters>) -> Self {
        let committee = committee(None);

        info!("###### Creating new cluster ######");
        info!("Validator keys:");
        let k = keys(None);
        let mut nodes = HashMap::new();

        for (index, key) in k.into_iter().enumerate() {
            info!("Key {index} -> {}", key.public().clone());

            nodes.insert(
                index,
                Arc::new(NodeDetails {
                    id: index,
                    name: key.public().clone(),
                    registry: Registry::new(),
                    store_path: Default::default(),
                    handlers: vec![],
                    is_primary: true,
                    tr_transaction_confirmation: None,
                    transaction_addr: None,
                }),
            );
        }

        Self {
            nodes,
            committee_shared: Arc::new(ArcSwap::from_pointee(committee)),
            parameters: parameters.unwrap_or_else(Self::parameters),
        }
    }

    /// Starts a cluster by the defined number of validators.
    /// For each validator one primary node is
    /// started.
    /// Returns a tuple for each spin up node with their id and
    /// the corresponding Registry
    pub async fn start(&mut self, nodes_number: usize) -> Vec<Arc<NodeDetails>> {
        let mut regs = Vec::new();

        if nodes_number > self.committee_shared.load().authorities.len() {
            panic!("Provided nodes number is greater than the maximum allowed");
        }

        for id in 0..nodes_number {
            info!("Spinning up node: {id}");
            let node = self.start_node(id, true, false).await.unwrap();

            regs.push(node);
        }

        regs
    }

    /// Starts a cluster by the defined number of validators.
    /// For each validator one primary node is
    /// started, and a single worker node is also started.
    /// Returns a tuple for each spin up node with their id and
    /// the corresponding Registry
    pub async fn start_with_worker(
        &mut self,
        nodes_number: usize,
        preserve_store: bool,
    ) -> (Vec<Arc<NodeDetails>>, Arc<NodeDetails>) {
        let mut regs = Vec::new();

        if nodes_number > self.committee_shared.load().authorities.len() {
            panic!("Provided nodes number is greater than the maximum allowed");
        }

        for id in 0..nodes_number {
            info!("Spinning up node: {id}");
            let node = self.start_node(id, true, false).await.unwrap();

            regs.push(node);
        }

        // spin up one worker
        info!("Spinning up worker node");
        let worker = self
            .start_node(nodes_number, false, preserve_store)
            .await
            .unwrap();


        (regs, worker)
    }

    /// Starts a node by the defined id - if not already running - and
    /// the details are returned. If the node is already running then an
    /// error is returned instead.
    /// When the preserve_store is true then the started node will use the
    /// same path that has been used the last time when the node was started.
    /// This is basically a way to use the same storage between node restarts.
    pub async fn start_node(
        &mut self,
        id: usize,
        is_primary: bool,
        preserve_store: bool,
    ) -> Result<Arc<NodeDetails>, ()> {
        let node = self.nodes.get(&id).unwrap();

        if node.is_running() {
            return Err(());
        }

        let mut k = keys(None);
        let keypair = k.remove(id);
        let name = keypair.public().clone();

        let registry = primary_metrics_registry(name.clone());

        // Make the data store.
        let store_path = if preserve_store {
            node.store_path.clone()
        } else {
            temp_dir()
        };

        info!("Node {} will use path {:?}", id, store_path.clone());

        let store: NodeStorage<Ed25519PublicKey> = NodeStorage::reopen(store_path.clone());

        // The channel returning the result for each transaction's execution.
        let (tx_transaction_confirmation, tr_transaction_confirmation) =
            channel(Node::CHANNEL_CAPACITY);

        let h: SubscriberResult<Vec<JoinHandle<()>>> = if is_primary {
            Node::spawn_primary(
                keypair,
                self.committee_shared.clone(),
                &store,
                self.parameters.clone(),
                /* consensus */ true,
                /* execution_state */ Arc::new(SimpleExecutionState),
                tx_transaction_confirmation,
                &registry,
            )
            .await
        } else {
            Ok(Node::spawn_workers(
                name.clone(),
                vec![id as u32],
                self.committee_shared.clone(),
                &store,
                self.parameters.clone(),
                &registry,
            ))
        };
        let transaction_addr = Some(
            self.committee_shared
                .load()
                .authorities
                .iter()
                .next()
                .unwrap()
                .1
                .workers
                .get(&0)
                .unwrap()
                .transactions
                .clone(),
        );
        let node: Arc<NodeDetails> = if is_primary {
            Arc::new(NodeDetails {
                id,
                name,
                registry,
                handlers: h.unwrap(),
                store_path,
                is_primary,
                tr_transaction_confirmation: Some(tr_transaction_confirmation),
                transaction_addr: None,
            })
        } else {
            Arc::new(NodeDetails {
                id,
                name,
                registry,
                handlers: h.unwrap(),
                store_path,
                is_primary,
                tr_transaction_confirmation: None,
                transaction_addr,
            })
        };

        // Insert to the nodes map
        self.nodes.insert(id, node.clone());

        Ok(node)
    }

    /// This method stops a node by the provided id. The method will return
    /// either when the node has been successfully stopped or even when
    /// the node doesn't exist.
    pub fn stop_node(&mut self, id: usize) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.handlers.iter().for_each(|h| h.abort());
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
