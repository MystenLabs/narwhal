// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{committee, keys, temp_dir};
use arc_swap::ArcSwap;
use config::{Committee, Parameters};
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use executor::SubscriberResult;
use node::{
    execution_state::SimpleExecutionState, metrics::primary_metrics_registry, Node, NodeStorage,
};
use prometheus::Registry;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{sync::mpsc::channel, task::JoinHandle};
use tracing::info;

#[allow(dead_code)]
pub struct NodeDetails {
    pub id: usize,
    pub name: Ed25519PublicKey,
    pub registry: Registry,
    handlers: SubscriberResult<Vec<JoinHandle<()>>>,
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

        for (index, key) in k.into_iter().enumerate() {
            info!("Key {index} -> {}", key.public().clone());
        }

        Self {
            nodes: HashMap::new(),
            committee_shared: Arc::new(ArcSwap::from_pointee(committee)),
            parameters: parameters.unwrap_or_else(Self::parameters),
        }
    }

    /// Starts a cluster by the defined number of validators.
    /// For each validator one primary and one worker node are
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
            let node = self.start_node(id).await;

            regs.push(node.unwrap());
        }

        regs
    }

    /// Starts a node by the defined id - if not already running - and
    /// the details are returned. If the node is already running then an
    /// error is returned instead.
    pub async fn start_node(&mut self, id: usize) -> Result<Arc<NodeDetails>, ()> {
        if self.nodes.contains_key(&id) {
            return Err(());
        }

        let mut k = keys(None);
        let keypair = k.remove(id);
        let name = keypair.public().clone();

        let registry = primary_metrics_registry(name.clone());

        // Make the data store.
        let store: NodeStorage<Ed25519PublicKey> = NodeStorage::reopen(temp_dir());

        // The channel returning the result for each transaction's execution.
        let (tx_transaction_confirmation, _) = channel(Node::CHANNEL_CAPACITY);

        let h = Node::spawn_primary(
            keypair,
            self.committee_shared.clone(),
            &store,
            self.parameters.clone(),
            /* consensus */ true,
            /* execution_state */ Arc::new(SimpleExecutionState),
            tx_transaction_confirmation,
            &registry,
        )
        .await;

        let node = Arc::new(NodeDetails {
            id,
            name,
            registry,
            handlers: h,
        });

        // Insert to the nodes map
        self.nodes.insert(id, node.clone());

        Ok(node)
    }

    /// This method stops a node by the provided id. The method will return
    /// either when the node has been successfully stopped or even when
    /// the node doesn't exist.
    pub fn stop_node(&mut self, id: usize) {
        if let Some(node) = self.nodes.remove(&id) {
            if let Ok(handlers) = &node.handlers {
                handlers.iter().for_each(|h| h.abort());
            }
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
