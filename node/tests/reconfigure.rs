// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use arc_swap::ArcSwap;
use bytes::Bytes;
use config::{Committee, Parameters};
use consensus::ConsensusOutput;
use crypto::{
    ed25519::{Ed25519KeyPair, Ed25519PublicKey},
    traits::{KeyPair, Signer, VerifyingKey},
};
use executor::{ExecutionIndices, ExecutionState, ExecutionStateError, ExecutorOutput};
use futures::future::join_all;
use network::{PrimaryNetwork, PrimaryToWorkerNetwork};
use node::{Node, NodeStorage};
use primary::PrimaryWorkerMessage;
use prometheus::Registry;
use std::{
    fmt::Debug,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use test_utils::{committee, keys};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::{interval, sleep, Duration, MissedTickBehavior},
};
use types::{PrimaryMessage, ReconfigureNotification, TransactionProto, TransactionsClient};

/// A simple/dumb execution engine.
struct SimpleExecutionState {
    index: usize,
    tx_reconfigure: Sender<(Ed25519KeyPair, Committee<Ed25519PublicKey>)>,
    epoch: AtomicU64,
}

impl SimpleExecutionState {
    pub fn new(
        index: usize,
        tx_reconfigure: Sender<(Ed25519KeyPair, Committee<Ed25519PublicKey>)>,
    ) -> Self {
        Self {
            index,
            tx_reconfigure,
            epoch: AtomicU64::new(0),
        }
    }
}

#[async_trait::async_trait]
impl ExecutionState for SimpleExecutionState {
    type Transaction = u64;
    type Error = SimpleExecutionError;
    type Outcome = u64;

    async fn handle_consensus_transaction<PublicKey: VerifyingKey>(
        &self,
        _consensus_output: &ConsensusOutput<PublicKey>,
        execution_indices: ExecutionIndices,
        transaction: Self::Transaction,
    ) -> Result<(Self::Outcome, Option<Committee<PublicKey>>), Self::Error> {
        println!("tx = {transaction:?}");

        // Change epoch every few certificates. Note that empty certificates are not provided to
        // this function (they are immediately skipped).
        if transaction >= self.epoch.load(Ordering::Relaxed)
            && execution_indices.next_certificate_index % 3 == 0
        {
            self.epoch.fetch_add(1, Ordering::Relaxed);

            let keypair = keys(None)
                .into_iter()
                .enumerate()
                .filter(|(i, _)| i == &self.index)
                .map(|(_, x)| x)
                .collect::<Vec<_>>()
                .pop()
                .unwrap();
            let mut committee = committee(None);
            committee.epoch = self.epoch.load(Ordering::Relaxed);
            println!(
                "[{}] Moved to E{}",
                keypair.public().clone(),
                committee.epoch()
            );

            self.tx_reconfigure
                .send((keypair, committee))
                .await
                .unwrap();
        }

        Ok((self.epoch.load(Ordering::Relaxed), None))
    }

    fn ask_consensus_write_lock(&self) -> bool {
        true
    }

    fn release_consensus_write_lock(&self) {}

    async fn load_execution_indices(&self) -> Result<ExecutionIndices, Self::Error> {
        Ok(ExecutionIndices::default())
    }
}

/// A simple/dumb execution error.
#[derive(Debug, thiserror::Error)]
pub enum SimpleExecutionError {
    #[error("Something went wrong in the authority")]
    ServerError,

    #[error("The client made something bad")]
    ClientError,
}

#[async_trait::async_trait]
impl ExecutionStateError for SimpleExecutionError {
    fn node_error(&self) -> bool {
        match self {
            Self::ServerError => true,
            Self::ClientError => false,
        }
    }

    fn to_string(&self) -> String {
        ToString::to_string(&self)
    }
}

async fn run_node<Keys, PublicKey, State>(
    keypair: Keys,
    committee: &Committee<PublicKey>,
    execution_state: Arc<State>,
    parameters: Parameters,
    mut rx_reconfigure: Receiver<(Keys, Committee<PublicKey>)>,
    tx_output: Sender<ExecutorOutput<State>>,
) where
    PublicKey: VerifyingKey,
    State: ExecutionState + Send + Sync + 'static,
    State::Outcome: Send + 'static,
    State::Error: Debug,
    Keys: KeyPair<PubKey = PublicKey> + Signer<PublicKey::Sig> + Send + 'static,
{
    let mut keypair = keypair;
    let mut name = keypair.public().clone();
    let mut committee = committee.clone();

    let mut handles = Vec::new();
    let mut primary_network = PrimaryNetwork::default();
    let mut worker_network = PrimaryToWorkerNetwork::default();

    // Listen for new committees.
    loop {
        println!("[{name}] Starting at E{}", committee.epoch());

        // Get a fresh store for the new epoch.
        let store = NodeStorage::reopen(test_utils::temp_dir());

        // Restart the relevant components.
        let primary_handles = Node::spawn_primary(
            keypair,
            Arc::new(ArcSwap::new(Arc::new(committee.clone()))),
            &store,
            parameters.clone(),
            /* consensus */ true,
            execution_state.clone(),
            tx_output.clone(),
            &Registry::new(),
        )
        .await
        .unwrap();

        let worker_handles = Node::spawn_workers(
            name.clone(),
            /* worker_ids */ vec![0],
            Arc::new(ArcSwap::new(Arc::new(committee.clone()))),
            &store,
            parameters.clone(),
            &Registry::new(),
        );

        handles.extend(primary_handles);
        handles.extend(worker_handles);

        // Wait for a committee change.
        let (new_keypair, new_committee) = match rx_reconfigure.recv().await {
            Some(x) => x,
            None => break,
        };
        println!("[{name}] Received {new_committee}");

        // Shutdown all relevant components.
        let address = committee
            .primary(&name)
            .expect("Our key is not in the committee")
            .primary_to_primary;
        let message = PrimaryMessage::<PublicKey>::Reconfigure(ReconfigureNotification::Shutdown);
        let primary_cancel_handle = primary_network.send(address, &message).await;

        let address = committee
            .worker(&name, /* id */ &0)
            .expect("Our key is not in the committee")
            .primary_to_worker;
        let message =
            PrimaryWorkerMessage::<PublicKey>::Reconfigure(ReconfigureNotification::Shutdown);
        let worker_cancel_handle = worker_network.send(address, &message).await;
        println!("[{name}] Shutdown signal successfully sent");

        // Ensure the message has been received.
        primary_cancel_handle.await;
        worker_cancel_handle.await.unwrap();

        // Wait for the components to shut down.
        join_all(handles.drain(..)).await;
        println!("[{name}] is down\n");

        // Update the settings for the next epoch.
        keypair = new_keypair;
        name = keypair.public().clone();
        committee = new_committee;
    }
}

async fn run_client<PublicKey: VerifyingKey>(
    name: PublicKey,
    committee: Committee<PublicKey>,
    mut rx_reconfigure: Receiver<u64>,
) {
    let target = committee
        .worker(&name, /* id */ &0)
        .expect("Our key or worker id is not in the committee")
        .transactions;
    let config = mysten_network::config::Config::new();
    let channel = config.connect_lazy(&target).unwrap();
    let mut client = TransactionsClient::new(channel);

    // Make a transaction to submit for ever.
    let mut tx = TransactionProto {
        transaction: Bytes::from(0u64.to_be_bytes().to_vec()),
    };

    // Repeatedly send transactions.
    let mut interval = interval(Duration::from_millis(50));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tokio::pin!(interval);

    loop {
        tokio::select! {
            // Wait a bit before repeating.
            _ = interval.tick() => {
                // Send a transactions.
                if client.submit_transaction(tx.clone()).await.is_err() {
                    // The workers are still down.
                    sleep(Duration::from_millis(20)).await;
                    // println!("Worker not ready: {e}");
                }
            },

            // Send transactions on the new epoch.
            Some(epoch) = rx_reconfigure.recv() => {
                tx = TransactionProto {
                    transaction: Bytes::from(epoch.to_le_bytes().to_vec()),
                };
                println!("[{name}] Set tx={tx:?}");
            }
        }
    }
}

#[tokio::test]
async fn restart() {
    let committee = committee(None);
    let parameters = Parameters::default();

    // Spawn the nodes.
    let mut states = Vec::new();
    let mut rx_nodes = Vec::new();
    for (i, keypair) in keys(None).into_iter().enumerate() {
        let (tx_output, rx_output) = channel(10);
        let (tx_node_reconfigure, rx_node_reconfigure) = channel(10);

        let execution_state = Arc::new(SimpleExecutionState::new(i, tx_node_reconfigure));
        states.push(execution_state.clone());

        let committee = committee.clone();
        let execution_state = execution_state.clone();
        let parameters = parameters.clone();
        tokio::spawn(async move {
            run_node(
                keypair,
                &committee,
                execution_state,
                parameters,
                rx_node_reconfigure,
                tx_output,
            )
            .await;
        });

        rx_nodes.push(rx_output);
    }

    // Give a chance to the nodes to start.
    tokio::task::yield_now().await;

    // Spawn some clients.
    let mut tx_clients = Vec::new();
    for keypair in keys(None) {
        let (tx_client_reconfigure, rx_client_reconfigure) = channel(10);
        tx_clients.push(tx_client_reconfigure);

        let name = keypair.public().clone();
        let committee = committee.clone();
        tokio::spawn(
            async move { run_client(name, committee.clone(), rx_client_reconfigure).await },
        );
    }

    // Listen to the outputs.
    let mut current_epoch = 0;
    while let Some(output) = &rx_nodes[0].recv().await {
        let (outcome, _tx) = output;
        println!("{outcome:?}");
        match outcome {
            Ok(epoch) => {
                if epoch > &current_epoch {
                    current_epoch = *epoch;
                    for tx in &tx_clients {
                        tx.send(current_epoch).await.unwrap();
                    }
                }
            }
            Err(e) => panic!("{e}"),
        }
    }
}
