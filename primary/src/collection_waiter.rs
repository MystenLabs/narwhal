use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use crate::error::{DagError, DagResult};
use crate::messages::Header;
use crypto::{Digest, traits::VerifyingKey};
use async_trait::async_trait;
use bytes::Bytes;
use ed25519_dalek::PublicKey;
use store::Store;
use config::Committee;
use network::SimpleSender;
use tracing::{debug, error};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot};
use crate::error::DagError::CollectionNotFound;
use futures::future::{BoxFuture};
use tokio::time::{sleep, Instant};
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};

pub type Transaction = Vec<u8>;

#[cfg(test)]
#[path = "tests/collection_waiter_tests.rs"]
pub mod collection_waiter_tests;

pub enum CollectionCommand {
    /// GetCollection dictates retrieving the collection data
    /// (vector of transactions) by a given collection digest.
    /// Results are sent to the provided Sender.
    GetCollection { id: Digest }
}

pub struct GetCollectionResult {
    id: Digest,
    transactions: Vec<Transaction>
}

pub struct BatchMessage {
    id: Digest,
    transactions: Vec<Transaction>
}

/// CollectionWaiter is responsible for fetching the collection data from the
/// downstream worker nodes. The term collection is equal to what is called
/// "batch" on the rest of the codebase. However, for the external API the
/// term used is collection.
pub struct CollectionWaiter<PublicKey: VerifyingKey> {
    /// The public key of this primary.
    name: PublicKey,

    /// The committee information.
    committee: Committee<PublicKey>,

    /// Storage that keeps the headers by their digest id.
    header_store: Store<Digest, Header<PublicKey>>,

    /// Receive all the requests to get a collection
    rx_commands: Receiver<CollectionCommand>,

    /// A channel sender where the fetched collections
    /// are communicated to.
    tx_get_collection: Sender<DagResult<GetCollectionResult>>,

    /// Whenever we have a get_collection request, we mark the
    /// processing as pending by adding it on the hashset. Once
    /// we have a result back - or timeout - we expect to remove
    /// the digest from the set.
    pending_get_collection: HashSet<Digest>,

    /// Network driver allowing to send messages.
    network: SimpleSender,

    /// The batch receive channel is listening for received
    /// messages for batches that have been requested
    rx_batch_receiver: Receiver<BatchMessage>,

    /// Maps batch ids to channels that "listen" for arrived batch messages.
    /// On the key we hold the batch id (we assume it's globally unique).
    /// On the value we hold a tuple of the channel to communicate the result
    /// to and also a timestamp of when the request was sent.
    tx_pending_batch: HashMap<Digest, (oneshot::Sender<BatchMessage>, u128)>
}

impl<PublicKey: VerifyingKey> CollectionWaiter<PublicKey> {
    // Create a new waiter and start listening on incoming
    // commands to fetch a collection
    pub fn spawn(name: PublicKey,
                 committee: Committee<PublicKey>,
                 header_store: Store<Digest, Header<PublicKey>>,
                 rx_commands: Receiver<CollectionCommand>,
                 tx_get_collection: Sender<DagResult<GetCollectionResult>>,
                 batch_receiver: Receiver<BatchMessage>) {
        let mut s = Self {
            name,
            committee,
            header_store,
            rx_commands,
            tx_get_collection,
            pending_get_collection: HashSet::new(),
            network: SimpleSender::new(),
            rx_batch_receiver: batch_receiver,
            tx_pending_batch: HashMap::new()
        };

        tokio::spawn(async move {
            s.handle_requests().await;
        });
    }


    // Runs a loop where it receives requests to fetch
    // collections from the worker nodes
    async fn handle_requests(&mut self) {
        let timeout_milis = 2_000;

        let timer = sleep(Duration::from_millis(timeout_milis));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(command) = self.rx_commands.recv() => {
                    self.handle_commands(command).await;
                },
                Some(batch_message) = self.rx_batch_receiver.recv() => {
                    self.handle_batch_message(batch_message).await;
                },
                () = &mut timer => {
                    debug!("Timer has been triggered!");
                    println!("Timer has been triggered!");
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(timeout_milis));
                }
            }
        }
    }

    async fn handle_batch_message(&mut self, message: BatchMessage) {
        println!("Received batch message id {}", message.id);
    }

    // Wait for a batch to be received
    async fn wait_for_batch(batch_id: Digest, batch_receiver: oneshot::Receiver<BatchMessage>) -> DagResult<BatchMessage> {
         let result = batch_receiver.await;

        if result.is_ok() {
            return DagResult::Ok(result.unwrap());
        }

        return DagResult::Err(DagError::BatchCanNotBeRetrieved(batch_id));
    }

    // Waits for all the batches to return, or times out
    async fn wait_for_all_batches(mut wait_for_batches: FuturesUnordered<BoxFuture<'_, DagResult<BatchMessage>>>) -> DagResult<Vec<BatchMessage>> {
        let mut batch_messages = Vec::new();

        while let Some(batch_result) = wait_for_batches.next().await {
            if batch_result.is_ok() {
                batch_messages.push(batch_result.unwrap());
            } else {
                return DagResult::Err(DagError::FailedToGatherBatchesForCollection);
            }
        }

        return DagResult::Ok(batch_messages);
    }

    async fn handle_commands(&mut self, command: CollectionCommand) {
        match command {
            CollectionCommand::GetCollection { id } => {
                debug!("Got new GetCollection command");
                println!("Got new GetCollection command");

                let result = self.header_store.read(id.clone()).await;
                match result {
                    Result::Ok(header) => {
                        // If header has not been found, send back an error.
                        if header.is_none() {
                            println!("No header found");
                            self.tx_get_collection.send(DagResult::Err(CollectionNotFound(id.clone()))).await;
                            return;
                        }

                        println!("Header has been found!");

                        // If similar request is already under processing, don't start a new one
                        if self.pending_get_collection.contains(&id.clone()) {
                            debug!("Collection with id {} has already pending request", id.clone());
                            return;
                        }

                        println!("No pending get collection");

                        // Get the "now" time
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                        // Add on a vector the receivers
                        let mut wait_for_batches = FuturesUnordered::new();

                        // otherwise we send requests to all workers to send us their batches
                        for (digest, worker_id) in header.unwrap().payload {
                            let b = Bytes::new();

                            println!("Sending batch {} request to worker id {}", digest, worker_id);

                            let worker_address = self.committee
                                .worker(&self.name, &worker_id)
                                .expect("Worker id not found")
                                .primary_to_worker;

                            self.network.send(worker_address, b).await;

                            // mark it as pending batch. Since we assume that batches are unique
                            // per collection, a clean up on a collection request will also clean
                            // up all the pending batch requests.
                            let (tx, rx) = oneshot::channel();
                            self.tx_pending_batch.insert(digest.clone(), (tx, now));

                            // add the receiver to a vector to poll later
                            let fut = Self::wait_for_batch(digest, rx);
                            wait_for_batches.push(Box::pin(fut));
                        }

                        println!("Now will wait for results");
                        debug!("Now will wait for results");

                        let res = Self::wait_for_all_batches(wait_for_batches).await;

                        let mut batch_messages = Vec::new();

                        while let Some(batch_result) = wait_for_batches.next().await {
                            if batch_result.is_ok() {
                                debug!("Got successful batches");
                                println!("Got successful batches");
                                batch_messages.push(batch_result.unwrap());
                            } else {
                                error!("Couldn't gather batches");
                                println!("Couldn't gather batches");
                            }
                        }

                        self.tx_get_collection.send(
                            DagResult::Ok(GetCollectionResult{
                                id: id.clone(),
                                transactions: Vec::new(),
                            })
                        ).await;

                        /*
                        if res.is_ok() {
                            debug!("Got successful batches");
                            self.tx_get_collection.send(
                                DagResult::Ok(GetCollectionResult{
                                    id: id.clone(),
                                    transactions: Vec::new(),
                                })
                            );
                        } else {
                            error!("Couldn't gather batches");
                        }
                          */
                        println!("Hey, got the results back!");

                    },
                    Result::Err(err) => {
                        error!("Store error");
                        self.tx_get_collection.send(DagResult::Err(CollectionNotFound(id.clone()))).await;
                    }
                }
            },
        }
    }
}