use crate::{messages::Header, Certificate, PrimaryWorkerMessage};
use bytes::Bytes;
use config::Committee;
use crypto::{traits::VerifyingKey, Digest};
use ed25519_dalek::{Digest as _, PublicKey, Sha512};
use futures::future::BoxFuture;
use futures::{
    future::try_join_all,
    stream::{futures_unordered::FuturesUnordered, StreamExt as _},
    FutureExt,
};
use network::SimpleSender;
use std::future::Future;
use std::pin::Pin;
use std::{
    collections::HashMap,
    fmt,
    fmt::Formatter,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use store::Store;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    time::timeout,
};
use tracing::error;
use tracing::log::debug;
use Result::*;

const BATCH_RETRIEVE_TIMEOUT_MILIS: u64 = 1_000;

pub type Transaction = Vec<u8>;

#[cfg(test)]
#[path = "tests/collection_waiter_tests.rs"]
pub mod collection_waiter_tests;

pub enum CollectionCommand {
    /// GetCollection dictates retrieving the collection data
    /// (vector of transactions) by a given collection digest.
    /// Results are sent to the provided Sender.
    _GetCollection { id: Digest },
}

#[derive(Clone, Debug)]
pub struct GetCollectionResult {
    id: Digest,
    _batches: Vec<BatchMessage>,
}

#[derive(Clone, Default, Debug)]
pub struct BatchMessage {
    id: Digest,
    _transactions: Vec<Transaction>,
}

pub type CollectionResult<T> = Result<T, CollectionError>;

#[derive(Debug, Clone)]
pub struct CollectionError {
    id: Digest,
    error: CollectionErrorType,
}

impl<T> From<CollectionError> for CollectionResult<T> {
    fn from(error: CollectionError) -> Self {
        CollectionResult::Err(error)
    }
}

impl fmt::Display for CollectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "collection id: {}, error type: {}", self.id, self.error)
    }
}

#[derive(Debug, Clone)]
pub enum CollectionErrorType {
    CollectionNotFound,
    BatchTimeout,
    BatchError,
}

impl fmt::Display for CollectionErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// CollectionWaiter is responsible for fetching the collection data from the
/// downstream worker nodes. The term collection is equal to what is called
/// "block" (or header) on the rest of the codebase. However, for the external API the
/// term used is collection.
///
/// In order for the component to be used from another component the following
/// input and output channels are defined:
///
/// # Inputs
/// * rx_commands - Provide this receiver in order to send a command to the waiter
/// (e.x GetCollection)
/// * rx_batch_receiver - Provide this receiver in order to send a batch message
/// to the components. Basically a requested batch that is received from a worker
/// should be sent on this channel.
///
/// # Outputs
/// * tx_get_collection - Provide this sender to receive the collection data that
/// have been requested.
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
    tx_get_collection: Sender<CollectionResult<GetCollectionResult>>,

    /// Whenever we have a get_collection request, we mark the
    /// processing as pending by adding it on the hashmap. Once
    /// we have a result back - or timeout - we expect to remove
    /// the digest from the map. The key is the collection id, and
    /// the value is the corresponding header.
    pending_get_collection: HashMap<Digest, Header<PublicKey>>,

    /// Network driver allowing to send messages.
    network: SimpleSender,

    /// The batch receive channel is listening for received
    /// messages for batches that have been requested
    rx_batch_receiver: Receiver<BatchMessage>,

    /// Maps batch ids to channels that "listen" for arrived batch messages.
    /// On the key we hold the batch id (we assume it's globally unique).
    /// On the value we hold a tuple of the channel to communicate the result
    /// to and also a timestamp of when the request was sent.
    tx_pending_batch: HashMap<Digest, (Sender<BatchMessage>, u128)>,
}

impl<PublicKey: VerifyingKey> CollectionWaiter<PublicKey> {
    // Create a new waiter and start listening on incoming
    // commands to fetch a collection
    pub fn spawn(
        name: PublicKey,
        committee: Committee<PublicKey>,
        header_store: Store<Digest, Header<PublicKey>>,
        rx_commands: Receiver<CollectionCommand>,
        tx_get_collection: Sender<CollectionResult<GetCollectionResult>>,
        batch_receiver: Receiver<BatchMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                header_store,
                rx_commands,
                tx_get_collection,
                pending_get_collection: HashMap::new(),
                network: SimpleSender::new(),
                rx_batch_receiver: batch_receiver,
                tx_pending_batch: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(command) = self.rx_commands.recv() => {
                    match self.handle_command(command).await {
                        Some(fut) => waiting.push(fut),
                        None => debug!("no processing for command, will not wait for any results")
                    }
                },
                // When we receive a BatchMessage (from a worker), this is
                // this is captured by the rx_batch_receiver channel and
                // handled appropriately.
                Some(batch_message) = self.rx_batch_receiver.recv() => {
                    self.handle_batch_message(batch_message).await;
                },
                // When we send a request to fetch a collection's batches
                // we wait on the results to come back before we proceed.
                // By iterating the waiting vector it allow us to proceed
                // whenever waiting has been finished for a request.
                Some(result) = waiting.next() => {
                    self.handle_batch_waiting_result(result).await;
                },
            }
        }
    }

    // handles received commands and returns back a future if needs to
    // wait for further results.
    async fn handle_command<'a>(
        &mut self,
        command: CollectionCommand,
    ) -> Option<BoxFuture<'a, CollectionResult<GetCollectionResult>>> {
        match command {
            CollectionCommand::_GetCollection { id } => {
                match self.header_store.read(id.clone()).await {
                    Ok(Some(header)) => {
                        // If similar request is already under processing, don't start a new one
                        if self.pending_get_collection.contains_key(&id.clone()) {
                            debug!(
                                "Collection with id {} has already pending request",
                                id.clone()
                            );
                            return None;
                        }

                        debug!("No pending get collection for {}", id.clone());

                        // Add on a vector the receivers
                        let mut batch_receivers = self.send_batch_requests(header.clone()).await;

                        let fut = Self::wait_for_all_batches(id.clone(), batch_receivers);

                        // Ensure that we mark this collection retrieval
                        // as pending so no other can initiate the process
                        self.pending_get_collection.insert(id.clone(), header);

                        debug!("Now waiting results for collection {}", id.clone());

                        return Some(fut.boxed());
                    }
                    _ => {
                        self.tx_get_collection
                            .send(
                                CollectionResult::from(CollectionError {
                                    id: id.clone(),
                                    error: CollectionErrorType::CollectionNotFound,
                                })
                            )
                            .await
                            .expect("Couldn't send CollectionNotFound error for a GetCollection request");
                    }
                }
            }
        }

        None
    }

    async fn handle_batch_waiting_result(&mut self, result: CollectionResult<GetCollectionResult>) {
        self.tx_get_collection
            .send(result.clone())
            .await
            .expect("Couldn't send GetCollectionResult message");

        let collection_id = result.map_or_else(|e| e.id, |r| r.id);

        // unlock the pending request & batches
        match self.pending_get_collection.remove(&collection_id) {
            Some(header) => {
                for (digest, _) in header.payload {
                    // unlock the pending request
                    self.tx_pending_batch.remove(&digest);
                }
            }
            None => {
                error!(
                    "Expected to find header with id {} for pending processing",
                    &collection_id
                );
            }
        }
    }

    // Sends requests to fetch the batches from the corresponding workers.
    // It returns a vector of tuples of the batch digest and a Receiver
    // channel of the fetched batch.
    async fn send_batch_requests(
        &mut self,
        header: Header<PublicKey>,
    ) -> Vec<(Digest, Receiver<BatchMessage>)> {
        // Get the "now" time
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis();

        // Add on a vector the receivers
        let mut batch_receivers = Vec::new();

        // otherwise we send requests to all workers to send us their batches
        for (digest, worker_id) in header.payload {
            debug!(
                "Sending batch {} request to worker id {}",
                digest.clone(),
                worker_id
            );

            let worker_address = self
                .committee
                .worker(&self.name, &worker_id)
                .expect("Worker id not found")
                .primary_to_worker;

            let message = PrimaryWorkerMessage::<PublicKey>::RequestBatch(digest.clone());
            let bytes = bincode::serialize(&message).expect("Failed to serialize batch request");

            self.network.send(worker_address, Bytes::from(bytes)).await;

            // mark it as pending batch. Since we assume that batches are unique
            // per collection, a clean up on a collection request will also clean
            // up all the pending batch requests.
            let (tx, rx) = channel(1);
            self.tx_pending_batch.insert(digest.clone(), (tx, now));

            // add the receiver to a vector to poll later
            batch_receivers.push((digest.clone(), rx));
        }

        batch_receivers
    }

    async fn handle_batch_message(&mut self, message: BatchMessage) {
        match self.tx_pending_batch.get(&message.id) {
            Some((sender, _)) => {
                debug!("Sending batch message with id {}", &message.id);
                sender
                    .send(message.clone())
                    .await
                    .expect("Couldn't send BatchMessage for pending batch");
            }
            None => {
                debug!("Couldn't find pending batch with id {}", message.id);
            }
        }
    }

    /// A helper method to "wait" for all the batch responses to be received.
    /// It gets the fetched batches and creates a GetCollectionResult ready
    /// to be sent back to the request.
    async fn wait_for_all_batches(
        collection_id: Digest,
        batches_receivers: Vec<(Digest, Receiver<BatchMessage>)>,
    ) -> CollectionResult<GetCollectionResult> {
        let waiting: Vec<_> = batches_receivers
            .into_iter()
            .map(|p| Self::wait_for_batch(collection_id.clone(), p.1))
            .collect();

        let result = try_join_all(waiting).await?;
        Ok(GetCollectionResult {
            id: collection_id,
            _batches: result,
        })
    }

    /// Waits for a batch to be received. If batch is not received in time,
    /// then a timeout is yielded and an error is returned.
    async fn wait_for_batch(
        collection_id: Digest,
        mut batch_receiver: Receiver<BatchMessage>,
    ) -> CollectionResult<BatchMessage> {
        // ensure that we won't wait forever for a batch result to come
        let timeout_duration = Duration::from_millis(BATCH_RETRIEVE_TIMEOUT_MILIS);

        return match timeout(timeout_duration, batch_receiver.recv()).await {
            Ok(result) => result.ok_or(CollectionError {
                id: collection_id,
                error: CollectionErrorType::BatchError,
            }),
            Err(_) => CollectionResult::from(CollectionError {
                id: collection_id,
                error: CollectionErrorType::BatchTimeout,
            }),
        };
    }
}
