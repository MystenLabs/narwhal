use crate::messages::Header;
use crate::PrimaryWorkerMessage;
use bytes::Bytes;
use config::Committee;
use crypto::{traits::VerifyingKey, Digest};
use futures::future::try_join_all;
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};
use network::SimpleSender;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::timeout;
use tracing::error;

const BATCH_RETRIEVE_TIMEOUT_MILIS: u64 = 1_000;

pub type Transaction = Vec<u8>;

#[cfg(test)]
#[path = "tests/collection_waiter_tests.rs"]
pub mod collection_waiter_tests;

pub enum CollectionCommand {
    /// GetCollection dictates retrieving the collection data
    /// (vector of transactions) by a given collection digest.
    /// Results are sent to the provided Sender.
    GetCollection { id: Digest },
}

#[derive(Clone, Debug)]
pub struct GetCollectionResult {
    id: Digest,
    batches: Vec<BatchMessage>,
}

#[derive(Clone, Default, Debug)]
pub struct BatchMessage {
    id: Digest,
    transactions: Vec<Transaction>,
}

pub type CollectionResult<T> = Result<T, CollectionError>;

#[derive(Debug)]
pub struct CollectionError {
    id: Digest,
    error: CollectionErrorType,
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
    GetCollectionTimeout,
}

impl fmt::Display for CollectionErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// CollectionWaiter is responsible for fetching the collection data from the
/// downstream worker nodes. The term collection is equal to what is called
/// "batch" on the rest of the codebase. However, for the external API the
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
                    match command {
                        CollectionCommand::GetCollection { id } => {
                            println!("Got new GetCollection command for collection {}", id.clone());

                            let result = self.header_store.read(id.clone()).await;
                            match result {
                                Result::Ok(header) => {
                                    // If header has not been found, send back an error.
                                    if header.is_none() {
                                        println!("No header found for collection {}", id.clone());
                                        self.tx_get_collection
                                        .send(Self::error_result(id.clone(), CollectionErrorType::CollectionNotFound))
                                        .await
                                        .expect("Couldn't send result for collection");
                                        continue;
                                    }

                                    // If similar request is already under processing, don't start a new one
                                    if self.pending_get_collection.contains_key(&id.clone()) {
                                        println!("Collection with id {} has already pending request", id.clone());
                                        continue;
                                    }

                                    println!("No pending get collection for {}", id.clone());

                                    // Get the "now" time
                                    let now = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .expect("Failed to measure time")
                                        .as_millis();

                                    // Add on a vector the receivers
                                    let mut batch_receivers = Vec::new();

                                    // otherwise we send requests to all workers to send us their batches
                                    for (digest, worker_id) in header.clone().unwrap().payload {
                                        println!("Sending batch {} request to worker id {}", digest.clone(), worker_id);

                                        let worker_address = self.committee
                                            .worker(&self.name, &worker_id)
                                            .expect("Worker id not found")
                                            .primary_to_worker;

                                        let message = PrimaryWorkerMessage::<PublicKey>::RequestBatch(digest.clone());
                                        let bytes = bincode::serialize(&message)
                                                    .expect("Failed to serialize batch request");

                                        self.network.send(worker_address, Bytes::from(bytes)).await;

                                        // mark it as pending batch. Since we assume that batches are unique
                                        // per collection, a clean up on a collection request will also clean
                                        // up all the pending batch requests.
                                        let (tx, rx) = channel(1);
                                        self.tx_pending_batch.insert(digest.clone(), (tx, now));

                                        // add the receiver to a vector to poll later
                                        batch_receivers.push((digest.clone(), rx));
                                    }

                                    let fut = Self::wait_for_all_batches(id.clone(), batch_receivers);
                                    waiting.push(fut);

                                    // Ensure that we mark this collection retrieval
                                    // as pending so no other can initiate the process
                                    self.pending_get_collection.insert(id.clone(), header.unwrap());

                                    println!("Now waiting results for collection {}", id.clone());
                                },
                                Result::Err(_err) => {
                                    error!("Store error");
                                    self.tx_get_collection
                                    .send(Self::error_result(id.clone(), CollectionErrorType::CollectionNotFound))
                                    .await
                                    .expect("Couldn't send CollectionNotFound error for a GetCollection request");
                                }
                            }
                        },
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
                    let collection_id = match result {
                        Ok(collection_result) => {
                            println!("GetCollection ready for collection {}", &collection_result.id);

                            self.tx_get_collection.send(CollectionResult::Ok(collection_result.clone()))
                            .await
                            .expect("Couldn't send GetCollectionResult message");

                            collection_result.id
                        },
                        Err(err) =>  {
                            self.tx_get_collection.send(Self::error_result(err.id.clone(), err.error))
                            .await
                            .expect("Couldn't send error for GetCollectionResult message");

                            err.id
                        }
                    };

                    // unlock the pending request & batches
                    let header = self.pending_get_collection.remove(&collection_id);
                    if header.is_some() {
                        for (digest, _) in header.unwrap().payload {
                            // unlock the pending request
                            self.tx_pending_batch.remove(&digest);
                        }
                    }
                },
            }
        }
    }

    async fn handle_batch_message(&mut self, message: BatchMessage) {
        println!("Received batch with id {}", &message.id);

        match self.tx_pending_batch.get(&message.id) {
            Some((sender, _)) => {
                println!("Sending batch message with id {}", &message.id);
                sender
                    .send(message.clone())
                    .await
                    .expect("Couldn't send BatchMessage for pending batch");
            }
            None => {
                println!("Couldn't find pending batch with id {}", message.id);
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
            .map(|p| Self::wait_for_batch(p.1))
            .collect();

        let handle = try_join_all(waiting).await;
        if handle.is_ok() {
            return CollectionResult::Ok(GetCollectionResult {
                id: collection_id,
                batches: handle.unwrap(),
            });
        }

        return Self::error_result(collection_id, CollectionErrorType::GetCollectionTimeout);
    }

    /// Waits for a batch to be received. If batch is not received in time,
    /// then a timeout is yielded and an error is returned.
    async fn wait_for_batch(
        mut batch_receiver: Receiver<BatchMessage>,
    ) -> Result<BatchMessage, CollectionErrorType> {
        // ensure that we won't wait forever for a batch result to come
        let timeout_duration = Duration::from_millis(BATCH_RETRIEVE_TIMEOUT_MILIS);

        let result = timeout(timeout_duration, batch_receiver.recv()).await;

        if result.is_ok() {
            return match result.unwrap() {
                Some(batch) => Result::Ok(batch),
                // Probably the channel has been closed - we don't expect
                // this, but still return an error back.
                None => Result::Err(CollectionErrorType::BatchError),
            };
        }

        return Result::Err(CollectionErrorType::BatchTimeout);
    }

    fn error_result<T>(collection_id: Digest, error: CollectionErrorType) -> CollectionResult<T> {
        return CollectionResult::Err(CollectionError {
            id: collection_id,
            error,
        });
    }
}
