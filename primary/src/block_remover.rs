#![allow(dead_code)]
#![allow(unused_variables)]
use crate::{
    block_remover::BlockErrorType::Failed, Certificate, Header, PayloadToken, PrimaryWorkerMessage,
};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{traits::VerifyingKey, Digest, Hash};
use futures::{
    future::{try_join_all, BoxFuture},
    stream::{futures_unordered::FuturesUnordered, StreamExt as _},
    FutureExt,
};
use network::SimpleSender;
use std::{collections::HashMap, time::Duration};
use store::{rocks::TypedStoreError, Store};
use tokio::{
    sync::{mpsc::Receiver, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tracing::{error, log::debug};

type RequestKey = Vec<u8>;

#[cfg(test)]
#[path = "tests/block_remover_tests.rs"]
pub mod block_remover_tests;

pub enum BlockRemoverCommand {
    RemoveBlocks {
        // the block ids to remove
        ids: Vec<Digest>,
        // the channel to communicate the results
        sender: oneshot::Sender<BlockRemoverResult<RemoveBlocksResponse>>,
    },
}

#[derive(Clone, Debug)]
pub struct RemoveBlocksResponse {
    // the block ids to remove
    ids: Vec<Digest>,
}

type BlockRemoverResult<T> = Result<T, BlockRemoverError>;

#[derive(Clone, Debug)]
pub struct BlockRemoverError {
    ids: Vec<Digest>,
    error: BlockErrorType,
}

#[derive(Clone, Debug)]
pub enum BlockErrorType {
    Timeout,
    Failed,
}

pub type DeleteBatchResult = Result<DeleteBatchMessage, DeleteBatchMessage>;

#[derive(Clone, Default, Debug)]
pub struct DeleteBatchMessage {
    ids: Vec<Digest>,
}

/// BlockRemover is responsible for removing blocks identified by
/// their certificate id (digest) from across our system. On high level
/// It will make sure that the DAG is updated, internal storage where
/// there certificates and headers are stored, and the corresponding
/// batches as well.
pub struct BlockRemover<PublicKey: VerifyingKey> {
    /// The public key of this primary.
    name: PublicKey,

    /// The committee information.
    committee: Committee<PublicKey>,

    /// Storage that keeps the Certificates by their digest id.
    certificate_store: Store<Digest, Certificate<PublicKey>>,

    /// Storage that keeps the headers by their digest id
    header_store: Store<Digest, Header<PublicKey>>,

    /// The persistent storage for payload markers from workers.
    payload_store: Store<(Digest, WorkerId), PayloadToken>,

    /// Network driver allowing to send messages.
    network: SimpleSender,

    /// Receives the commands to execute against
    rx_commands: Receiver<BlockRemoverCommand>,

    /// Checks whether a pending request already exists
    pending_removal_requests: HashMap<RequestKey, Vec<Certificate<PublicKey>>>,

    /// Holds the senders that are pending to be notified for
    /// a removal request.
    tx_removal_results:
        HashMap<RequestKey, Vec<oneshot::Sender<BlockRemoverResult<RemoveBlocksResponse>>>>,

    tx_worker_removal_results: HashMap<RequestKey, oneshot::Sender<DeleteBatchResult>>,

    /// Receives all the responses to the requests to delete a batch.
    rx_delete_batches: Receiver<DeleteBatchResult>,
}

impl<PublicKey: VerifyingKey> BlockRemover<PublicKey> {
    pub fn spawn(
        name: PublicKey,
        committee: Committee<PublicKey>,
        certificate_store: Store<Digest, Certificate<PublicKey>>,
        header_store: Store<Digest, Header<PublicKey>>,
        payload_store: Store<(Digest, WorkerId), PayloadToken>,
        network: SimpleSender,
        rx_commands: Receiver<BlockRemoverCommand>,
        rx_delete_batches: Receiver<DeleteBatchResult>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                certificate_store,
                header_store,
                payload_store,
                network,
                rx_commands,
                pending_removal_requests: HashMap::new(),
                tx_removal_results: HashMap::new(),
                tx_worker_removal_results: HashMap::new(),
                rx_delete_batches,
            }
            .run()
            .await;
        })
    }

    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(command) = self.rx_commands.recv() => {
                    if let Some(fut) = self.handle_command(command).await {
                        waiting.push(fut);
                    }
                },
                Some(batch_result) = self.rx_delete_batches.recv() => {
                    self.handle_delete_batch_result(batch_result).await;
                },
                Some(result) = waiting.next() => {
                    self.handle_remove_waiting_result(result).await;
                }
            }
        }
    }

    async fn handle_remove_waiting_result(
        &mut self,
        result: BlockRemoverResult<RemoveBlocksResponse>,
    ) {
        let block_ids = result.clone().map_or_else(|e| e.ids, |r| r.ids);
        let key = Self::construct_request_key(&block_ids);

        match self.tx_removal_results.remove(&key) {
            None => {
                error!(
                    "We should expect to have a channel to send the results for key {:?}",
                    key
                );
            }
            Some(senders) => {
                let broadcast = |result_to_send: BlockRemoverResult<RemoveBlocksResponse>| {
                    for sender in senders {
                        sender
                            .send(result_to_send.clone())
                            .expect("Couldn't send message to channel");
                    }
                };

                let cleanup_successful = match self.pending_removal_requests.remove(&key) {
                    None => {
                        error!("Expected to find pending request for this key {:?}", &key);
                        Err(())
                    }
                    Some(certificates) => {
                        let batches_by_worker =
                            Self::map_batches_by_worker(certificates.as_slice());
                        // Clean up any possible pending result channel (e.x in case of a timeout channels are not cleaned up)
                        // So we ensure that we "unlock" the pending request and give the opportunity
                        // a request to re-execute if the downstream clean up operations fail.
                        for batch_ids in batches_by_worker.values() {
                            let request_key = Self::construct_request_key(batch_ids);

                            self.tx_worker_removal_results.remove(&request_key);
                        }

                        // Do the further clean up only if the result is successful
                        if result.is_ok() {
                            self.cleanup_internal_state(certificates, batches_by_worker)
                                .await
                                .map_err(|e| ())
                        } else {
                            Ok(())
                        }
                    }
                };

                // if the clean up is successful we want to send the result
                // whatever this is. Otherwise, we are sending back an error
                // response.
                if cleanup_successful.is_ok() {
                    broadcast(result);
                } else {
                    broadcast(Err(BlockRemoverError {
                        ids: block_ids,
                        error: BlockErrorType::Failed,
                    }));
                }
            }
        }
    }

    async fn cleanup_internal_state(
        &mut self,
        certificates: Vec<Certificate<PublicKey>>,
        batches_by_worker: HashMap<WorkerId, Vec<Digest>>,
    ) -> Result<(), TypedStoreError> {
        let header_ids: Vec<Digest> = certificates
            .clone()
            .into_iter()
            .map(|c| c.header.id)
            .collect();
        self.header_store.remove_all(header_ids).await?;

        // delete batch from the payload store as well
        let mut batches_to_cleanup: Vec<(Digest, WorkerId)> = Vec::new();
        for (worker_id, batch_ids) in batches_by_worker {
            batch_ids.into_iter().for_each(|d| {
                batches_to_cleanup.push((d, worker_id));
            })
        }
        self.payload_store.remove_all(batches_to_cleanup).await?;

        /* * * * * * * * * * * * * * * * * * * * * * * * *
         *         TODO: DAG deletion could go here?
         * * * * * * * * * * * * * * * * * * * * * * * * */

        // NOTE: delete certificates in the end since if we need to repeat the request
        // we want to be able to find them in storage.
        let certificate_ids: Vec<Digest> =
            certificates.as_slice().iter().map(|c| c.digest()).collect();
        self.certificate_store.remove_all(certificate_ids).await?;

        Ok(())
    }

    async fn handle_delete_batch_result(&mut self, batch_result: DeleteBatchResult) {
        let ids = batch_result.clone().map_or_else(|e| e.ids, |r| r.ids);
        let key = Self::construct_request_key(&ids);

        if let Some(sender) = self.tx_worker_removal_results.remove(&key) {
            sender
                .send(batch_result)
                .expect("couldn't send delete batch result to channel");
        } else {
            error!("no pending delete request has been found for key {:?}", key);
        }
    }

    async fn handle_command<'a>(
        &mut self,
        command: BlockRemoverCommand,
    ) -> Option<BoxFuture<'a, BlockRemoverResult<RemoveBlocksResponse>>> {
        match command {
            BlockRemoverCommand::RemoveBlocks { ids, sender } => {
                // check whether we have a similar request pending
                // to make the check easy we sort the digests in asc order,
                // and then we merge all the bytes to form a key
                let key = Self::construct_request_key(&ids);

                if self.pending_removal_requests.contains_key(&key) {
                    // request already pending, nothing to do, just add the sender to the list
                    // of pending to be notified ones.
                    self.tx_removal_results
                        .entry(key)
                        .or_insert_with(Vec::new)
                        .push(sender);

                    debug!("Removal blocks has an already pending request for the provided ids");
                    return None;
                }

                // find the blocks in certificates store
                match self.certificate_store.read_all(ids.clone()).await {
                    Ok(certificates) => {
                        // make sure that all of them exist, otherwise return an error
                        let num_of_non_found = certificates
                            .clone()
                            .into_iter()
                            .filter(|c| c.is_none())
                            .count();

                        if num_of_non_found > 0 {
                            // TODO: do we care on erroring back if not all certificates are
                            // found?
                        }

                        // ensure that we store only the found certificates
                        let found_certificates: Vec<Certificate<PublicKey>> =
                            certificates.into_iter().flatten().collect();

                        let receivers = self
                            .send_delete_requests_to_workers(found_certificates.clone())
                            .await;

                        // now spin up a waiter
                        let fut = Self::wait_for_responses(ids, receivers);

                        // add the certificates on the pending map
                        self.pending_removal_requests
                            .insert(key.clone(), found_certificates);

                        // add the sender to the pending map
                        self.tx_removal_results
                            .entry(key)
                            .or_insert_with(Vec::new)
                            .push(sender);

                        return Some(fut.boxed());
                    }
                    Err(_) => {
                        sender
                            .send(Err(BlockRemoverError { ids, error: Failed }))
                            .expect("Couldn't send error to channel");
                    }
                }
            }
        }

        None
    }

    async fn send_delete_requests_to_workers(
        &mut self,
        certificates: Vec<Certificate<PublicKey>>,
    ) -> Vec<(RequestKey, oneshot::Receiver<DeleteBatchResult>)> {
        // For each certificate, batch the requests by worker
        // and send the requests
        let batches_by_worker = Self::map_batches_by_worker(certificates.as_slice());

        let mut receivers: Vec<(RequestKey, oneshot::Receiver<DeleteBatchResult>)> = Vec::new();

        // now send the requests
        for (worker_id, batch_ids) in batches_by_worker {
            // send the batches to each worker id
            let worker_address = self
                .committee
                .worker(&self.name, &worker_id)
                .expect("Worker id not found")
                .primary_to_worker;

            let message = PrimaryWorkerMessage::<PublicKey>::DeleteBatches(batch_ids.clone());
            let serialised_message =
                bincode::serialize(&message).expect("Failed to serialize delete batch request");

            // send the request
            self.network
                .send(worker_address, Bytes::from(serialised_message))
                .await;

            // create a key based on the provided batch ids and use it as a request
            // key to identify the channel to forward the response once the delete
            // response is received.
            let worker_request_key = Self::construct_request_key(&batch_ids);

            let (tx, rx) = oneshot::channel();
            receivers.push((worker_request_key.clone(), rx));

            self.tx_worker_removal_results
                .insert(worker_request_key, tx);
        }

        receivers
    }

    async fn wait_for_responses(
        block_ids: Vec<Digest>,
        receivers: Vec<(RequestKey, oneshot::Receiver<DeleteBatchResult>)>,
    ) -> BlockRemoverResult<RemoveBlocksResponse> {
        let waiting: Vec<_> = receivers
            .into_iter()
            .map(|p| Self::wait_for_delete_response(p.0, p.1))
            .collect();

        let result = try_join_all(waiting).await;
        if result.as_ref().is_ok() {
            return Ok(RemoveBlocksResponse { ids: block_ids });
        }

        Err(BlockRemoverError {
            ids: block_ids,
            error: result.err().unwrap(),
        })
    }

    // this method waits to receive the result to a DeleteBatchRequest. We only care to report
    // a successful delete or not. It returns true if the batches have been successfully deleted,
    // otherwise false in any other case.
    async fn wait_for_delete_response(
        request_key: RequestKey,
        rx: oneshot::Receiver<DeleteBatchResult>,
    ) -> Result<RequestKey, BlockErrorType> {
        match timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(_)) => Ok(request_key),
            Ok(Err(_)) => Err(BlockErrorType::Failed),
            Err(_) => Err(BlockErrorType::Timeout),
        }
    }

    fn construct_request_key(ids: &[Digest]) -> RequestKey {
        let mut ids_cloned = ids.to_vec();
        ids_cloned.sort();

        let result: RequestKey = ids_cloned.into_iter().flat_map(|d| d.to_vec()).collect();

        result
    }

    // a helper method that collects all the batches from each certificate and maps
    // them by the worker id.
    fn map_batches_by_worker(
        certificates: &[Certificate<PublicKey>],
    ) -> HashMap<WorkerId, Vec<Digest>> {
        let mut batches_by_worker: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
        for certificate in certificates.iter() {
            for (batch_id, worker_id) in &certificate.header.payload {
                batches_by_worker
                    .entry(*worker_id)
                    .or_insert_with(Vec::new)
                    .push(batch_id.clone());
            }
        }

        batches_by_worker
    }
}

pub struct BlockHandler {}

impl BlockHandler {
    /// it retrieves a block by providing the block digest id (practically the
    /// certificate digest).
    pub async fn _get_block(_block_digest: Digest) -> Result<(), ()> {
        Ok(())
    }

    /// it receives an iterator of digests where each identifies a block, and it
    /// performs all the necessary steps to remove them from our system.
    pub async fn _remove_blocks(
        _block_digests: impl IntoIterator<Item = Digest>,
    ) -> Result<(), BlockRemoverError> {
        Ok(())
    }
}
