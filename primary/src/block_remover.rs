use std::collections::HashMap;
use std::io::Error;
use std::time::Duration;
use bytes::Bytes;
use futures::future::BoxFuture;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tracing::log::debug;
use config::{Committee, WorkerId};
use crypto::{Digest, traits::VerifyingKey};
use network::SimpleSender;
use crate::{Certificate, Header, PrimaryWorkerMessage};

#[cfg(test)]
#[path = "tests/block_remover_tests.rs"]
pub mod block_remover_tests;

pub enum BlockRemoverCommand {
    RemoveBlocks {
        // the block ids to remove
        ids: Vec<Digest>,
        // the channel to communicate the results
        sender: oneshot::Sender<BlockRemoverResult<RemoveBlocksResponse>>,
    }
}

type BlockRemoverResult<T> = Result<T, BlockRemoverError>;

pub enum BlockRemoverError {
    /// TODO: define errors here
}

pub struct RemoveBlocksResponse {
    /// TODO: add context here
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

    /// Network driver allowing to send messages.
    network: SimpleSender,

    /// Receives the commands to execute against
    rx_commands: Receiver<BlockRemoverCommand>,

    /// Checks whether a pending request already exists
    pending_removal_requests: HashMap<Vec<u8>, Vec<Certificate<PublicKey>>>,

    /// Holds the senders that are pending to be notified for
    /// a removal request.
    tx_removal_requests_map: HashMap<Vec<u8>, Vec<oneshot::Sender<BlockRemoverResult<RemoveBlocksResponse>>>>,

    tx_pending_worker_requests: HashMap<Vec<u8>, Sender<()>>,
}

impl<PublicKey: VerifyingKey> BlockRemover<PublicKey> {
    pub fn spawn(name: PublicKey,
                 committee: Committee<PublicKey>,
                 certificate_store: Store<Digest, Certificate<PublicKey>>,
                 header_store: Store<Digest, Header<PublicKey>>,
                 network: SimpleSender,
                 rx_commands: Receiver<BlockRemoverCommand>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                certificate_store,
                header_store,
                network,
                rx_commands,
                pending_removal_requests: HashMap::new(),
                tx_removal_requests_map: HashMap::new(),
            }.run().await;
        })
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(command) = self.rx_commands.recv() => {
                    self.handle_command(command).await;
                }
            }
        }
    }

    async fn handle_command<'a>(&mut self, command: BlockRemoverCommand)
                                -> Option<BoxFuture<'a, BlockRemoverResult<RemoveBlocksResponse>>> {
        match command {
            BlockRemoverCommand::RemoveBlocks { ids, sender } => {
                // check whether we have a similar request pending
                // to make the check easy we sort the digests in asc order,
                // and then we merge all the bytes to form a key
                let key = Self::construct_request_key(ids.as_slice());

                if self.pending_removal_requests.contains_key(&key) {
                    // request already pending, nothing to do, just add the sender to the list
                    // of pending to be notified ones.
                    self.tx_removal_requests_map
                        .entry(key)
                        .or_insert_with(Vec::new)
                        .push(sender);

                    debug!("Removal blocks has an already pending request for the provided ids");
                    return None;
                }

                // find the blocks in certificates store
                match self.certificate_store.read_all(ids).await {
                    Ok(certificates) => {
                        // make sure that all of them exist, otherwise return an error
                        let num_of_non_found = certificates
                            .into_iter()
                            .filter(|c| c.is_none())
                            .count();

                        if num_of_non_found > 0 {
                            // TODO: do we care on erroring back if not all certificates are
                            // found?
                        }

                        // For each certificate, batch the requests by worker
                        // and send the requests
                        let mut batches_by_worker: HashMap<WorkerId, Vec<Digest>> = HashMap::new();
                        for c in certificates.into_iter() {
                            if let Some(certificate) = c {
                                for (batch_id, worker_id) in certificate.header.payload {
                                    batches_by_worker
                                        .entry(worker_id)
                                        .or_insert_with(Vec::new)
                                        .push(batch_id);
                                }
                            }
                        }

                        // now send the requests
                        for (worker_id, batch_ids) in batches_by_worker {
                            // send the batches to each worker id
                            let worker_address = self
                                .committee
                                .worker(&self.name, &worker_id)
                                .expect("Worker id not found")
                                .primary_to_worker;

                            let message = PrimaryWorkerMessage::<PublicKey>::DeleteBatches(batches);
                            let serialised_message = bincode::serialize(&message).expect("Failed to serialize delete batch request");
                            ;

                            // send the request
                            self.network.send(worker_address, Bytes::from(serialised_message)).await;
                        }

                        let (tx, rx) = channel(batches_by_worker.len());
                        self.tx_pending_worker_requests.insert(key, tx);

                        // now spin up a waiter


                    }
                    Err(_) => {}
                }
            }
        }

        None
    }

    async fn wait_for_responses(request_id: &[u8], num_of_expected_responses: u32, mut requests_receiver: Receiver<()>) -> BlockRemoverResult<RemoveBlocksResponse>  {
        let timer = sleep(Duration::from_millis(2000));
        tokio::pin!(timer);

        let mut responses_received = 0;
        while counter < num_of_expected_responses  {
            tokio::select! {
                Some(result) = requests_receiver.recv() => {

                },
                () = &mut timer => {
                    println!("timed out before getting back all results");
                }
            }
            responses_received += 1;
        }


    }

    fn construct_request_key(ids: &[Digest]) -> Vec<u8> {
        ids
            .into_iter()
            .sorted()
            .flat_map(|d| d.to_vec())
            .collect()
            .to_vec()
    }
}

pub struct BlockHandler {}

impl BlockHandler {
    /// it retrieves a block by providing the block digest id (practically the
    /// certificate digest).
    pub async fn get_block(block_digest: Digest) -> Result<(), ()> {
        Ok(())
    }

    /// it receives an iterator of digests where each identifies a block, and it
    /// performs all the necessary steps to remove them from our system.
    pub async fn remove_blocks(block_digests: impl IntoIterator<Item=Digest>) -> Result<(), BlockRemoverError> {
        Ok(())
    }
}