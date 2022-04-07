use crate::utils::{AuthorityState, ConnectionWaiter};
use bytes::Bytes;
use consensus::{ConsensusOutput, SequenceNumber};
use crypto::{ed25519::Ed25519PublicKey, traits::VerifyingKey};
use futures::{stream::StreamExt, SinkExt};
use primary::{Batch, BatchDigest, Certificate};
use std::{cmp::Ordering, error::Error, net::SocketAddr, sync::Arc};
use store::Store;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, warn};
use worker::{SerializedBatchMessage, WorkerMessage};

/// The possible successful outcome when processing a consensus message.
enum ProcessingOutcome {
    /// All went well (or at least there is nothing to do on our side).
    Ok,
    /// We missed some outputs and need to sync with the consensus node.
    MissingOutputs,
}

/// The `Subscriber` receives certificates sequenced by the consensus and updates
/// the authority's database. The client assumes that the messages it receives have
/// already been authenticated (ie. they really come from a trusted consensus node) and
/// integrity-validated (ie. no corrupted messages).
pub struct Subscriber<State: AuthorityState, PublicKey: VerifyingKey> {
    /// The network address of the consensus node.
    address: SocketAddr,
    /// The (temporary) storage holding all transactions' data.
    store: Store<BatchDigest, SerializedBatchMessage>,
    /// The (global) state to perform execution.
    state: Arc<State>,
    /// A channel to the batch loader to download transaction's data.
    tx_batch_loader: Sender<ConsensusOutput<PublicKey>>,
    /// The index of the latest consensus message we processed.
    last_consensus_index: SequenceNumber,
}

impl<State: AuthorityState, PublicKey: VerifyingKey> Drop for Subscriber<State, PublicKey> {
    fn drop(&mut self) {
        self.state.release_consensus_write_lock();
    }
}

impl<State: AuthorityState, PublicKey: VerifyingKey> Subscriber<State, PublicKey> {
    /*
    /// Create a new consensus handler with the input authority state.
    pub fn new(state: Arc<AuthorityState>) -> SuiResult<Self> {
        // Ensure there is a single consensus client modifying the state.
        let status = state
            .ask_consensus_write_lock
            .fetch_add(1, AtomicOrdering::SeqCst);
        fp_ensure!(status == 0, SuiError::OnlyOneConsensusClientPermitted);

        // Load the last consensus index from storage.
        let last_consensus_index = state.last_consensus_index()?;

        // Return a consensus client only if all went well (safety-critical).
        Ok(Self {
            state,
            last_consensus_index,
        })
    }

    /// Spawn the consensus client in a new tokio task.
    pub fn spawn(
        mut handler: Self,
        address: SocketAddr,
        buffer_size: usize,
    ) -> JoinHandle<SuiResult<()>> {
        log::info!("Consensus client connecting to {}", address);
        tokio::spawn(async move { handler.run(address, buffer_size).await })
    }
    */

    /// Synchronize with the consensus in case we missed part of its output sequence.
    /// It is safety-critical that we process the consensus' outputs in the complete
    /// and right order.
    async fn synchronize(
        &mut self,
        connection: &mut Framed<TcpStream, LengthDelimitedCodec>,
    ) -> Result<(), Box<dyn Error>> {
        /*
        let request = ConsensusSync {
            sequence_number: self.last_consensus_index,
        };
        let bytes = Bytes::from(serialize_consensus_sync(&request));
        connection
            .sink()
            .send(bytes)
            .await
            .map_err(|e| SuiError::ClientIoError {
                error: e.to_string(),
            })
        */
        Ok(())
    }

    /// Process a single sequenced certificate.
    async fn handle_consensus_message(
        &mut self,
        message: ConsensusOutput<PublicKey>,
    ) -> Result<ProcessingOutcome, Box<dyn Error>> {
        let consensus_index = message.consensus_index;

        // Check that the latest consensus index is as expected; otherwise synchronize.
        match self.last_consensus_index.cmp(&consensus_index) {
            Ordering::Greater => {
                // That is fine, it may happen when the consensus node crashed and recovered.
                debug!("Consensus index of authority bigger than expected");
                return Ok(ProcessingOutcome::Ok);
            }
            Ordering::Less => {
                debug!("Authority is synchronizing missed sequenced certificates");
                return Ok(ProcessingOutcome::MissingOutputs);
            }
            Ordering::Equal => (),
        }

        // Send the certificate to the batch loader to download all transactions' data.
        self.tx_batch_loader
            .send(message)
            .await
            .expect("Failed to send message ot batch loader");

        // Update the latest consensus index. The state will atomically persist the change
        // when processing executing the transaction. It is important to increment the consensus
        // index before deserializing the certificate because the consensus core will increment
        // its own index regardless of deserialization or other protocol-specific failures.
        self.last_consensus_index += 1;

        Ok(ProcessingOutcome::Ok)
    }

    async fn execute_transactions(
        &mut self,
        message: ConsensusOutput<PublicKey>,
    ) -> Result<(), Box<dyn Error>> {
        // It is now guaranteed that the store holds all transaction data referenced by the input certificate.
        for digest in message.certificate.header.payload.keys() {
            let batch = self.store.read(*digest).await?.unwrap(); // TODO: unwrap
            let transactions = match bincode::deserialize(&batch) {
                // TODO: double deserializeation!!!
                Ok(WorkerMessage::<PublicKey>::Batch(Batch(x))) => x,
                _ => panic!("??"), // TODO: the worker sent us crap
            };

            for transaction in transactions {
                // The consensus simply orders bytes, so we first need to deserialize the
                // certificate. If the deserialization fail it is safe to ignore the
                // certificate since all correct authorities will do the same. Remember that a
                // bad authority or client may input random bytes to the consensus.
                let command = match bincode::deserialize(&transaction) {
                    Ok(x) => x,
                    Err(e) => {
                        debug!("{e}");
                        continue;
                    }
                };

                let consensus_index = 0; // TODO
                                         // TODO: the function below may return errors to ignore as well as storeErrors
                self.state
                    .handle_consensus_transaction(consensus_index, command)
                    .await;
            }
        }
        Ok(())
    }

    /// Main loop connecting to the consensus. This mainly acts as a light client.
    async fn run(&mut self) -> Result<(), Box<dyn Error>> {
        // The connection waiter ensures we do not attempt to reconnect immediately after failure.
        let mut connection_waiter = ConnectionWaiter::default();

        // Continuously connects to the consensus node.
        'main: loop {
            // Wait a bit before re-attempting connections.
            connection_waiter.wait().await;

            // Subscribe to the consensus' output.
            let mut connection = match TcpStream::connect(self.address).await {
                Ok(x) => Framed::new(x, LengthDelimitedCodec::new()),
                Err(e) => {
                    warn!(
                        "Failed to subscribe to consensus output (retry {}): {e}",
                        connection_waiter.status(),
                    );
                    continue 'main;
                }
            };

            // Listen to sequenced certificates and process them.
            loop {
                let message = match connection.next().await {
                    Some(Ok(bytes)) => match bincode::deserialize(&bytes.to_vec()) {
                        Ok(message) => message,
                        Err(e) => {
                            warn!("Failed to deserialize consensus output {}", e);
                            continue 'main;
                        }
                    },
                    Some(Err(e)) => {
                        warn!("Failed to receive data from consensus: {}", e);
                        continue 'main;
                    }
                    None => {
                        debug!("Connection dropped by consensus");
                        continue 'main;
                    }
                };

                match self.handle_consensus_message(message).await {
                    Err(e) => {
                        // We may want to log the errors that are the user's fault (i.e., that
                        // are neither our fault or the fault of consensus) for debug purposes.
                        // It is safe to continue by ignoring those certificates/transactions
                        // since all honest subscribers will do the same.
                        debug!("{e}");

                        // We however must take special care to errors that are our fault, such as
                        // storage errors. We may be the only authority experiencing it, and thus cannot
                        // continue to process certificates until the problem is fixed.
                        error!("{e}");
                        return Err(e);
                    }
                    // The authority missed some consensus outputs and needs to sync.
                    Ok(ProcessingOutcome::MissingOutputs) => {
                        if let Err(e) = self.synchronize(&mut connection).await {
                            warn!("Failed to send sync request to consensus: {e}");
                            continue 'main;
                        }
                        connection_waiter.reset();
                    }
                    // Everything went well, nothing to do.
                    Ok(ProcessingOutcome::Ok) => connection_waiter.reset(),
                }
            }
        }
    }
}
