use crate::utils::{AuthorityState, ConnectionWaiter, SubscriberError, SubscriberResult};
use consensus::{ConsensusOutput, SequenceNumber};
use crypto::traits::VerifyingKey;
use futures::{
    future::try_join_all,
    stream::{FuturesOrdered, StreamExt},
};
use primary::{Batch, BatchDigest};
use std::{cmp::Ordering, net::SocketAddr, sync::Arc};
use store::Store;
use tokio::{net::TcpStream, sync::mpsc::Sender, task::JoinHandle};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, info, warn};
use worker::{SerializedBatchMessage, WorkerMessage};

/// The `Subscriber` receives certificates sequenced by the consensus and execute every
/// transaction it references. We assume that the messages we receives from consensus has
/// already been authenticated (ie. they really come from a trusted consensus node) and
/// integrity-validated (ie. no corrupted messages).
pub struct Subscriber<State: AuthorityState, PublicKey: VerifyingKey> {
    /// The network address of the consensus node.
    address: SocketAddr,
    /// The temporary storage holding all transactions' data (that may be too big to hold in memory).
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

impl<State, PublicKey> Subscriber<State, PublicKey>
where
    State: AuthorityState + Send + Sync + 'static,
    PublicKey: VerifyingKey,
{
    /// Create a new subscriber with the input authority state.
    pub fn new(
        address: SocketAddr,
        store: Store<BatchDigest, SerializedBatchMessage>,
        state: Arc<State>,
        tx_batch_loader: Sender<ConsensusOutput<PublicKey>>,
    ) -> SubscriberResult<Self> {
        info!("Consensus client connecting to {}", address);

        // Ensure there is a single consensus client modifying the state.
        if !state.ask_consensus_write_lock() {
            return Err(SubscriberError::OnlyOneConsensusClientPermitted);
        }

        // Load the last consensus index from storage.
        let last_consensus_index = 0;

        // Return a consensus client only if all went well (safety-critical).
        Ok(Self {
            address,
            store,
            state,
            tx_batch_loader,
            last_consensus_index,
        })
    }

    /// Spawn the subscriber  in a new tokio task.
    pub fn spawn(mut subscriber: Self) -> JoinHandle<SubscriberResult<()>> {
        info!("Consensus subscriber connecting to {}", subscriber.address);
        tokio::spawn(async move { subscriber.run().await })
    }

    /// Synchronize with the consensus in case we missed part of its output sequence.
    /// It is safety-critical that we process the consensus' outputs in the complete
    /// and right order. This function reads the consensus outputs out of a stream and
    /// return them in the right order.
    async fn synchronize(
        connection: &mut Framed<TcpStream, LengthDelimitedCodec>,
        last_known_client_index: u64,
        last_known_server_index: u64,
    ) -> SubscriberResult<Vec<ConsensusOutput<PublicKey>>> {
        let mut next_ordinary_sequence = last_known_server_index + 1;
        let mut next_catchup_sequence = last_known_client_index + 1;
        let mut buffer = Vec::new();
        let mut sequence = Vec::new();
        loop {
            let bytes = connection.next().await.unwrap().unwrap();
            let output: ConsensusOutput<PublicKey> = bincode::deserialize(&bytes).unwrap();
            let consensus_index = output.consensus_index;

            if consensus_index == next_ordinary_sequence {
                buffer.push(output);
                next_ordinary_sequence += 1;
            } else if consensus_index == next_catchup_sequence {
                sequence.push(output);
                next_catchup_sequence += 1;
            } else {
                return Err(SubscriberError::UnexpectedConsensusIndex(consensus_index));
            }

            if consensus_index == last_known_server_index {
                break;
            }
        }

        sequence.extend(buffer);
        Ok(sequence)
    }

    /// Process a single consensus output message. If we realize we are missing part of the sequence,
    /// we first sync every missing output and return them on the right order.
    async fn handle_consensus_message(
        &self,
        message: &ConsensusOutput<PublicKey>,
        connection: &mut Framed<TcpStream, LengthDelimitedCodec>,
    ) -> SubscriberResult<Vec<ConsensusOutput<PublicKey>>> {
        let consensus_index = message.consensus_index;

        // Check that the latest consensus index is as expected; otherwise synchronize.
        let need_to_sync = match self.last_consensus_index.cmp(&consensus_index) {
            Ordering::Greater => {
                // That is fine, it may happen when the consensus node crashes and recovers.
                debug!("Consensus index of authority bigger than expected");
                return Ok(Vec::default());
            }
            Ordering::Less => {
                debug!("Authority is synchronizing missed sequenced certificates");
                true
            }
            Ordering::Equal => false,
        };

        // Send the certificate to the batch loader to download all transactions' data.
        self.tx_batch_loader
            .send(message.clone())
            .await
            .expect("Failed to send message ot batch loader");

        // Synchronize missing consensus outputs if we need to.
        if need_to_sync {
            let last_known_client_index = self.last_consensus_index;
            let last_known_server_index = message.consensus_index;
            Self::synchronize(connection, last_known_client_index, last_known_server_index).await
        } else {
            Ok(vec![message.clone()])
        }
    }

    /// Execute every transaction referenced by a specific consensus message.
    async fn execute_transactions(
        &self,
        message: &ConsensusOutput<PublicKey>,
    ) -> SubscriberResult<()> {
        // The store should now hold all transaction data referenced by the input certificate.
        for digest in message.certificate.header.payload.keys() {
            let batch = match self.store.read(*digest).await? {
                Some(x) => x,
                None => {
                    // If two certificates contain the exact same batch (eg. by the actions of a Byzantine
                    // consensus node), some correct client may already have deleted the batch from they temporary
                    // storage while others may not. This is not a problem, we can simply ignore the second batch
                    // since there is no point in executing twice the same transactions (as the second execution
                    // attempt will always fail).
                    debug!("Duplicate batch {digest}");
                    continue;
                }
            };

            // Deserialize the consensus workers' batch message to retrieve a list of transactions.
            let transactions = match bincode::deserialize(&batch) {
                Ok(WorkerMessage::<PublicKey>::Batch(Batch(x))) => x,
                Ok(_) => return Err(SubscriberError::UnexpectedProtocolMessage),
                Err(e) => return Err(SubscriberError::SerializationError(e)),
            };

            for transaction in transactions {
                // The consensus simply orders bytes, so we first need to deserialize the transaction.
                // If the deserialization fail it is safe to ignore the transaction since all correct clients
                // will do the same. Remember that a bad authority or client may input random bytes to the consensus.
                let command = match bincode::deserialize(&transaction) {
                    Ok(x) => x,
                    Err(e) => {
                        debug!("Failed to deserialize transaction: {e}");
                        continue;
                    }
                };

                // TODO: Return to the result to the higher level client.
                // TODO: Should we execute on another task so we can keep downloading batches while we execute?
                let result = self
                    .state
                    .handle_consensus_transaction(message.consensus_index, command)
                    .await
                    .map_err(|e| SubscriberError::from(e));

                match &result {
                    Err(SubscriberError::ClientExecutionError(e)) => {
                        // We may want to log the errors that are the user's fault (i.e., that are neither our fault
                        // or the fault of consensus) for debug purposes. It is safe to continue by ignoring those
                        // certificates/transactions since all honest subscribers will do the same.
                        debug!("{e}");
                        continue;
                    }
                    _ => (),
                }

                // We must take special care to errors that are our fault, such as storage errors. We may be the
                // only authority experiencing it, and thus cannot continue to process certificates until the
                // problem is fixed.
                result?;
            }
        }
        Ok(())
    }

    /// Wait for particular data to become available in the storage and then returns.
    async fn waiter<T>(
        missing: Vec<BatchDigest>,
        store: &Store<BatchDigest, SerializedBatchMessage>,
        deliver: T,
    ) -> SubscriberResult<T> {
        let waiting: Vec<_> = missing.into_iter().map(|x| store.notify_read(x)).collect();
        try_join_all(waiting)
            .await
            .map(|_| deliver)
            .map_err(SubscriberError::from)
    }

    /// Main loop connecting to the consensus to listen to sequence messages.
    async fn run(&mut self) -> SubscriberResult<()> {
        let mut waiting = FuturesOrdered::new();

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

            // Listen to sequenced consensus message and process them.
            loop {
                tokio::select! {
                    // Receive the ordered sequence of consensus messages from a consensus node.
                    result = connection.next() => {
                        let message = match result {
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

                        // Process the consensus message (synchronize missing messages, download transaction data).
                        let sequence = self.handle_consensus_message(&message, &mut connection).await?;

                        // Update the latest consensus index. The state will atomically persist the change when
                        // executing the transaction. It is important to increment the consensus index before
                        // deserializing the transaction data because the consensus core will increment its own
                        // index regardless of deserialization or other application-specific failures.
                        self.last_consensus_index += sequence.len() as u64;

                        // Wait for the transaction data to be available in the store. We will then execute the transactions.
                        for message in sequence {
                            let digests = message.certificate.header.payload.keys().cloned().collect();
                            let future = Self::waiter(digests, &self.store, message);
                            waiting.push(future);
                        }

                        // Reset the connection timeout delay.
                        connection_waiter.reset();
                    },

                    // Receive here consensus messages for which we have downloaded all transactions data.
                    // TODO: It is not nice that we do not pull these futures in case we loose connection with
                    // the consensus node (see the outer loop labeled 'main').
                    Some(result) = waiting.next() => {
                        let message = result?;

                        // Execute all transactions associated with the consensus output message. This function
                        // also persist the necessary data to enable crash-recovery.
                        self.execute_transactions(&message).await?;

                        // Cleanup the temporary persistent storage.
                        for digest in message.certificate.header.payload.into_keys() {
                            self.store.remove(digest).await;
                        }
                    }
                }
            }
        }
    }
}
