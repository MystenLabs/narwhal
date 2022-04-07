use crate::{utils::ConnectionWaiter, DEFAULT_CHANNEL_SIZE};
use blake2::digest::Update;
use bytes::Bytes;
use config::WorkerId;
use consensus::ConsensusOutput;
use crypto::{traits::VerifyingKey, Hash};
use futures::{
    future::try_join_all,
    stream::{FuturesOrdered, StreamExt as _},
    SinkExt, StreamExt as _,
};
use primary::{BatchDigest, Certificate, CertificateDigest};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    net::SocketAddr,
};
use store::{Store, StoreError};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, error, warn};
use worker::{SerializedBatchMessage, WorkerMessage};

pub struct BatchLoader<PublicKey: VerifyingKey> {
    store: Store<BatchDigest, SerializedBatchMessage>,
    rx_input: Receiver<ConsensusOutput<PublicKey>>,
    tx_output: Sender<ConsensusOutput<PublicKey>>,
    already_requested: HashSet<CertificateDigest>,
    addresses: HashMap<WorkerId, SocketAddr>,
    connections: HashMap<WorkerId, Sender<Vec<BatchDigest>>>,
}

impl<PublicKey: VerifyingKey> BatchLoader<PublicKey> {
    /// Wait for particular data to become available in the storage and then returns.
    async fn waiter<T>(
        missing: Vec<BatchDigest>,
        store: &Store<BatchDigest, SerializedBatchMessage>,
        deliver: T,
    ) -> Result<T, StoreError> {
        let waiting: Vec<_> = missing.into_iter().map(|x| store.notify_read(x)).collect();
        try_join_all(waiting).await.map(|_| deliver)
    }

    async fn run(&mut self) -> Result<(), StoreError> {
        let mut waiting = FuturesOrdered::new();

        loop {
            tokio::select! {
                Some(message) = self.rx_input.recv() => {
                    let certificate = &message.certificate;

                    // Send a request for every batch referenced by the certificate.
                    // TODO: Can probably write it better
                    let mut map = HashMap::new();
                    for (digest, worker_id) in certificate.header.payload.iter() {
                        map.entry(*worker_id).or_insert_with(Vec::new).push(*digest);
                    }
                    for (worker_id, digests) in map {
                        let sender = self.connections.entry(worker_id).or_insert_with(|| {
                            let (sender, receiver) = channel(DEFAULT_CHANNEL_SIZE);
                            let address = self.addresses.get(&worker_id).unwrap(); // TODO: unwrap
                            SyncConnection::spawn::<PublicKey>(*address, self.store.clone(), receiver);
                            sender
                        });
                        sender.send(digests).await.expect("Failed to send message to sync connection");
                    }

                    // Mark the certificate as pending until we get all the batches it references.
                    if self.already_requested.insert(certificate.digest()) {
                        let digests = certificate.header.payload.keys().cloned().collect();
                        let future = Self::waiter(digests, &self.store, message);
                        waiting.push(future);
                    }
                },

                Some(result) = waiting.next() => {
                    let message = result?;

                    // Cleanup internal state. The persistent storage is cleaned after processing all
                    // transactions of the batch.
                    self.already_requested.remove(&message.certificate.digest());

                    // All the transactions referenced by this certificate are ready for processing.
                    self.tx_output.send(message).await.expect("Failed to send certificate");
                }
            }
        }
    }
}

struct SyncConnection {
    address: SocketAddr,
    store: Store<BatchDigest, SerializedBatchMessage>,
    rx_request: Receiver<Vec<BatchDigest>>,
}

impl SyncConnection {
    pub fn spawn<PublicKey: VerifyingKey>(
        address: SocketAddr,
        store: Store<BatchDigest, SerializedBatchMessage>,
        rx_request: Receiver<Vec<BatchDigest>>,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                store,
                rx_request,
            }
            .run::<PublicKey>()
            .await;
        });
    }

    async fn run<PublicKey: VerifyingKey>(&mut self) -> Result<(), Box<dyn Error>> {
        // The connection waiter ensures we do not attempt to reconnect immediately after failure.
        let mut connection_waiter = ConnectionWaiter::default();

        // Continuously connects to the worker.
        'main: loop {
            // Wait a bit before re-attempting connections.
            connection_waiter.wait().await;

            // Connect to the worker.
            let mut connection = match TcpStream::connect(self.address).await {
                Ok(x) => Framed::new(x, LengthDelimitedCodec::new()),
                Err(e) => {
                    warn!(
                        "Failed to connect to worker (retry {}): {e}",
                        connection_waiter.status(),
                    );
                    continue 'main;
                }
            };

            // Listen to sync request and update the store with the replies.
            loop {
                tokio::select! {
                    Some(digests) = self.rx_request.recv() => {
                        let message = WorkerMessage::<PublicKey>::ClientBatchRequest(digests);
                        let serialized = bincode::serialize(&message).expect("Failed to serialize request");
                        if let Err(e) = connection.send(Bytes::from(serialized)).await {
                            warn!("Failed to send sync request to worker {}: {e}", self.address);
                            continue 'main;
                        }
                    },

                    Some(result) = connection.next() => {
                        match result {
                            Ok(batch) => {
                                // TODO: We can probably avoid re-computing the hash of the bach since we trust the worker.
                                let digest = BatchDigest::new(crypto::blake2b_256(|hasher| hasher.update(&batch)));
                                self.store.write(digest, batch.to_vec()).await;
                            },
                            Err(e) => {
                                warn!("Failed to receive batch reply from worker {}: {e}", self.address);
                                continue 'main;
                            }
                        }
                    }
                }
            }
        }
    }
}
