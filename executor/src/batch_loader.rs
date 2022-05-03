// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    errors::{SubscriberError, SubscriberResult},
    DEFAULT_CHANNEL_SIZE,
};
use blake2::digest::Update;
use config::WorkerId;
use consensus::ConsensusOutput;
use crypto::traits::VerifyingKey;
use futures::stream::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};
use store::Store;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};
use tonic::transport::Channel;
use tracing::warn;
use types::{BatchDigest, BincodeEncodedPayload, ClientBatchRequest, WorkerToWorkerClient};
use worker::SerializedBatchMessage;

/// Download transactions data from the consensus workers and notifies the called when the job is done.
pub struct BatchLoader<PublicKey: VerifyingKey> {
    /// The temporary storage holding all transactions' data (that may be too big to hold in memory).
    store: Store<BatchDigest, SerializedBatchMessage>,
    /// Receive consensus outputs for which to download the associated transaction data.
    rx_input: Receiver<ConsensusOutput<PublicKey>>,
    /// The network addresses of the consensus workers.
    addresses: HashMap<WorkerId, SocketAddr>,
    /// A map of connections with the consensus workers.
    connections: HashMap<WorkerId, Sender<Vec<BatchDigest>>>,
}

impl<PublicKey: VerifyingKey> BatchLoader<PublicKey> {
    /// Spawn a new batch loaded in a dedicated tokio task.
    pub fn spawn(
        store: Store<BatchDigest, SerializedBatchMessage>,
        rx_input: Receiver<ConsensusOutput<PublicKey>>,
        addresses: HashMap<WorkerId, SocketAddr>,
    ) -> JoinHandle<SubscriberResult<()>> {
        tokio::spawn(async move {
            Self {
                store,
                rx_input,
                addresses,
                connections: HashMap::new(),
            }
            .run()
            .await
        })
    }

    /// Receive consensus messages for which we need to download the associated transaction data.
    async fn run(&mut self) -> SubscriberResult<()> {
        while let Some(message) = self.rx_input.recv().await {
            let certificate = &message.certificate;

            // Send a request for every batch referenced by the certificate.
            // TODO: Can we write it better without allocating a HashMap every time?
            let mut map = HashMap::with_capacity(certificate.header.payload.len());
            for (digest, worker_id) in certificate.header.payload.iter() {
                map.entry(*worker_id).or_insert_with(Vec::new).push(*digest);
            }
            for (worker_id, digests) in map {
                let address = self
                    .addresses
                    .get(&worker_id)
                    .ok_or(SubscriberError::UnexpectedWorkerId(worker_id))?;

                let sender = self.connections.entry(worker_id).or_insert_with(|| {
                    let (sender, receiver) = channel(DEFAULT_CHANNEL_SIZE);
                    SyncConnection::spawn::<PublicKey>(*address, self.store.clone(), receiver);
                    sender
                });

                sender
                    .send(digests)
                    .await
                    .expect("Sync connections are kept alive and never die");
            }
        }
        Ok(())
    }
}

/// Connect (and maintain a connection) with a specific worker. Then download batches from that
/// specific worker.
struct SyncConnection {
    /// The address of the worker to connect with.
    address: SocketAddr,
    /// The temporary storage holding all transactions' data (that may be too big to hold in memory).
    store: Store<BatchDigest, SerializedBatchMessage>,
    /// Receive the batches to download from the worker.
    rx_request: Receiver<Vec<BatchDigest>>,
    /// Keep a set of requests already made to avoid asking twice for the same batch.
    to_request: HashSet<BatchDigest>,
}

impl SyncConnection {
    /// Spawn a new worker connection in a dedicated tokio task.
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
                to_request: HashSet::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop keeping the connection with a worker alive and receive batches to download.
    async fn run(&mut self) {
        let url = format!("http://{}", self.address);
        let channel = Channel::from_shared(url)
            .expect("URI should be valid")
            .connect_lazy();
        let mut client = WorkerToWorkerClient::new(channel);

        while let Some(digests) = self.rx_request.recv().await {
            // Filter digests that we already requested.
            for digest in digests {
                self.to_request.insert(digest);
            }

            let missing = self.to_request.iter().copied().collect();
            // Request the batch from the worker.
            let message = ClientBatchRequest(missing);
            //TODO wrap this call in the retry
            let mut stream = match client
                .client_batch_request(BincodeEncodedPayload::try_from(&message).unwrap())
                .await
            {
                Ok(stream) => stream.into_inner(),
                Err(e) => {
                    warn!(
                        "Failed to send sync request to worker {}: {e}",
                        self.address
                    );
                    continue;
                }
            };

            // Receive the batch data from the worker.
            while let Some(result) = stream.next().await {
                match result {
                    Ok(batch) => {
                        let batch = batch.payload;
                        // Store the batch in the temporary store.
                        // TODO: We can probably avoid re-computing the hash of the bach since we trust the worker.
                        let digest =
                            BatchDigest::new(crypto::blake2b_256(|hasher| hasher.update(&batch)));
                        self.store.write(digest, batch.to_vec()).await;

                        // Cleanup internal state.
                        self.to_request.remove(&digest);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to receive batch reply from worker {}: {e}",
                            self.address
                        );
                    }
                }
            }
        }
    }
}
