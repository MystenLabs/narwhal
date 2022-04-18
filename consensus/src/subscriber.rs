// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    store::{ConsensusStore, StoreResult},
    ConsensusOutput, ConsensusSyncRequest,
};
use crypto::traits::VerifyingKey;
use primary::{Certificate, CertificateDigest};
use std::sync::Arc;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::{debug, error};

/// Convenience alias indicating the persistent storage holding certificates.
type CertificateStore<PublicKey> = store::Store<CertificateDigest, Certificate<PublicKey>>;

/// Pushes the consensus output to subscriber clients and helps them to remain up to date.
pub struct SubscriberHandler<PublicKey: VerifyingKey> {
    // The persistent store holding the consensus state.
    consensus_store: Arc<ConsensusStore<PublicKey>>,
    // The persistent store holding the certificates.
    certificate_store: CertificateStore<PublicKey>,
    // Channel to receive the output of consensus.
    rx_sequence: Receiver<ConsensusOutput<PublicKey>>,
    /// Channel to receive sync requests from the client.
    rx_client: Receiver<ConsensusSyncRequest>,
    /// Channel to send new consensus outputs to the client.
    tx_client: Sender<ConsensusOutput<PublicKey>>,
}

impl<PublicKey: VerifyingKey> SubscriberHandler<PublicKey> {
    /// Spawn a new subscriber handler in a dedicated tokio task.
    pub fn spawn(
        consensus_store: Arc<ConsensusStore<PublicKey>>,
        certificate_store: CertificateStore<PublicKey>,
        rx_sequence: Receiver<ConsensusOutput<PublicKey>>,
        rx_client: Receiver<ConsensusSyncRequest>,
        tx_client: Sender<ConsensusOutput<PublicKey>>,
    ) -> JoinHandle<StoreResult<()>> {
        tokio::spawn(async move {
            Self {
                consensus_store,
                certificate_store,
                rx_sequence,
                rx_client,
                tx_client,
            }
            .run()
            .await
        })
    }

    /// Main loop responsible to update the client with the latest consensus outputs and answer
    /// its sync requests.
    async fn run(&mut self) -> StoreResult<()> {
        loop {
            tokio::select! {
                // Forward new consensus outputs to the client.
                Some(message) = self.rx_sequence.recv() => {
                    if self.tx_client.send(message).await.is_err() {
                        debug!("Client connection dropped");
                    }
                },

                // Receive client sync requests.
                Some(request) = self.rx_client.recv() => self
                    .synchronize(request)
                    .await?
            }
        }
    }

    /// Help the subscriber missing chunks of the output sequence to get up to speed.
    async fn synchronize(&mut self, request: ConsensusSyncRequest) -> StoreResult<()> {
        // Load the digests from the consensus store.
        let digests = self
            .consensus_store
            .read_sequenced_certificates(&request.missing)?
            .into_iter()
            .take_while(|x| x.is_some())
            .map(|x| x.unwrap());

        // Load the actual certificates from the certificate store.
        let certificates = self.certificate_store.read_all(digests).await?;

        // Transmit each certificate to the subscriber (in the right order).
        for (certificate, consensus_index) in certificates.into_iter().zip(request.missing) {
            match certificate {
                Some(certificate) => {
                    let message = ConsensusOutput {
                        certificate,
                        consensus_index,
                    };
                    if self.tx_client.send(message).await.is_err() {
                        debug!("Connection dropped by client");
                        break;
                    }
                }
                None => {
                    // TODO: We should return an error and exit the task.
                    error!("Inconsistency between consensus and certificates store");
                    break;
                }
            }
        }
        Ok(())
    }
<<<<<<< HEAD
=======

    /// Main loop interacting with the subscriber.
    async fn run(&mut self) {
        let (tx_output, mut rx_output) = channel(Self::MAX_PENDING_UPDATES);

        // Notify the core of a new subscriber.
        self.tx_subscriber
            .send(tx_output)
            .await
            .expect("Failed to send new subscriber to core");

        // Interact with the subscriber.
        // TODO [issue #120]: Better error handling (we have a lot of prints and breaks here).
        loop {
            tokio::select! {
                // Update the subscriber every time a certificate is sequenced.
                Some(message) = rx_output.recv() => {
                    let serialized = bincode::serialize(&message).expect("Failed to serialize update");
                    if self.socket.send(Bytes::from(serialized)).await.is_err() {
                        debug!("Connection dropped by subscriber {}", self.peer);
                        break;
                    }
                },

                // Receive sync requests form the subscriber.
                Some(buffer) = self.socket.next() => match buffer {
                    Ok(bytes) => match bincode::deserialize(&bytes) {
                        Ok(request) => if let Err(e) = self.synchronize(request).await {
                            error!("{e}");
                        },
                        Err(e) => {
                            debug!("subscriber {} sent malformed sync request: {}", self.peer, e);
                            break;
                        }
                    },
                    Err(e) => {
                        debug!("Error while reading TCP stream: {e}");
                        break;
                    }
                }
            }
        }
        debug!("Connection with subscriber {} closed", self.peer);
    }
>>>>>>> main
}
