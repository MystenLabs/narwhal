// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crypto::{traits::VerifyingKey, Hash};
use futures::{
    future::BoxFuture,
    stream::{FuturesOrdered, StreamExt},
    FutureExt,
};
use primary::{
    Batch, BlockCommand, BlockResult, Certificate, CertificateDigest, GetBlockResponse, Round,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};

/// Represents the subscriber's unique id.
pub type SubscriberId = usize;

/// A global sequence number assigned to every transaction.
pub type SequenceNumber = u64;

/// The output format understandable by subscribers.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConsensusOutput {
    #[serde(with = "serde_bytes")]
    pub message: Vec<u8>,
    pub sequence_number: SequenceNumber,
}

/// The messages sent by the subscriber server to the sequencer core to notify
/// the core of a new subscriber.
#[derive(Debug)]
pub struct SubscriberMessage(Sender<ConsensusOutput>, SubscriberId);

/// Receive sequenced certificates from consensus, fetch the transactions it reference, and
/// ships them to any listening subscriber.
pub struct SubscriberCore<PublicKey: VerifyingKey> {
    /// Receive users' certificates to sequence
    rx_input: Receiver<Certificate<PublicKey>>,
    /// Communicate with subscribers to update with the output of the sequence.
    rx_subscriber: Receiver<SubscriberMessage>,
    /// Communicate with the `BlockWaiter` to fetch transactions' data.
    tx_block_waiter: Sender<BlockCommand>,

    /// The global consensus index.
    consensus_index: SequenceNumber,
    /// The latest committed round of each validator.
    last_committed: HashMap<PublicKey, Round>,

    /// Hold a channel to communicate with each subscriber.
    subscribers: HashMap<SubscriberId, Sender<ConsensusOutput>>,
    /// The current number of subscribers.
    subscribers_count: usize,
    /// The maximum number of subscribers.
    max_subscribers: usize,
}

impl<PublicKey: VerifyingKey> SubscriberCore<PublicKey> {
    /// Cerate a new subscriber core and spawn it in a new tokio task.
    pub fn spawn(
        rx_input: Receiver<Certificate<PublicKey>>,
        rx_subscriber: Receiver<SubscriberMessage>,
        tx_block_waiter: Sender<BlockCommand>,
        max_subscribers: usize,
    ) -> JoinHandle<BlockResult<()>> {
        tokio::spawn(async move {
            Self {
                rx_input,
                rx_subscriber,
                tx_block_waiter,
                consensus_index: SequenceNumber::default(),
                last_committed: HashMap::new(),
                subscribers: HashMap::new(),
                subscribers_count: 0,
                max_subscribers,
            }
            .run()
            .await
        })
    }

    /// Register a new subscriber.
    fn register_subscriber(&mut self, message: SubscriberMessage) {
        match self.subscribers_count < self.max_subscribers {
            true => {
                let SubscriberMessage(sender, id) = message;
                self.subscribers.insert(id, sender);
                self.subscribers_count += 1;
                log::debug!(
                    "Registered subscriber {:?} (total subscribers count: {})",
                    id,
                    self.subscribers_count
                );
            }
            false => log::debug!("Cannot accept more subscribers (limit reached)"),
        }
    }

    /// Fetch the transactions' data referenced by a certificate.
    async fn fetch_transactions<'a>(
        &mut self,
        certificate_round: Round,
        certificate_id: CertificateDigest,
    ) -> BoxFuture<'a, BlockResult<(Round, Vec<Batch>)>> {
        let (sender, receiver) = channel(1);
        let command = BlockCommand::GetBlock {
            id: certificate_id,
            sender,
        };

        self.tx_block_waiter
            .send(command)
            .await
            .expect("Failed to send command to block waiter");

        Self::waiter(certificate_round, receiver).boxed()
    }

    /// Wait for the `BlockWaiter` to retrieve the transactions referenced by a certificate.
    async fn waiter(
        certificate_round: Round,
        mut receiver: Receiver<BlockResult<GetBlockResponse>>,
    ) -> BlockResult<(Round, Vec<Batch>)> {
        receiver
            .recv()
            .await
            .expect("Failed to receive response from block waiter")
            .map(|GetBlockResponse { batches, .. }| {
                (
                    certificate_round,
                    batches.into_iter().map(|x| x.transactions).collect(),
                )
            })
    }

    /// Keep in memory the last committed round of each authority.
    fn update_last_committed(&mut self, certificate: Certificate<PublicKey>) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = std::cmp::max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());
    }

    /// Persist the last committed round per authority and the global consensus index.
    async fn persist_state(&self) {
        // Atomically the last committed round per validator and the last consensus index. These are
        // loaded by the consensus upon rebooting. We also need to atomically persist the last
        // consensus index.
        unimplemented!();
    }

    /// Update all subscribers with the latest certificate.
    async fn update_subscribers(&mut self, batch: Batch) {
        // TODO: It is probably better to ship the whole batch to the subscribers.
        for transaction in batch.0 {
            // Convert the transaction in a format understandable by the subscriber.
            let output = ConsensusOutput {
                message: transaction,
                sequence_number: self.consensus_index,
            };

            // Increment the consensus index.
            self.consensus_index += 1;

            // Notify the subscribers of the new output. If a subscriber's channel is full (the subscriber
            // is slow), we simply skip this output. The subscriber will eventually sync to catch up.
            let mut to_drop = Vec::new();
            for (id, subscriber) in &self.subscribers {
                if subscriber.is_closed() {
                    to_drop.push(*id);
                    continue;
                }
                if subscriber.capacity() > 0 && subscriber.send(output.clone()).await.is_err() {
                    to_drop.push(*id);
                }
            }

            // Cleanup the list subscribers that dropped the connection.
            for id in to_drop {
                self.subscribers.remove(&id);
                self.subscribers_count -= 1;
            }
        }
    }

    /// Main loop ordering input bytes.
    async fn run(&mut self) -> BlockResult<()> {
        // Hold futures until we fetch the transactions referenced by certificates.
        let mut waiting = FuturesOrdered::new();

        // Main loop listening to new sequenced certificates and shipping the transactions they
        // reference to subscribers.
        loop {
            tokio::select! {
                // Receive ordered certificates.
                Some(certificate) = self.rx_input.recv() => {
                    // Fetch the transactions referenced by the certificate.
                    let round = certificate.round();
                    let id = certificate.digest();
                    let future = self.fetch_transactions(round, id).await;
                    waiting.push(future);

                    // Update (in memory) the latest committed round per authority.
                    self.update_last_committed(certificate);
                },

                // Receive subscribers to update with the consensus' output.
                Some(message) = self.rx_subscriber.recv() => self.register_subscriber(message),

                // Bytes are ready to be delivered, notify the subscribers.
                Some(message) = waiting.next() => {
                    let (certificate_round, batches) = message?;

                    // Once in a while persist the current state. Not doing it frequently simply
                    // means that the node will do more work upon rebooting (after a crash). All
                    // subscribers are able to handle duplicate messages (efficiently).
                    if certificate_round % 100 == 0 {
                        self.persist_state().await;
                    }

                    // Update the subscribers with sequenced transactions.
                    for batch in batches {
                        self.update_subscribers(batch).await;
                    }
                }
            }
        }
    }
}
