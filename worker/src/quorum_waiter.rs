// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use config::{SharedCommittee, Stake};
use crypto::traits::VerifyingKey;
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};
use network::CancelHandler;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use types::SerializedBatchMessage;

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

#[derive(Debug)]
pub struct QuorumWaiterMessage<PublicKey> {
    /// A serialized `WorkerMessage::Batch` message.
    pub batch: SerializedBatchMessage,
    /// The cancel handlers to receive the acknowledgments of our broadcast.
    pub handlers: Vec<(PublicKey, CancelHandler<()>)>,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
pub struct QuorumWaiter<PublicKey: VerifyingKey> {
    /// The committee information.
    committee: SharedCommittee<PublicKey>,
    /// The stake of this authority.
    stake: Stake,
    /// Input Channel to receive commands.
    rx_message: Receiver<QuorumWaiterMessage<PublicKey>>,
    /// Channel to deliver batches for which we have enough acknowledgments.
    tx_batch: Sender<SerializedBatchMessage>,
}

impl<PublicKey: VerifyingKey> QuorumWaiter<PublicKey> {
    /// Spawn a new QuorumWaiter.
    pub fn spawn(
        committee: SharedCommittee<PublicKey>,
        stake: Stake,
        rx_message: Receiver<QuorumWaiterMessage<PublicKey>>,
        tx_batch: Sender<Vec<u8>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self {
                committee,
                stake,
                rx_message,
                tx_batch,
            }
            .run()
            .await;
        })
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(wait_for: CancelHandler<()>, deliver: Stake) -> Stake {
        wait_for.await;
        deliver
    }

    /// Main loop.
    async fn run(&mut self) {
        while let Some(QuorumWaiterMessage { batch, handlers }) = self.rx_message.recv().await {
            let mut wait_for_quorum: FuturesUnordered<_> = handlers
                .into_iter()
                .map(|(name, handler)| {
                    let stake = self.committee.stake(&name);
                    Self::waiter(handler, stake)
                })
                .collect();

            // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
            // delivered and we send its digest to the primary (that will include it into
            // the dag). This should reduce the amount of synching.
            let mut total_stake = self.stake;
            while let Some(stake) = wait_for_quorum.next().await {
                total_stake += stake;
                if total_stake >= self.committee.quorum_threshold() {
                    self.tx_batch
                        .send(batch)
                        .await
                        .expect("Failed to deliver batch");
                    break;
                }
            }
        }
    }
}
