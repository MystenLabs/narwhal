// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    errors::{SubscriberError, SubscriberResult},
    try_fut_and_permit,
};
use consensus::ConsensusOutput;
use futures::{future::try_join_all, stream::FuturesOrdered, FutureExt, TryStreamExt};
use store::Store;
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        watch,
    },
    task::JoinHandle,
};
use types::{BatchDigest, ReconfigureNotification, SerializedBatchMessage};

#[cfg(test)]
#[path = "tests/subscriber_tests.rs"]
pub mod subscriber_tests;

/// The `Subscriber` receives certificates sequenced by the consensus and waits until the
/// `BatchLoader` downloaded all the transactions references by the certificates; it then
/// forward the certificates to the Executor Core.
pub struct Subscriber {
    /// The temporary storage holding all transactions' data (that may be too big to hold in memory).
    store: Store<BatchDigest, SerializedBatchMessage>,
    /// Receive reconfiguration updates.
    rx_reconfigure: watch::Receiver<ReconfigureNotification>,
    /// A channel to receive consensus messages.
    rx_consensus: Receiver<ConsensusOutput>,
    /// A channel to the batch loader to download transaction's data.
    tx_batch_loader: Sender<ConsensusOutput>,
    /// A channel to send the complete and ordered list of consensus outputs to the executor. This
    /// channel is used once all transactions data are downloaded.
    tx_executor: Sender<ConsensusOutput>,
}

impl Subscriber {
    /// Spawn a new subscriber in a new tokio task.
    #[must_use]
    pub fn spawn(
        store: Store<BatchDigest, SerializedBatchMessage>,
        rx_reconfigure: watch::Receiver<ReconfigureNotification>,
        rx_consensus: Receiver<ConsensusOutput>,
        tx_batch_loader: Sender<ConsensusOutput>,
        tx_executor: Sender<ConsensusOutput>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self {
                store,
                rx_reconfigure,
                rx_consensus,
                tx_batch_loader,
                tx_executor,
            }
            .run()
            .await
            .expect("Failed to run subscriber")
        })
    }

    /// Wait for particular data to become available in the storage and then returns.
    async fn waiter<T>(
        missing: Vec<BatchDigest>,
        store: Store<BatchDigest, SerializedBatchMessage>,
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

        // Listen to sequenced consensus message and process them.
        loop {
            tokio::select! {
                // Receive the ordered sequence of consensus messages from a consensus node.
                (Some(message), permit) = try_fut_and_permit!(self.rx_consensus.recv().map(Ok), self.tx_batch_loader) => {
                    // Send the certificate to the batch loader to download all transactions' data.
                    permit.send(message.clone());

                    // Wait for the transaction data to be available in the store. We will then forward these
                    // transactions to the Executor Core for execution.
                    let digests = message.certificate.header.payload.keys().cloned().collect();
                    let future = Self::waiter(digests, self.store.clone(), message);
                    waiting.push(future);
                },

                // Receive here consensus messages for which we have downloaded all transactions data.
                (Some(message), permit) = try_fut_and_permit!(waiting.try_next(), self.tx_executor) => {
                    permit.send(message)
                },

                // Check whether the committee changed.
                result = self.rx_reconfigure.changed() => {
                    result.expect("Committee channel dropped");
                    let message = self.rx_reconfigure.borrow().clone();
                    if let ReconfigureNotification::Shutdown = message {
                        return Ok(());
                    }
                }
            }
        }
    }
}
