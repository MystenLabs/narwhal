// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::metrics::WorkerMetrics;
use config::Committee;
#[cfg(feature = "benchmark")]
use std::convert::TryInto;
use std::sync::Arc;
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};
#[cfg(feature = "benchmark")]
use byteorder::{LittleEndian, ReadBytesExt};
use types::{
    error::DagError,
    metered_channel::{Receiver, Sender},
    Batch, ReconfigureNotification, Transaction,
};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The committee information.
    committee: Committee,
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch.
    max_batch_delay: Duration,
    /// Receive reconfiguration updates.
    rx_reconfigure: watch::Receiver<ReconfigureNotification>,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<Batch>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// Metrics handler
    node_metrics: Arc<WorkerMetrics>,
}

impl BatchMaker {
    #[must_use]
    pub fn spawn(
        committee: Committee,
        batch_size: usize,
        max_batch_delay: Duration,
        rx_reconfigure: watch::Receiver<ReconfigureNotification>,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<Batch>,
        node_metrics: Arc<WorkerMetrics>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self {
                committee,
                batch_size,
                max_batch_delay,
                rx_reconfigure,
                rx_transaction,
                tx_message,
                current_batch: Batch(Vec::with_capacity(batch_size * 2)),
                current_batch_size: 0,
                node_metrics,
            }
            .run()
            .await;
        })
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(self.max_batch_delay);
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.0.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal(false).await;
                        timer.as_mut().reset(Instant::now() + self.max_batch_delay);
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.0.is_empty() {
                        self.seal(true).await;
                    }
                    timer.as_mut().reset(Instant::now() + self.max_batch_delay);
                }

                // Trigger reconfigure.
                result = self.rx_reconfigure.changed() => {
                    result.expect("Committee channel dropped");
                    let message = self.rx_reconfigure.borrow().clone();
                    match message {
                        ReconfigureNotification::NewEpoch(new_committee) => {
                            self.committee = new_committee;
                        },
                        ReconfigureNotification::UpdateCommittee(new_committee) => {
                            self.committee = new_committee;

                        },
                        ReconfigureNotification::Shutdown => return
                    }
                    tracing::debug!("Committee updated to {}", self.committee);
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self, timeout: bool) {
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .0
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Batch = Batch(self.current_batch.0.drain(..).collect());

        #[cfg(feature = "benchmark")]
        {
            let message = types::WorkerMessage::Batch(batch.clone());
            let serialized =
                bincode::serialize(&message).expect("Failed to serialize our own batch");

            // NOTE: This is one extra hash that is only needed to print the following log entries.
            if let Ok(digest) = types::serialized_batch_digest(&serialized) {
                for id in tx_ids {
                    // NOTE: This log entry is used to compute performance.
                    tracing::info!(
                        "Batch {:?} contains sample tx {}",
                        digest,
                        u64::from_be_bytes(id)
                    );
                }

                // NOTE: This log entry is used to compute performance.
                tracing::info!("Batch {:?} contains {} B", digest, size);
                let tracking_ids: Vec<_> = batch.0.iter().map(|tx| if tx.len() >= 8usize {
                    (&tx[0..8]).read_u64::<LittleEndian>().unwrap()
                } else {
                    0
                }).collect();
                tracing::debug!("List of transaction tracking ids in batch {:?}: {:?}", digest, tracking_ids);
            }
        }

        let reason = if timeout { "timeout" } else { "size_reached" };

        self.node_metrics
            .created_batch_size
            .with_label_values(&[self.committee.epoch.to_string().as_str(), reason])
            .observe(size as f64);

        // Send the batch through the deliver channel for further processing.
        if self.tx_message.send(batch).await.is_err() {
            tracing::debug!("{}", DagError::ShuttingDown);
        }
    }
}
