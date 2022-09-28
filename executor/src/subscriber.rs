// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::notifier::BatchIndex;
use crate::{errors::SubscriberResult, metrics::ExecutorMetrics};

use config::{Committee, SharedWorkerCache, WorkerId};
use consensus::ConsensusOutput;
use crypto::{NetworkPublicKey, PublicKey};

use futures::future::join;
use futures::stream::FuturesOrdered;
use futures::FutureExt;
use futures::StreamExt;

use network::P2pNetwork;
use network::Primary2WorkerRpc;

use prometheus::IntGauge;
use std::future::Future;
use std::{sync::Arc, time::Duration};

use tokio::time::Instant;
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
};
use tracing::{debug, error};
use tracing::{info, instrument};
use types::{metered_channel, Batch, BatchDigest, ReconfigureNotification};

/// The `Subscriber` receives certificates sequenced by the consensus and waits until the
/// downloaded all the transactions references by the certificates; it then
/// forward the certificates to the Executor Core.
pub struct Subscriber {
    /// Receive reconfiguration updates.
    rx_reconfigure: watch::Receiver<ReconfigureNotification>,
    /// A channel to receive consensus messages.
    rx_consensus: metered_channel::Receiver<ConsensusOutput>,
    /// Ordered batches for the consumer
    tx_notifier: metered_channel::Sender<(BatchIndex, Batch)>,
    /// The metrics handler
    metrics: Arc<ExecutorMetrics>,
    fetcher: Fetcher,
}

struct Fetcher {
    name: PublicKey,
    network: P2pNetwork,
    worker_cache: SharedWorkerCache,
    committee: Committee,
    metrics: Arc<ExecutorMetrics>,
}

impl Subscriber {
    /// Returns the max amount of pending consensus messages we should expect.
    const MAX_PENDING_PAYLOADS: usize = 32;

    /// Spawn a new subscriber in a new tokio task.
    #[must_use]
    pub fn spawn(
        name: PublicKey,
        network: oneshot::Receiver<P2pNetwork>,
        worker_cache: SharedWorkerCache,
        committee: Committee,
        rx_reconfigure: watch::Receiver<ReconfigureNotification>,
        rx_consensus: metered_channel::Receiver<ConsensusOutput>,
        tx_notifier: metered_channel::Sender<(BatchIndex, Batch)>,
        metrics: Arc<ExecutorMetrics>,
        restored_consensus_output: Vec<ConsensusOutput>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            // This is ugly but has to be done this way for now
            // Currently network incorporate both server and client side of RPC interface
            // To construct server side we need to set up routes first, which requires starting Primary
            // Some cleanup is needed
            let network = network.await.expect("Failed to receive network");
            info!("Starting subscriber");

            let fetcher = Fetcher {
                name,
                network,
                worker_cache,
                committee,
                metrics: metrics.clone(),
            };
            let subscriber = Subscriber {
                rx_reconfigure,
                rx_consensus,
                metrics,
                tx_notifier,
                fetcher,
            };
            subscriber
                .run(restored_consensus_output)
                .await
                .expect("Failed to run subscriber")
        })
    }

    /// Main loop connecting to the consensus to listen to sequence messages.
    async fn run(
        mut self,
        restored_consensus_output: Vec<ConsensusOutput>,
    ) -> SubscriberResult<()> {
        // It's important to have the futures in ordered fashion as we want
        // to guarantee that will deliver to the executor the certificates
        // in the same order we received from rx_consensus. So it doesn't
        // mater if we somehow managed to fetch the batches from a later
        // certificate. Unless the earlier certificate's payload has been
        // fetched, no later certificate will be delivered.
        let mut waiting = FuturesOrdered::new();

        // First handle any consensus output messages that were restored due to a restart.
        // This needs to happen before we start listening on rx_consensus and receive messages sequenced after these.
        for message in restored_consensus_output {
            let futures = self.fetcher.fetch_payloads(message);
            for future in futures {
                // todo - limit number pending futures on startup
                waiting.push_back(future);
                self.metrics.subscriber_recovered_certificates_count.inc();
            }
        }

        // Listen to sequenced consensus message and process them.
        loop {
            tokio::select! {
                // Receive the ordered sequence of consensus messages from a consensus node.
                Some(message) = self.rx_consensus.recv(), if waiting.len() < Self::MAX_PENDING_PAYLOADS => {
                    // We can schedule more then MAX_PENDING_PAYLOADS payloads but
                    // don't process more consensus messages when more
                    // then MAX_PENDING_PAYLOADS is pending
                    for future in self.fetcher.fetch_payloads(message) {
                        waiting.push_back(future);
                    }
                },

                // Receive here consensus messages for which we have downloaded all transactions data.
                (message, permit) = join(waiting.next(), self.tx_notifier.reserve()), if !waiting.is_empty() => {
                    if let Ok(permit) = permit {
                        permit.send(message.expect("We don't poll empty queue"));
                    } else {
                        error!("tx_notifier closed");
                        return Ok(());
                    }
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

            self.metrics
                .waiting_elements_subscriber
                .set(waiting.len() as i64);
        }
    }
}

impl Fetcher {
    /// Returns ordered vector of futures for downloading individual payloads for certificate
    /// Order of futures returned follows order of payloads in the certificate
    /// See fetch_payload for more details
    fn fetch_payloads(
        &self,
        deliver: ConsensusOutput,
    ) -> Vec<impl Future<Output = (BatchIndex, Batch)> + '_> {
        debug!("Fetching payload for {:?}", deliver);
        let mut ret = vec![];
        let authorities = deliver.certificate.signed_authorities(&self.committee);
        for (batch_index, (digest, worker_id)) in
            deliver.certificate.header.payload.iter().enumerate()
        {
            let batch_index = BatchIndex {
                consensus_output: deliver.clone(),
                next_certificate_index: deliver.consensus_index,
                batch_index: batch_index as u64,
            };
            ret.push(
                self.fetch_payload(*digest, *worker_id, authorities.clone())
                    .map(move |batch| (batch_index, batch)),
            );
        }

        ret
    }

    /// Fetches single payload from network
    /// This future performs infinite retries and blocks until Batch is available
    /// As an optimization it tries to download from local worker first, but then fans out
    /// requests to remote worker if not found locally
    #[instrument(level = "debug", skip_all, fields(digest = % digest, worker_id = % worker_id))]
    async fn fetch_payload(
        &self,
        digest: BatchDigest,
        worker_id: WorkerId,
        authorities: Vec<PublicKey>,
    ) -> Batch {
        if let Some(payload) = self.try_fetch_locally(digest, worker_id).await {
            return payload;
        }
        let _timer = self.metrics.subscriber_remote_fetch_latency.start_timer();
        let mut stagger = Duration::from_secs(0);
        let mut futures = vec![];
        for authority in &authorities {
            let worker = self.worker_cache.load().worker(authority, &worker_id);
            let worker = match worker {
                Ok(worker) => worker.name,
                Err(err) => {
                    error!(
                        "Worker {} not found for authority {}: {:?}",
                        worker_id, authority, err
                    );
                    continue;
                }
            };
            let future = self.fetch_from_worker(stagger, worker, digest, authority.clone());
            futures.push(future.boxed());
            stagger += Duration::from_secs(1);
        }
        let (batch, _, _) = futures::future::select_all(futures).await;
        batch
    }

    #[instrument(level = "debug", skip_all, fields(digest = % digest, worker_id = % worker_id))]
    async fn try_fetch_locally(&self, digest: BatchDigest, worker_id: WorkerId) -> Option<Batch> {
        let _timer = self.metrics.subscriber_local_fetch_latency.start_timer();
        let worker = self
            .worker_cache
            .load()
            .worker(&self.name, &worker_id)
            .expect("Own worker not found in cache")
            .name;
        let payload = self.network.get_payload(&worker, digest).await;
        match payload {
            Ok(Some(batch)) => {
                debug!("Payload {} found locally", digest);
                self.metrics.subscriber_local_hit.inc();
                return Some(batch);
            }
            Ok(None) => debug!("Payload {} not found locally", digest),
            Err(err) => error!("Error communicating with out own worker: {}", err),
        }
        None
    }

    /// This future performs fetch from given worker
    /// This future performs infinite retries with exponential backoff
    /// You can specify stagger_delay before request is issued
    #[instrument(level = "debug", skip_all, fields(stagger_delay = ? stagger_delay, worker = % worker, digest = % digest, authority = % authority))]
    async fn fetch_from_worker(
        &self,
        stagger_delay: Duration,
        worker: NetworkPublicKey,
        digest: BatchDigest,
        authority: PublicKey,
    ) -> Batch {
        tokio::time::sleep(stagger_delay).await;
        let max_timeout = Duration::from_secs(60);
        let mut timeout = Duration::from_secs(10);
        loop {
            let deadline = Instant::now() + timeout;
            let get_payload_guard =
                PendingGuard::make_inc(&self.metrics.pending_remote_get_payload);
            let payload =
                tokio::time::timeout_at(deadline, self.network.get_payload(&worker, digest)).await;
            drop(get_payload_guard);
            match payload {
                Ok(Ok(Some(payload))) => return payload,
                Ok(Ok(None)) => error!("[Protocol violation] Payload {} was not found at worker {} while authority {} signed certificate", digest, worker, authority),
                Ok(Err(err)) => debug!(
                    "Error retrieving payload {} from {}: {}",
                    digest, worker, err
                ),
                Err(_elapsed) => debug!("Timeout retrieving payload {} from {}",
                    digest, worker
                ),
            }
            timeout += timeout / 2;
            timeout = std::cmp::min(max_timeout, timeout);
            // Since the call might have returned before timeout, we wait until originally planned deadline
            tokio::time::sleep_until(deadline).await;
        }
    }
}

// todo - make it generic so that other can reuse
struct PendingGuard<'a> {
    metric: &'a IntGauge,
}

impl<'a> PendingGuard<'a> {
    pub fn make_inc(metric: &'a IntGauge) -> Self {
        metric.inc();
        Self { metric }
    }
}

impl<'a> Drop for PendingGuard<'a> {
    fn drop(&mut self) {
        self.metric.dec()
    }
}
