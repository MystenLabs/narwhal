// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::primary::Reconfigure;
use config::Committee;
use crypto::traits::VerifyingKey;
use futures::{
    future::try_join_all,
    stream::{futures_unordered::FuturesUnordered, StreamExt as _},
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use store::Store;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    watch,
};
use tracing::error;
use types::{
    error::{DagError, DagResult},
    Certificate, CertificateDigest, HeaderDigest, Round,
};

/// Waits to receive all the ancestors of a certificate before looping it back to the `Core`
/// for further processing.
pub struct CertificateWaiter<PublicKey: VerifyingKey> {
    /// The committee information.
    committee: Committee<PublicKey>,
    /// The persistent storage.
    store: Store<CertificateDigest, Certificate<PublicKey>>,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,
    /// Watch channel notifying of epoch changes, it is only used for cleanup.
    rx_reconfigure: watch::Receiver<Reconfigure<PublicKey>>,
    /// Receives sync commands from the `Synchronizer`.
    rx_synchronizer: Receiver<Certificate<PublicKey>>,
    /// Loops back to the core certificates for which we got all parents.
    tx_core: Sender<Certificate<PublicKey>>,
    /// List of digests (certificates) that are waiting to be processed. Their processing will
    /// resume when we get all their dependencies. The map holds a cancellation `Sender`
    /// which we can use to give up on a certificate.
    pending: HashMap<HeaderDigest, (Round, Sender<()>)>,
}

impl<PublicKey: VerifyingKey> CertificateWaiter<PublicKey> {
    pub fn spawn(
        committee: Committee<PublicKey>,
        store: Store<CertificateDigest, Certificate<PublicKey>>,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_reconfigure: watch::Receiver<Reconfigure<PublicKey>>,
        rx_synchronizer: Receiver<Certificate<PublicKey>>,
        tx_core: Sender<Certificate<PublicKey>>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                consensus_round,
                gc_depth,
                rx_reconfigure,
                rx_synchronizer,
                tx_core,
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for particular data to become available in the storage and then
    /// delivers the specified header.
    async fn waiter(
        missing: Vec<CertificateDigest>,
        store: &Store<CertificateDigest, Certificate<PublicKey>>,
        deliver: Certificate<PublicKey>,
        mut cancel_handle: Receiver<()>,
    ) -> DagResult<Certificate<PublicKey>> {
        let waiting: Vec<_> = missing.into_iter().map(|x| store.notify_read(x)).collect();

        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| deliver).map_err(DagError::from)
            }
            // the request for this certificate is obsolete, for instance because its round is obsolete (GC'd).
            _ = cancel_handle.recv() => Ok(deliver),
        }
    }

    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(certificate) = self.rx_synchronizer.recv() => {
                    if certificate.epoch() < self.committee.epoch() {
                        continue;
                    }

                    // Ensure we process only once this certificate.
                    let header_id = certificate.header.id;
                    if self.pending.contains_key(&header_id) {
                        continue;
                    }

                    // Add the certificate to the waiter pool. The waiter will return it to us when
                    // all its parents are in the store.
                    let wait_for = certificate
                        .header
                        .parents
                        .iter().cloned()
                        .collect();
                    let (tx_cancel, rx_cancel) = channel(1);
                    self.pending.insert(header_id, (certificate.round(), tx_cancel));
                    let fut = Self::waiter(wait_for, &self.store, certificate, rx_cancel);
                    waiting.push(fut);
                }
                Some(result) = waiting.next() => match result {
                    Ok(certificate) => {
                        // TODO [issue #115]: To ensure crash-recovery of consensus, it is not enough to send every
                        // certificate for which their ancestors are in the storage. After recovery, we may also
                        // need to send a all parents certificates with rounds greater then `last_committed`.

                        self.tx_core.send(certificate).await.expect("Failed to send certificate");
                    },
                    Err(e) => {
                        error!("{e}");
                        panic!("Storage failure: killing node.");
                    }
                },
                result = self.rx_reconfigure.changed() => {
                    result.expect("Committee channel dropped");
                    let message = self.rx_reconfigure.borrow_and_update().clone();
                    match message {
                        Reconfigure::NewCommittee(committee) => {
                            self.committee = committee;
                            self.pending.clear();
                        },
                        Reconfigure::Shutdown(_token) => ()
                    }

                }
            }

            // Cleanup internal state. Deliver the certificates waiting on garbage collected ancestors.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let gc_round = round - self.gc_depth;
                for (r, cancel_handle) in self.pending.values() {
                    if *r <= gc_round + 1 {
                        let _ = cancel_handle.send(()).await;
                    }
                }
                self.pending.retain(|_, (r, _)| *r > gc_round + 1);
            }
        }
    }
}
