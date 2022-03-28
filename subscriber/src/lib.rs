// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
mod core;
mod server;
mod store;

use crate::{core::SubscriberCore, store::ConsensusStore};
use crypto::traits::VerifyingKey;
use primary::BlockCommand;
use serde::{Deserialize, Serialize};
use server::SubscriberServer;
use std::{net::SocketAddr, path::Path, sync::Arc};
use tokio::sync::mpsc::{channel, Sender};

/// The default channel size used in the subscriber logic.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

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
pub struct SubscriberMessage(Sender<ConsensusOutput>);

/// Spawn a new subscriber handler.
pub fn spawn_subscriber_handler<PublicKey: VerifyingKey>(
    address: SocketAddr,
    store_path: &Path,
    tx_block_waiter: Sender<BlockCommand>,
    max_subscribers: usize,
) {
    let (_tx_sequenced_certificate, rx_sequenced_certificate) = channel(DEFAULT_CHANNEL_SIZE);
    let (tx_subscriber, rx_subscriber) = channel(DEFAULT_CHANNEL_SIZE);

    // Load the consensus store.
    let store = Arc::new(ConsensusStore::<PublicKey>::open(store_path, None));

    // Spawn the subscriber core handler.
    SubscriberCore::spawn(
        /* rx_input */ rx_sequenced_certificate,
        rx_subscriber,
        tx_block_waiter,
        max_subscribers,
        store.clone(),
    );

    // Spawn the subscriber server.
    SubscriberServer::spawn(address, tx_subscriber, store);
}
