// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{store::ConsensusStore, SubscriberMessage};
use bytes::Bytes;
use crypto::traits::VerifyingKey;
use futures::{stream::StreamExt, SinkExt};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{channel, Sender},
    task::JoinHandle,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

/// A connection with a single subscriber.
struct SubscriberConnection<PublicKey: VerifyingKey> {
    /// Notify the sequencer's core of a new subscriber.
    pub tx_subscriber: Sender<SubscriberMessage>,
    /// The persistent storage. It is only used to help subscribers to sync, this
    /// task never writes to the store.
    _store: Arc<ConsensusStore<PublicKey>>,
    /// The TCP socket connection to the subscriber.
    socket: Framed<TcpStream, LengthDelimitedCodec>,
}

impl<PublicKey: VerifyingKey> SubscriberConnection<PublicKey> {
    /// The number of pending updates that the subscriber can hold in memory.
    const MAX_PENDING_UPDATES: usize = 1_000;

    /// Create a new subscriber server.
    pub fn spawn(
        tx_subscriber: Sender<SubscriberMessage>,
        store: Arc<ConsensusStore<PublicKey>>,
        socket: Framed<TcpStream, LengthDelimitedCodec>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self {
                tx_subscriber,
                _store: store,
                socket,
            }
            .run()
            .await
        })
    }

    /// Main loop interacting with the subscriber.
    async fn run(&mut self) {
        let (tx_output, mut rx_output) = channel(Self::MAX_PENDING_UPDATES);

        // Notify the core of a new subscriber.
        self.tx_subscriber
            .send(SubscriberMessage(tx_output))
            .await
            .expect("Failed to send new subscriber to core");

        // Interact with the subscriber.
        loop {
            tokio::select! {
                // Update the subscriber every time a certificate is sequenced.
                Some(message) = rx_output.recv() => {
                    let serialized = bincode::serialize(&message).expect("Failed to serialize update");
                    if self.socket.send(Bytes::from(serialized)).await.is_err() {
                        log::debug!("Connection dropped by subscriber");
                        break;
                    }
                },

                // Receive sync requests form the subscriber.
                Some(buffer) = self.socket.next() => match buffer {
                    Ok(_buffer) => {
                        // TODO: Implement the synchronizer.
                    },
                    Err(e) => {
                        log::debug!("Error while reading TCP stream: {}", e);
                        break;
                    }
                }
            }
        }
    }
}

/// For each incoming request, we spawn a new runner responsible to receive messages and forward them
/// through the provided deliver channel.
pub struct SubscriberServer<PublicKey: VerifyingKey> {
    /// Address to listen to.
    address: SocketAddr,
    /// Channel to notify the sequencer's core of a new subscriber.
    tx_subscriber: Sender<SubscriberMessage>,
    /// The persistent storage. It is only used to help subscribers to sync, neither this task nor its
    /// children tasks write to the store.
    store: Arc<ConsensusStore<PublicKey>>,
}

impl<PublicKey: VerifyingKey> SubscriberServer<PublicKey> {
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn spawn(
        address: SocketAddr,
        tx_subscriber: Sender<SubscriberMessage>,
        store: Arc<ConsensusStore<PublicKey>>,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                tx_subscriber,
                store,
            }
            .run()
            .await;
        });
    }

    /// Main loop responsible to accept incoming connections and spawn a new runner to handle it.
    async fn run(&self) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind TCP port");

        log::debug!("Listening on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    log::debug!("Failed to establish connection with subscriber {}", e);
                    continue;
                }
            };

            // TODO: Limit the number of subscribers here rather than in the core.
            log::debug!("Incoming connection established with {}", peer);
            let core_channel = self.tx_subscriber.clone();
            let store = self.store.clone();
            let socket = Framed::new(socket, LengthDelimitedCodec::new());
            SubscriberConnection::spawn(core_channel, store, socket);
        }
    }
}
