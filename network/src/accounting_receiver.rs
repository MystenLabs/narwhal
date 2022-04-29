// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::central_accounting::{AccountingMessage, ReceiverId};
use crate::error::NetworkError;
use crate::receiver::TcpWriter;
use crate::Writer;
use bytes::Bytes;
use futures::stream::StreamExt as _;
use futures::SinkExt;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, info, warn};

/// For each incoming request, we spawn a new runner responsible to receive messages and forward them
/// to the CentralAccounting layer through the provided channel.
pub struct AccountingReceiver {
    /// Address to listen to.
    address: SocketAddr,
}

impl AccountingReceiver {
    /// Create a new AccountingReceiver.
    pub fn new(address: SocketAddr) -> Self {
        AccountingReceiver { address }
    }

    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn spawn(self, id: ReceiverId, tx_message: Sender<AccountingMessage>) {
        tokio::spawn(async move {
            self.run(id, tx_message).await;
        });
    }

    /// Main loop responsible to accept incoming connections and spawn a new runner to handle it.
    async fn run(self, id: ReceiverId, tx_forwarding: Sender<AccountingMessage>) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind TCP port");

        debug!("Listening on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    let err = NetworkError::FailedToListen(e);
                    warn!("{err}");
                    continue;
                }
            };
            info!("Incoming connection established with {peer}");

            Self::spawn_runner(socket, peer, id, tx_forwarding.clone()).await;
        }
    }

    /// Spawn a new runner to handle a specific TCP connection. It receives messages and forwards them
    /// using to the CentralAccounting layer.
    async fn spawn_runner(
        socket: TcpStream,
        peer: SocketAddr,
        receiver_id: ReceiverId,
        tx_forwarding: Sender<AccountingMessage>,
    ) {
        tokio::spawn(async move {
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (tcp_writer, mut reader) = transport.split();
            let (tx_response, tr_response): (Writer, Receiver<Bytes>) = channel(100);
            Self::spawn_response_writer(tr_response, tcp_writer).await;

            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(message) => {
                        let accounting_message = AccountingMessage {
                            receiver_id,
                            message: message.freeze(),
                            tx_response: tx_response.clone(),
                        };
                        _ = tx_forwarding.send(accounting_message).await;
                    }
                    Err(e) => {
                        warn!("{e}");
                        return;
                    }
                }
            }
            warn!("Connection closed by peer {peer}");
        });
    }

    /// Spawns a new task that will listen for any responses from the accounting layer and
    /// forward them to the tcp writer.
    pub async fn spawn_response_writer(
        mut tr_response: Receiver<Bytes>,
        mut tcp_writer: TcpWriter,
    ) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                   Some(response) = tr_response.recv() => {
                        if let Err(e) = tcp_writer.send(response).await {
                            warn!("{e}");
                        }
                    }
                }
            }
        });
    }
}
