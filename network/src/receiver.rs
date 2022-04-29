// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::error::NetworkError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{SplitSink, StreamExt as _};
use futures::SinkExt;
use std::borrow::BorrowMut;
use std::{error::Error, net::SocketAddr};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver as mspcReceiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, info, warn};

#[cfg(test)]
#[path = "tests/receiver_tests.rs"]
pub mod receiver_tests;

/// Convenient alias for the writer end of the TCP channel.
pub type TcpWriter = SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>;

/// Convenient alias for the output of the dispatch function.
pub type Writer = Sender<Bytes>;

#[async_trait]
pub trait MessageHandler: Clone + Send + Sync + 'static {
    /// Defines how to handle an incoming message. A typical usage is to define a `MessageHandler` with a
    /// number of `Sender<T>` channels. Then implement `dispatch` to deserialize incoming messages and
    /// forward them through the appropriate delivery channel. Then `writer` can be used to send back
    /// responses or acknowledgments to the sender machine (see unit tests for examples).
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>>;
}

/// For each incoming request, we spawn a new runner responsible to receive messages and forward them
/// through the provided deliver channel.
pub struct Receiver<Handler: MessageHandler> {
    /// Address to listen to.
    address: SocketAddr,
    /// Struct responsible to define how to handle received messages.
    handler: Handler,
}

impl<Handler: MessageHandler> Receiver<Handler> {
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn spawn(address: SocketAddr, handler: Handler) {
        tokio::spawn(async move {
            Self { address, handler }.run().await;
        });
    }

    /// Main loop responsible to accept incoming connections and spawn a new runner to handle it.
    async fn run(&self) {
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
            Self::spawn_runner(socket, peer, self.handler.clone()).await;
        }
    }

    /// Spawn a new runner to handle a specific TCP connection. It receives messages and process them
    /// using the provided handler.
    async fn spawn_runner(socket: TcpStream, peer: SocketAddr, handler: Handler) {
        tokio::spawn(async move {
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (writer, mut reader) = transport.split();

            let (tx_response, tr_response): (Writer, mspcReceiver<Bytes>) = channel(100);
            Self::spawn_response_writer(tr_response, writer).await;

            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(message) => {
                        if let Err(e) = handler
                            .dispatch(tx_response.clone().borrow_mut(), message.freeze())
                            .await
                        {
                            warn!("{e}");
                            return;
                        }
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
    async fn spawn_response_writer(
        mut tr_response: mspcReceiver<Bytes>,
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
