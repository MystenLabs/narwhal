// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use bytes::Bytes;
use consensus::{ConsensusOutput, ConsensusSyncRequest, SequenceNumber};
use crypto::ed25519::Ed25519PublicKey;
use futures::{SinkExt, StreamExt};
use primary::Certificate;
use std::net::SocketAddr;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Receiver,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct MockSequencer {
    address: SocketAddr,
    rx_sequence: Receiver<Certificate<Ed25519PublicKey>>,
    consensus_index: SequenceNumber,
    sequence: Vec<ConsensusOutput<Ed25519PublicKey>>,
}

impl MockSequencer {
    pub fn spawn(address: SocketAddr, rx_sequence: Receiver<Certificate<Ed25519PublicKey>>) {
        tokio::spawn(async move {
            Self {
                address,
                rx_sequence,
                consensus_index: SequenceNumber::default(),
                sequence: Vec::new(),
            }
            .run()
            .await;
        });
    }

    async fn synchronize(
        &mut self,
        request: ConsensusSyncRequest,
        connection: &mut Framed<TcpStream, LengthDelimitedCodec>,
    ) {
        for i in request.missing {
            let message = &self.sequence[i as usize];
            let serialized = bincode::serialize(message).unwrap();
            connection.send(Bytes::from(serialized)).await.unwrap();
        }
    }

    async fn run(&mut self) {
        let socket = TcpListener::bind(&self.address).await.unwrap();

        loop {
            let (socket, _) = socket.accept().await.unwrap();
            let mut connection = Framed::new(socket, LengthDelimitedCodec::new());

            loop {
                tokio::select! {
                    // Update the subscriber every time a message is sequenced.
                    Some(certificate) = self.rx_sequence.recv() => {
                        let message = ConsensusOutput {
                            certificate,
                            consensus_index: self.consensus_index
                        };

                        self.consensus_index += 1;
                        self.sequence.push(message.clone());

                        let serialized = bincode::serialize(&message).unwrap();
                        let _ = connection.send(Bytes::from(serialized)).await;
                    },

                    // Receive sync requests form the subscriber.
                    Some(bytes) = connection.next() => {
                        let request = bincode::deserialize(&bytes.unwrap()).unwrap();
                        self.synchronize(request, &mut connection).await;
                    }
                }
            }
        }
    }
}
