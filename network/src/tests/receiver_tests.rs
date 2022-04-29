// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::accounting_receiver::AccountingReceiver;
use crate::Writer;
use crate::{MessageHandler, Receiver};
use async_trait::async_trait;
use futures::SinkExt;
use std::error::Error;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::{
    sync::mpsc::{channel, Sender},
    time::{sleep, Duration},
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Clone)]
struct TestHandler {
    deliver: Sender<String>,
}

#[async_trait]
impl MessageHandler for TestHandler {
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize the message.
        let message = bincode::deserialize(&message)?;

        // Deliver the message to the application.
        self.deliver.send(message).await.unwrap();
        Ok(())
    }
}

#[derive(Clone)]
struct TestHandlerNoAck {
    deliver: Sender<String>,
}

#[async_trait]
impl MessageHandler for TestHandlerNoAck {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Deserialize the message.
        let message = bincode::deserialize(&message)?;

        // Deliver the message to the application.
        self.deliver.send(message).await.unwrap();
        Ok(())
    }
}

#[tokio::test]
async fn receive() {
    // Make the network receiver.
    let address = "127.0.0.1:4000".parse::<SocketAddr>().unwrap();
    let (tx, mut rx) = channel(1);
    Receiver::spawn(address, TestHandler { deliver: tx });
    sleep(Duration::from_millis(50)).await;

    // Send a message.
    let sent = "Hello, world!";
    let bytes = Bytes::from(bincode::serialize(sent).unwrap());
    let stream = TcpStream::connect(address).await.unwrap();
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    transport.send(bytes.clone()).await.unwrap();

    // Ensure the message gets passed to the channel.
    let message = rx.recv().await;
    assert!(message.is_some());
    let received = message.unwrap();
    assert_eq!(received, sent);
}

#[tokio::test]
async fn receive_no_ack() {
    // Make the network receiver.
    let address = "127.0.0.1:4001".parse::<SocketAddr>().unwrap();
    let (tx, mut rx) = channel(1);
    Receiver::spawn(address, TestHandlerNoAck { deliver: tx });
    sleep(Duration::from_millis(50)).await;

    // Send a message.
    let sent = "Hello, world!";
    let bytes = Bytes::from(bincode::serialize(sent).unwrap());
    let stream = TcpStream::connect(address).await.unwrap();
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    transport.send(bytes.clone()).await.unwrap();

    // Ensure the message gets passed to the channel.
    let message = rx.recv().await;
    assert!(message.is_some());
    let received = message.unwrap();
    assert_eq!(received, sent);
}

#[tokio::test]
async fn accounting_receive() {
    // Make the central accounting.
    let mut central_accounting = CentralAccounting::<TestHandler>::new(1);

    // Make the first accounting receiver.
    let address_1 = "127.0.0.1:4003".parse::<SocketAddr>().unwrap();
    let (tx_1, mut rx_1) = channel(1);
    let test_handler_1 = TestHandler { deliver: tx_1 };
    let accounting_receiver_1 = AccountingReceiver::new(address_1);

    // Make the second accounting receiver.
    let address_2 = "127.0.0.1:4002".parse::<SocketAddr>().unwrap();
    let (tx_2, mut rx_2) = channel(1);
    let test_handler_2 = TestHandler { deliver: tx_2 };
    let accounting_receiver_2 = AccountingReceiver::new(address_2);

    // Register the accounting receivers with the central accounting.
    let (receiver_id_1, tx_message_1) = central_accounting.register(test_handler_1).await;
    let (receiver_id_2, tx_message_2) = central_accounting.register(test_handler_2).await;

    // Start the central accounting.
    central_accounting.spawn().await;

    // Start the accounting receivers with the provided receiverIDs and tx_forwarding channels.
    accounting_receiver_1.spawn(receiver_id_1, tx_message_1);
    accounting_receiver_2.spawn(receiver_id_2, tx_message_2);
    sleep(Duration::from_millis(50)).await;

    // Send a message to receiver 1.
    let sent_1 = "Hello, #1 world!";
    let bytes_1 = Bytes::from(bincode::serialize(sent_1).unwrap());
    let stream_1 = TcpStream::connect(address_1).await.unwrap();
    let mut transport_1 = Framed::new(stream_1, LengthDelimitedCodec::new());
    transport_1.send(bytes_1.clone()).await.unwrap();

    // Ensure the message gets passed to the channel.
    let message_1 = rx_1.recv().await;
    assert!(message_1.is_some());
    let received_1 = message_1.unwrap();
    assert_eq!(received_1, sent_1);

    // Send a message to receiver 2.
    let sent_2 = "Hello, #2 world!";
    let bytes_2 = Bytes::from(bincode::serialize(sent_2).unwrap());
    let stream_2 = TcpStream::connect(address_2).await.unwrap();
    let mut transport_2 = Framed::new(stream_2, LengthDelimitedCodec::new());
    transport_2.send(bytes_2.clone()).await.unwrap();

    // Ensure the message gets passed to the channel.
    let message_2 = rx_2.recv().await;
    assert!(message_2.is_some());
    let received_2 = message_2.unwrap();
    assert_eq!(received_2, sent_2);
}
