// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{MessageHandler, Writer};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver as mspcReceiver, Sender};
use tracing::warn;

#[cfg(test)]
#[path = "tests/receiver_tests.rs"]
pub mod receiver_tests;

static COUNTER: AtomicUsize = AtomicUsize::new(1);

fn get_id() -> ReceiverId {
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Unique identifier of an Accounting Receiver.
pub type ReceiverId = usize;

/// CentralAccounting is the centralized layer to put all the logic of what checks to do
/// on an incoming external request. Based on the output of the checks, it can either dispatch a
/// message or not.
pub struct CentralAccounting<Handler: MessageHandler> {
    /// Sender that gets cloned and provided to each registered accounting receiver.
    tx_message: Sender<AccountingMessage>,

    /// Receiver of all the messages forwarded from the accounting receivers.
    tr_message: mspcReceiver<AccountingMessage>,

    /// The handler associated with each registered receiver, by ReceiverID.
    receiver_info: HashMap<ReceiverId, Handler>,
}

/// The message with the following metadata: the receiver that sent it, and the Sender for the
/// TCP Connection on which to send the response.
pub struct AccountingMessage {
    /// The Receiver Id of the accounting receiver that sent this message.
    pub(crate) receiver_id: ReceiverId,

    /// The message Bytes read from TCP connection.
    pub(crate) message: Bytes,

    /// The channel to forward the response.
    pub(crate) tx_response: Writer,
}

impl<Handler: MessageHandler> CentralAccounting<Handler> {
    /// Create a new CentralAccounting.
    pub fn new(channel_capacity: usize) -> Self {
        let (tx_message, tr_message) = channel(channel_capacity);
        CentralAccounting {
            tx_message,
            tr_message,
            receiver_info: HashMap::new(),
        }
    }

    /// Register will register an AccountingReceiver with the CentralAccounting. This function is
    /// to be called once for each AccountingReceiver, and it will provide the parameters
    /// necessary to start an AccountingReceiver.
    pub async fn register(&mut self, handler: Handler) -> (ReceiverId, Sender<AccountingMessage>) {
        let id = get_id();
        self.receiver_info.insert(id, handler);
        (id, self.tx_message.clone())
    }

    /// Spawn runs the CentralAccounting. This should be called after registration of every
    /// accounting_receiver.
    pub async fn spawn(mut self) {
        tokio::spawn(async move {
            self.run().await;
        });
    }

    /// This contains the main loop for receiving message to inspect and forwards them to the
    /// respective components using the message handler associated with each receiver.
    async fn run(&mut self) {
        loop {
            select! {
                Some(mut message) = self.tr_message.recv() => {
                    // use receiver_id to look up the handler for this receiver
                    let handler = self.receiver_info.get(&message.receiver_id);
                    let handler = match handler {
                        Some(h) => h,
                        None => {
                            continue;
                        }
                    };

                    // call dispatch
                    if let Err(e) = handler
                        .dispatch(&mut message.tx_response, message.message)
                        .await
                    {
                        warn!("{e}");
                    }
                }
            }
        }
    }
}
