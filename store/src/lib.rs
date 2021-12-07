// Copyright(C) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0
#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

use eyre::Result;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cmp::Eq,
    collections::{HashMap, VecDeque},
    hash::Hash,
};
use tokio::sync::{
    mpsc::{channel, Sender},
    oneshot,
};

pub mod traits;
pub use traits::Map;
pub mod rocks;
#[cfg(test)]
#[path = "tests/store_tests.rs"]
pub mod store_tests;

pub type StoreError = eyre::Error;
type StoreResult<T> = Result<T, StoreError>;

pub enum StoreCommand<Key, Value> {
    Write(Key, Value),
    Delete(Key),
    Read(Key, oneshot::Sender<StoreResult<Option<Value>>>),
    NotifyRead(Key, oneshot::Sender<StoreResult<Option<Value>>>),
}

#[derive(Clone)]
pub struct Store<K, V> {
    channel: Sender<StoreCommand<K, V>>,
}

impl<Key, Value> Store<Key, Value>
where
    Key: Hash + Eq + Serialize + DeserializeOwned + Send + 'static,
    Value: Serialize + DeserializeOwned + Send + Clone + 'static,
{
    pub fn new(keyed_db: rocks::DBMap<Key, Value>) -> Self {
        let mut obligations = HashMap::<_, VecDeque<oneshot::Sender<_>>>::new();
        let (tx, mut rx) = channel(100);
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    StoreCommand::Write(key, value) => {
                        let _ = keyed_db.insert(&key, &value);
                        if let Some(mut senders) = obligations.remove(&key) {
                            while let Some(s) = senders.pop_front() {
                                let _ = s.send(Ok(Some(value.clone())));
                            }
                        }
                    }
                    StoreCommand::Delete(key) => {
                        let _ = keyed_db.remove(&key);
                        if let Some(mut senders) = obligations.remove(&key) {
                            while let Some(s) = senders.pop_front() {
                                let _ = s.send(Ok(None));
                            }
                        }
                    }

                    StoreCommand::Read(key, sender) => {
                        let response = keyed_db.get(&key);
                        let _ = sender.send(response);
                    }
                    StoreCommand::NotifyRead(key, sender) => {
                        let response = keyed_db.get(&key);
                        if let Ok(Some(_)) = response {
                            let _ = sender.send(response);
                        } else {
                            obligations
                                .entry(key)
                                .or_insert_with(VecDeque::new)
                                .push_back(sender)
                        }
                    }
                }
            }
        });
        Self { channel: tx }
    }
}

impl<Key, Value> Store<Key, Value>
where
    Key: Serialize + DeserializeOwned + Send,
    Value: Serialize + DeserializeOwned + Send,
{
    pub async fn write(&self, key: Key, value: Value) {
        if let Err(e) = self.channel.send(StoreCommand::Write(key, value)).await {
            panic!("Failed to send Write command to store: {}", e);
        }
    }

    pub async fn remove(&self, key: Key) {
        if let Err(e) = self.channel.send(StoreCommand::Delete(key)).await {
            panic!("Failed to send Delete command to store: {}", e);
        }
    }

    pub async fn read(&self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self.channel.send(StoreCommand::Read(key, sender)).await {
            panic!("Failed to send Read command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to Read command from store")
    }

    pub async fn notify_read(&self, key: Key) -> StoreResult<Option<Value>> {
        let (sender, receiver) = oneshot::channel();
        if let Err(e) = self
            .channel
            .send(StoreCommand::NotifyRead(key, sender))
            .await
        {
            panic!("Failed to send NotifyRead command to store: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive reply to NotifyRead command from store")
    }
}
