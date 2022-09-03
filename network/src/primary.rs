// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    bounded_executor::RetryError,
    metrics::{Metrics, PrimaryNetworkMetrics},
    traits::{BaseNetwork, LuckyNetwork, ReliableNetwork, UnreliableNetwork},
    BoundedExecutor, CancelOnDropHandler, MessageResult, MAX_TASK_CONCURRENCY,
};
use async_trait::async_trait;
use exponential_backoff::Backoff;
use multiaddr::Multiaddr;
use rand::{rngs::SmallRng, SeedableRng as _};
use std::collections::HashMap;
use std::time::Duration;
use tokio::{runtime::Handle, task::JoinHandle};
use tonic::{transport::Channel, Code};
use tracing::error;
use types::{
    BincodeEncodedPayload, PrimaryMessage, PrimaryToPrimaryClient, PrimaryToWorkerClient,
    PrimaryWorkerMessage,
};

pub struct PrimaryNetwork {
    clients: HashMap<Multiaddr, PrimaryToPrimaryClient<Channel>>,
    config: mysten_network::config::Config,
    backoff: Backoff,
    /// Small RNG just used to shuffle nodes and randomize connections (not crypto related).
    rng: SmallRng,
    // One bounded executor per address
    executors: HashMap<Multiaddr, BoundedExecutor>,
    metrics: Option<Metrics<PrimaryNetworkMetrics>>,
}

fn default_executor() -> BoundedExecutor {
    BoundedExecutor::new(MAX_TASK_CONCURRENCY, Handle::current())
}

impl Default for PrimaryNetwork {
    fn default() -> Self {
        let backoff = Backoff::new(
            u32::MAX,
            Duration::from_millis(500),
            Duration::from_secs(15),
        );

        Self {
            clients: Default::default(),
            config: Default::default(),
            backoff,
            rng: SmallRng::from_entropy(),
            executors: HashMap::new(),
            metrics: None,
        }
    }
}

impl PrimaryNetwork {
    pub fn new(metrics: Metrics<PrimaryNetworkMetrics>) -> Self {
        Self {
            metrics: Some(Metrics::from(metrics, "primary".to_string())),
            ..Default::default()
        }
    }

    fn update_metrics(&self) {
        if let Some(m) = &self.metrics {
            for (addr, executor) in &self.executors {
                m.set_network_available_tasks(
                    executor.available_capacity() as i64,
                    Some(addr.to_string()),
                );
            }
        }
    }

    pub fn cleanup<'a, I>(&mut self, to_remove: I)
    where
        I: IntoIterator<Item = &'a Multiaddr>,
    {
        for address in to_remove {
            self.clients.remove(address);
        }
    }
}

impl BaseNetwork for PrimaryNetwork {
    type Client = PrimaryToPrimaryClient<Channel>;

    type Message = PrimaryMessage;

    fn client(&mut self, address: Multiaddr) -> Self::Client {
        self.clients
            .entry(address.clone())
            .or_insert_with(|| Self::create_client(&self.config, address))
            .clone()
    }

    fn create_client(config: &mysten_network::config::Config, address: Multiaddr) -> Self::Client {
        //TODO don't panic here if address isn't supported
        let channel = config.connect_lazy(&address).unwrap();
        PrimaryToPrimaryClient::new(channel)
    }
}

#[async_trait]
impl UnreliableNetwork for PrimaryNetwork {
    async fn unreliable_send_message(
        &mut self,
        address: Multiaddr,
        message: BincodeEncodedPayload,
    ) -> JoinHandle<()> {
        let mut client = self.client(address.clone());
        let handle = self
            .executors
            .entry(address)
            .or_insert_with(default_executor)
            .spawn(async move {
                let _ = client.send_message(message).await;
            })
            .await;

        self.update_metrics();

        handle
    }
}

#[async_trait]
impl LuckyNetwork for PrimaryNetwork {
    fn rng(&mut self) -> &mut SmallRng {
        &mut self.rng
    }
}

#[async_trait]
impl ReliableNetwork for PrimaryNetwork {
    // Safety
    // Since this spawns an unbounded task, this should be called in a time-restricted fashion.
    // Here the callers are [`PrimaryNetwork::broadcast`] and [`PrimaryNetwork::send`],
    // at respectively N and K calls per round.
    //  (where N is the number of primaries, K the number of workers for this primary)
    // See the TODO on spawn_with_retries for lifting this restriction.
    async fn send_message(
        &mut self,
        address: Multiaddr,
        message: BincodeEncodedPayload,
    ) -> CancelOnDropHandler<MessageResult> {
        let client = self.client(address.clone());

        let message_send = move || {
            let mut client = client.clone();
            let message = message.clone();

            async move {
                match client.send_message(message).await {
                    Ok(_) => Ok(()),
                    Err(e) => match e.code() {
                        Code::FailedPrecondition | Code::InvalidArgument => {
                            // these errors are not recoverable through retrying, see
                            // https://github.com/hyperium/tonic/blob/master/tonic/src/status.rs
                            error!("Irrecoverable network error: {e}");
                            Err(RetryError::Permanent)
                        }
                        _ => {
                            // this returns a backoff::Error::Transient
                            // so that if tonic::Status is returned, we retry
                            Err(RetryError::Transient)
                        }
                    },
                }
            }
        };

        let backoff = self.backoff.clone();

        let adapter = |result| match result {
            Ok(()) => Ok(tonic::Response::new(types::Empty {})),
            Err(e) => Err(eyre::Report::from(e)),
        };

        let handle = self
            .executors
            .entry(address)
            .or_insert_with(default_executor)
            .spawn_with_retries_and_adapter(backoff, message_send, adapter)
            .await;

        self.update_metrics();

        CancelOnDropHandler(handle)
    }
}
pub struct PrimaryToWorkerNetwork {
    clients: HashMap<Multiaddr, PrimaryToWorkerClient<Channel>>,
    config: mysten_network::config::Config,
    executor: BoundedExecutor,
    metrics: Option<Metrics<PrimaryNetworkMetrics>>,
}

impl PrimaryToWorkerNetwork {
    pub fn new(metrics: Metrics<PrimaryNetworkMetrics>) -> Self {
        Self {
            metrics: Some(Metrics::from(metrics, "primary_to_worker".to_string())),
            ..Default::default()
        }
    }

    fn update_metrics(&self) {
        if let Some(m) = &self.metrics {
            m.set_network_available_tasks(self.executor.available_capacity() as i64, None);
        }
    }

    pub fn cleanup<'a, I>(&mut self, to_remove: I)
    where
        I: IntoIterator<Item = &'a Multiaddr>,
    {
        // TODO: Add protection for primary owned worker addresses (issue#840).
        for address in to_remove {
            self.clients.remove(address);
        }
    }
}

impl Default for PrimaryToWorkerNetwork {
    fn default() -> Self {
        Self {
            clients: Default::default(),
            config: Default::default(),
            executor: BoundedExecutor::new(MAX_TASK_CONCURRENCY, Handle::current()),
            metrics: None,
        }
    }
}

impl BaseNetwork for PrimaryToWorkerNetwork {
    type Client = PrimaryToWorkerClient<Channel>;
    type Message = PrimaryWorkerMessage;

    fn client(&mut self, address: Multiaddr) -> PrimaryToWorkerClient<Channel> {
        self.clients
            .entry(address.clone())
            .or_insert_with(|| Self::create_client(&self.config, address))
            .clone()
    }

    fn create_client(
        config: &mysten_network::config::Config,
        address: Multiaddr,
    ) -> PrimaryToWorkerClient<Channel> {
        //TODO don't panic here if address isn't supported
        let channel = config.connect_lazy(&address).unwrap();
        PrimaryToWorkerClient::new(channel)
    }
}

#[async_trait]
impl UnreliableNetwork for PrimaryToWorkerNetwork {
    async fn unreliable_send_message(
        &mut self,
        address: Multiaddr,
        message: BincodeEncodedPayload,
    ) -> JoinHandle<()> {
        let mut client = self.client(address.clone());
        let handler = self
            .executor
            .spawn(async move {
                let _ = client.send_message(message).await;
            })
            .await;

        self.update_metrics();

        handler
    }
}
