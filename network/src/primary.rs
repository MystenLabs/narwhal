// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    traits::{Lucky, ReliableNetwork2, UnreliableNetwork2},
    BoundedExecutor, CancelOnDropHandler, RetryConfig, MAX_TASK_CONCURRENCY,
};
use anemo::PeerId;
use async_trait::async_trait;
use crypto::NetworkPublicKey;
use multiaddr::Multiaddr;
use rand::{rngs::SmallRng, SeedableRng as _};
use std::collections::HashMap;
use tokio::{runtime::Handle, task::JoinHandle};
use types::{
    PrimaryMessage, PrimaryToPrimaryClient, PrimaryToWorkerClient, PrimaryWorkerMessage,
    WorkerMessage, WorkerToWorkerClient,
};

fn default_executor() -> BoundedExecutor {
    BoundedExecutor::new(MAX_TASK_CONCURRENCY, Handle::current())
}

pub struct P2pNetwork {
    network: anemo::Network,
    retry_config: RetryConfig,
    /// Small RNG just used to shuffle nodes and randomize connections (not crypto related).
    rng: SmallRng,
    // One bounded executor per address
    executors: HashMap<PeerId, BoundedExecutor>,
}

impl P2pNetwork {
    pub fn new(network: anemo::Network) -> Self {
        let retry_config = RetryConfig {
            // Retry forever
            retrying_max_elapsed_time: None,
            ..Default::default()
        };

        Self {
            network,
            retry_config,
            rng: SmallRng::from_entropy(),
            executors: HashMap::new(),
        }
    }

    pub fn cleanup<'a, I>(&mut self, _to_remove: I)
    where
        I: IntoIterator<Item = &'a Multiaddr>,
    {
        // TODO This function was previously used to remove old clients on epoch changes. This may
        // not be necessary with the new networking stack so we'll need to revisit if this function
        // is even needed. For now do nothing.
    }

    async fn unreliable_send<F, Fut, O>(&mut self, peer: NetworkPublicKey, f: F) -> JoinHandle<()>
    where
        F: FnOnce(anemo::Peer) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = O> + Send,
    {
        let network = self.network.clone();
        let peer_id = PeerId(peer.0.to_bytes());
        self.executors
            .entry(peer_id)
            .or_insert_with(default_executor)
            .spawn(async move {
                if let Some(peer) = network.peer(peer_id) {
                    let _ = f(peer).await;
                }
            })
            .await
    }

    async fn send<F, Fut>(
        &mut self,
        peer: NetworkPublicKey,
        f: F,
    ) -> CancelOnDropHandler<anyhow::Result<anemo::Response<()>>>
    where
        F: Fn(anemo::Peer) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = Result<anemo::Response<()>, anemo::rpc::Status>> + Send,
    {
        // Safety
        // Since this spawns an unbounded task, this should be called in a time-restricted fashion.
        // Here the callers are [`PrimaryNetwork::broadcast`] and [`PrimaryNetwork::send`],
        // at respectively N and K calls per round.
        //  (where N is the number of primaries, K the number of workers for this primary)
        // See the TODO on spawn_with_retries for lifting this restriction.

        let network = self.network.clone();
        let peer_id = PeerId(peer.0.to_bytes());
        let message_send = move || {
            let network = network.clone();
            let f = f.clone();

            async move {
                if let Some(peer) = network.peer(peer_id) {
                    f(peer).await.map_err(|e| {
                        // this returns a backoff::Error::Transient
                        // so that if anemo::Status is returned, we retry
                        backoff::Error::transient(anyhow::anyhow!("RPC error: {e:?}"))
                    })
                } else {
                    Err(backoff::Error::transient(anyhow::anyhow!(
                        "not connected to peer {peer_id}"
                    )))
                }
            }
        };

        let handle = self
            .executors
            .entry(peer_id)
            .or_insert_with(default_executor)
            .spawn_with_retries(self.retry_config, message_send);

        CancelOnDropHandler(handle)
    }
}

impl Lucky for P2pNetwork {
    fn rng(&mut self) -> &mut SmallRng {
        &mut self.rng
    }
}

//
// Primary-to-Primary
//

#[async_trait]
impl UnreliableNetwork2<PrimaryMessage> for P2pNetwork {
    async fn unreliable_send(
        &mut self,
        peer: NetworkPublicKey,
        message: &PrimaryMessage,
    ) -> JoinHandle<()> {
        let message = message.to_owned();
        let f = move |peer| async move {
            PrimaryToPrimaryClient::new(peer)
                .send_message(message)
                .await
        };
        self.unreliable_send(peer, f).await
    }
}

#[async_trait]
impl ReliableNetwork2<PrimaryMessage> for P2pNetwork {
    async fn send(
        &mut self,
        peer: NetworkPublicKey,
        message: &PrimaryMessage,
    ) -> CancelOnDropHandler<anyhow::Result<anemo::Response<()>>> {
        let message = message.to_owned();
        let f = move |peer| {
            let message = message.clone();
            async move {
                PrimaryToPrimaryClient::new(peer)
                    .send_message(message)
                    .await
            }
        };

        self.send(peer, f).await
    }
}

//
// Primary-to-Worker
//

#[async_trait]
impl UnreliableNetwork2<PrimaryWorkerMessage> for P2pNetwork {
    async fn unreliable_send(
        &mut self,
        peer: NetworkPublicKey,
        message: &PrimaryWorkerMessage,
    ) -> JoinHandle<()> {
        let message = message.to_owned();
        let f =
            move |peer| async move { PrimaryToWorkerClient::new(peer).send_message(message).await };
        self.unreliable_send(peer, f).await
    }
}

#[async_trait]
impl ReliableNetwork2<PrimaryWorkerMessage> for P2pNetwork {
    async fn send(
        &mut self,
        peer: NetworkPublicKey,
        message: &PrimaryWorkerMessage,
    ) -> CancelOnDropHandler<anyhow::Result<anemo::Response<()>>> {
        let message = message.to_owned();
        let f = move |peer| {
            let message = message.clone();
            async move { PrimaryToWorkerClient::new(peer).send_message(message).await }
        };

        self.send(peer, f).await
    }
}

//
// Worker-to-Worker
//

#[async_trait]
impl UnreliableNetwork2<WorkerMessage> for P2pNetwork {
    async fn unreliable_send(
        &mut self,
        peer: NetworkPublicKey,
        message: &WorkerMessage,
    ) -> JoinHandle<()> {
        let message = message.to_owned();
        let f =
            move |peer| async move { WorkerToWorkerClient::new(peer).send_message(message).await };
        self.unreliable_send(peer, f).await
    }
}

#[async_trait]
impl ReliableNetwork2<WorkerMessage> for P2pNetwork {
    async fn send(
        &mut self,
        peer: NetworkPublicKey,
        message: &WorkerMessage,
    ) -> CancelOnDropHandler<anyhow::Result<anemo::Response<()>>> {
        let message = message.to_owned();
        let f = move |peer| {
            let message = message.clone();
            async move { WorkerToWorkerClient::new(peer).send_message(message).await }
        };

        self.send(peer, f).await
    }
}
