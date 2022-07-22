// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use config::Committee;
use crypto::traits::VerifyingKey;
use futures::FutureExt;
use multiaddr::Multiaddr;
use network::{BoundedExecutor, CancelHandler, RetryConfig, MAX_TASK_CONCURRENCY};
use primary::WorkerPrimaryMessage;
use tokio::{
    runtime::Handle,
    sync::{mpsc::Receiver, watch},
    task::JoinHandle,
};
use tonic::transport::Channel;
use types::{BincodeEncodedPayload, ReconfigureNotification, WorkerToPrimaryClient};

// Send batches' digests to the primary.
pub struct PrimaryConnector<PublicKey: VerifyingKey> {
    /// The public key of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee<PublicKey>,
    /// Receive reconfiguration updates.
    rx_reconfigure: watch::Receiver<ReconfigureNotification<PublicKey>>,
    /// Input channel to receive the messages to send to the primary.
    rx_digest: Receiver<WorkerPrimaryMessage<PublicKey>>,
    /// A network sender to send the batches' digests to the primary.
    primary_client: WorkerToPrimaryNetwork,
}

impl<PublicKey: VerifyingKey> PrimaryConnector<PublicKey> {
    pub fn spawn(
        name: PublicKey,
        committee: Committee<PublicKey>,
        rx_reconfigure: watch::Receiver<ReconfigureNotification<PublicKey>>,
        rx_digest: Receiver<WorkerPrimaryMessage<PublicKey>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                rx_reconfigure,
                rx_digest,
                primary_client: WorkerToPrimaryNetwork::default(),
            }
            .run()
            .await;
        })
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                // Send the digest through the network.
                Some(digest) = self.rx_digest.recv() => {
                    let address = self.committee
                        .primary(&self.name)
                        .expect("Our public key is not in the committee")
                        .worker_to_primary;
                    let handle = self.primary_client.send(address, &digest).await;
                    handle.await;
                },

                // Trigger reconfigure.
                result = self.rx_reconfigure.changed() => {
                    result.expect("Committee channel dropped");
                    let message = self.rx_reconfigure.borrow().clone();
                    match message {
                        ReconfigureNotification::NewCommittee(new_committee) => {
                            self.committee = new_committee;
                        },
                        ReconfigureNotification::Shutdown => return
                    }
                }
            }
        }
    }
}

pub struct WorkerToPrimaryNetwork {
    client: Option<WorkerToPrimaryClient<Channel>>,
    config: mysten_network::config::Config,
    retry_config: RetryConfig,
    executor: BoundedExecutor,
}

impl Default for WorkerToPrimaryNetwork {
    fn default() -> Self {
        let retry_config = RetryConfig {
            // Retry for forever
            retrying_max_elapsed_time: None,
            ..Default::default()
        };

        Self {
            client: Default::default(),
            config: Default::default(),
            retry_config,
            executor: BoundedExecutor::new(MAX_TASK_CONCURRENCY, Handle::current()),
        }
    }
}

impl WorkerToPrimaryNetwork {
    pub async fn send<PublicKey: VerifyingKey>(
        &mut self,
        address: Multiaddr,
        message: &WorkerPrimaryMessage<PublicKey>,
    ) -> CancelHandler<()> {
        if self.client.is_none() {
            let channel = self.config.connect_lazy(&address).unwrap();
            self.client = Some(WorkerToPrimaryClient::new(channel));
        }
        let message =
            BincodeEncodedPayload::try_from(message).expect("Failed to serialize payload");
        let client = self.client.as_mut().unwrap().clone();

        let handle = self
            .executor
            .spawn(
                self.retry_config
                    .retry(move || {
                        let mut client = client.clone();
                        let message = message.clone();
                        async move { client.send_message(message).await.map_err(Into::into) }
                    })
                    .map(|response| {
                        response.expect("we retry forever so this shouldn't fail");
                    }),
            )
            .await;
        CancelHandler(handle)
    }
}
