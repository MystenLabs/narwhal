// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use self::{configuration::NarwhalConfiguration, validator::NarwhalValidator};
use crate::{
    block_synchronizer::handler::Handler,
    grpc_server::{metrics::EndpointMetrics, proposer::NarwhalProposer},
    BlockCommand, BlockRemoverCommand,
};
use config::SharedCommittee;
use consensus::dag::Dag;

use crypto::PublicKey;
use multiaddr::Multiaddr;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tracing::{error, info};
use types::{metered_channel::Sender, ConfigurationServer, ProposerServer, ValidatorServer};

mod configuration;
pub mod metrics;
mod proposer;
mod validator;

pub struct ConsensusAPIGrpc<SynchronizerHandler: Handler + Send + Sync + 'static> {
    name: PublicKey,
    // Multiaddr of gRPC server
    socket_address: Multiaddr,
    tx_get_block_commands: Sender<BlockCommand>,
    tx_block_removal_commands: Sender<BlockRemoverCommand>,
    get_collections_timeout: Duration,
    remove_collections_timeout: Duration,
    block_synchronizer_handler: Arc<SynchronizerHandler>,
    dag: Option<Arc<Dag>>,
    committee: SharedCommittee,
    endpoints_metrics: EndpointMetrics,
}

impl<SynchronizerHandler: Handler + Send + Sync + 'static> ConsensusAPIGrpc<SynchronizerHandler> {
    #[must_use]
    pub fn spawn(
        name: PublicKey,
        socket_address: Multiaddr,
        tx_get_block_commands: Sender<BlockCommand>,
        tx_block_removal_commands: Sender<BlockRemoverCommand>,
        get_collections_timeout: Duration,
        remove_collections_timeout: Duration,
        block_synchronizer_handler: Arc<SynchronizerHandler>,
        dag: Option<Arc<Dag>>,
        committee: SharedCommittee,
        endpoints_metrics: EndpointMetrics,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let _ = Self {
                name,
                socket_address,
                tx_get_block_commands,
                tx_block_removal_commands,
                get_collections_timeout,
                remove_collections_timeout,
                block_synchronizer_handler,
                dag,
                committee,
                endpoints_metrics,
            }
            .run()
            .await
            .map_err(|e| error!("{:?}", e));
        })
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let narwhal_validator = NarwhalValidator::new(
            self.tx_get_block_commands.to_owned(),
            self.tx_block_removal_commands.to_owned(),
            self.get_collections_timeout,
            self.remove_collections_timeout,
            self.block_synchronizer_handler.clone(),
            self.dag.clone(),
        );

        let narwhal_proposer = NarwhalProposer::new(self.dag.clone(), Arc::clone(&self.committee));
        let narwhal_configuration = NarwhalConfiguration::new(
            self.committee
                .load()
                .primary(&self.name)
                .expect("Our public key or worker id is not in the committee")
                .expect("Primary addresses have not been initialized.")
                .primary_to_primary,
            Arc::clone(&self.committee),
        );

        let config = mysten_network::config::Config::default();
        let server = config
            .server_builder_with_metrics(self.endpoints_metrics.clone())
            .add_service(ValidatorServer::new(narwhal_validator))
            .add_service(ConfigurationServer::new(narwhal_configuration))
            .add_service(ProposerServer::new(narwhal_proposer))
            .bind(&self.socket_address)
            .await?;
        let local_addr = server.local_addr();
        info!("Consensus API gRPC Server listening on {local_addr}");

        server.serve().await?;

        Ok(())
    }
}
