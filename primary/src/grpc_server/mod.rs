// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use self::{configuration::NarwhalConfiguration, validator::NarwhalValidator};
use crate::{
    block_synchronizer::handler::Handler, grpc_server::proposer::NarwhalProposer, BlockCommand,
    BlockRemoverCommand,
};
use config::SharedCommittee;
use consensus::dag::Dag;
use crypto::traits::VerifyingKey;
use multiaddr::Multiaddr;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::Sender;
use tracing::error;
use types::{ConfigurationServer, ProposerServer, ValidatorServer};

mod configuration;
mod proposer;
mod validator;

pub struct ConsensusAPIGrpc<
    PublicKey: VerifyingKey,
    SynchronizerHandler: Handler<PublicKey> + Send + Sync + 'static,
> {
    socket_addr: Multiaddr,
    tx_get_block_commands: Sender<BlockCommand>,
    tx_block_removal_commands: Sender<BlockRemoverCommand>,
    get_collections_timeout: Duration,
    remove_collections_timeout: Duration,
    block_synchronizer_handler: Arc<SynchronizerHandler>,
    dag: Option<Arc<Dag<PublicKey>>>,
    committee: SharedCommittee<PublicKey>,
}

impl<PublicKey: VerifyingKey, SynchronizerHandler: Handler<PublicKey> + Send + Sync + 'static>
    ConsensusAPIGrpc<PublicKey, SynchronizerHandler>
{
    pub fn spawn(
        socket_addr: Multiaddr,
        tx_get_block_commands: Sender<BlockCommand>,
        tx_block_removal_commands: Sender<BlockRemoverCommand>,
        get_collections_timeout: Duration,
        remove_collections_timeout: Duration,
        block_synchronizer_handler: Arc<SynchronizerHandler>,
        dag: Option<Arc<Dag<PublicKey>>>,
        committee: SharedCommittee<PublicKey>,
    ) {
        tokio::spawn(async move {
            let _ = Self {
                socket_addr,
                tx_get_block_commands,
                tx_block_removal_commands,
                get_collections_timeout,
                remove_collections_timeout,
                block_synchronizer_handler,
                dag,
                committee,
            }
            .run()
            .await
            .map_err(|e| error!("{:?}", e));
        });
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

        let narwhal_configuration = NarwhalConfiguration::new();

        let config = mysten_network::config::Config::default();
        config
            .server_builder()
            .add_service(ValidatorServer::new(narwhal_validator))
            .add_service(ConfigurationServer::new(narwhal_configuration))
            .add_service(ProposerServer::new(narwhal_proposer))
            .bind(&self.socket_addr)
            .await?
            .serve()
            .await?;

        Ok(())
    }
}
