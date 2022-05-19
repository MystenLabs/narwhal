// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use self::configuration::NarwhalConfiguration;
use self::validator::NarwhalValidator;
use crate::{block_synchronizer::handler::Handler, BlockCommand, BlockRemoverCommand};
use consensus::dag::Dag;
use crypto::traits::VerifyingKey;
use multiaddr::Multiaddr;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::Sender;
use tracing::error;
use types::{ConfigurationServer, ValidatorServer};

mod configuration;
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

        let narwhal_configuration = NarwhalConfiguration::new();

        let config = mysten_network::config::Config::default();
        config
            .server_builder()
            .add_service(ValidatorServer::new(narwhal_validator))
            .add_service(ConfigurationServer::new(narwhal_configuration))
            .bind(&self.socket_addr)
            .await?
            .serve()
            .await?;

        Ok(())
    }
}
