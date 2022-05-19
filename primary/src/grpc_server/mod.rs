// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use self::{configuration::NarwhalConfiguration, validator::NarwhalValidator};
use crate::{BlockCommand, BlockRemoverCommand};
use config::Committee;
use consensus::dag::Dag;
use crypto::traits::VerifyingKey;
use multiaddr::Multiaddr;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::Sender;
use tracing::error;
use types::{ConfigurationServer, ValidatorServer};

mod configuration;
mod validator;

pub struct ConsensusAPIGrpc<PublicKey: VerifyingKey> {
    socket_addr: Multiaddr,
    tx_get_block_commands: Sender<BlockCommand>,
    tx_block_removal_commands: Sender<BlockRemoverCommand>,
    get_collections_timeout: Duration,
    remove_collections_timeout: Duration,
    dag: Option<Arc<Dag<PublicKey>>>,
    committee: Committee<PublicKey>,
}

impl<PublicKey: VerifyingKey> ConsensusAPIGrpc<PublicKey> {
    pub fn spawn(
        socket_addr: Multiaddr,
        tx_get_block_commands: Sender<BlockCommand>,
        tx_block_removal_commands: Sender<BlockRemoverCommand>,
        get_collections_timeout: Duration,
        remove_collections_timeout: Duration,
        dag: Option<Arc<Dag<PublicKey>>>,
        committee: Committee<PublicKey>,
    ) {
        tokio::spawn(async move {
            let _ = Self {
                socket_addr,
                tx_get_block_commands,
                tx_block_removal_commands,
                get_collections_timeout,
                remove_collections_timeout,
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
            self.dag.clone(),
            self.committee.clone(),
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
