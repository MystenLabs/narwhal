// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use self::validator::NarwhalValidator;
use crate::{BlockCommand, BlockRemoverCommand};
use multiaddr::Multiaddr;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tracing::error;
use types::ValidatorServer;

mod validator;

pub struct ConsensusAPIGrpc {
    socket_addr: Multiaddr,
    tx_get_block_commands: Sender<BlockCommand>,
    get_collections_timeout: Duration,
    tx_block_removal_commands: Sender<BlockRemoverCommand>,
    remove_collections_timeout: Duration,
}

impl ConsensusAPIGrpc {
    pub fn spawn(
        socket_addr: Multiaddr,
        tx_get_block_commands: Sender<BlockCommand>,
        get_collections_timeout: Duration,
        tx_block_removal_commands: Sender<BlockRemoverCommand>,
        remove_collections_timeout: Duration,
    ) {
        tokio::spawn(async move {
            let _ = Self {
                socket_addr,
                tx_get_block_commands,
                get_collections_timeout,
                tx_block_removal_commands,
                remove_collections_timeout,
            }
            .run()
            .await
            .map_err(|e| error!("{:?}", e));
        });
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let narwhal = NarwhalValidator::new(
            self.tx_get_block_commands.to_owned(),
            self.get_collections_timeout,
            self.tx_block_removal_commands.to_owned(),
            self.remove_collections_timeout,
        );

        let config = mysten_network::config::Config::default();
        config
            .server_builder()
            .add_service(ValidatorServer::new(narwhal))
            .bind(&self.socket_addr)
            .await?
            .serve()
            .await?;

        Ok(())
    }
}
