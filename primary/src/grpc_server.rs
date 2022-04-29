// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use crate::block_waiter::{BlockError, BlocksError, GetBlockResponse, GetBlocksResponse};
use crate::BlockCommand;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tonic::{transport::Server, Request, Response, Status};
use tracing::error;
use types::{
    BatchMessageProto, CertificateDigest, CollectionRetrievalResult, GetCollectionsRequest,
    GetCollectionsResponse, Validator, ValidatorServer,
};

#[derive(Debug)]
pub struct Narwhal {
    tx_get_block_commands: Sender<BlockCommand>,
}

impl Narwhal {
    fn new(tx_get_block_commands: Sender<BlockCommand>) -> Self {
        Self {
            tx_get_block_commands,
        }
    }
}

#[tonic::async_trait]
impl Validator for Narwhal {
    async fn get_collections(
        &self,
        request: Request<GetCollectionsRequest>,
    ) -> Result<Response<GetCollectionsResponse>, Status> {
        let collection_ids = request.into_inner().collection_id;

        let get_blocks_response;

        if collection_ids.len() == 1 {
            // Get a single block
            let tx_get_block: oneshot::Sender<Result<GetBlockResponse, BlockError>>;
            let rx_get_block: oneshot::Receiver<Result<GetBlockResponse, BlockError>>;
            (tx_get_block, rx_get_block) = oneshot::channel();

            let collection_id =
                CertificateDigest::new(collection_ids[0].f_bytes.clone().try_into().unwrap());

            self.tx_get_block_commands
                .send(BlockCommand::GetBlock {
                    id: collection_id,
                    sender: tx_get_block,
                })
                .await
                .unwrap();

            let timer = sleep(Duration::from_millis(5_000));
            tokio::pin!(timer);

            tokio::select! {
                Ok(result) = rx_get_block => {
                    get_blocks_response = match result {
                        Ok(block) => {
                            let mut retrieval_results = vec![];
                            for batch in block.batches {
                                retrieval_results.push(CollectionRetrievalResult{
                                    retrieval_result: Some(types::RetrievalResult::Batch(BatchMessageProto::from(batch)))
                                });
                            }

                            Ok(GetCollectionsResponse {
                                result: retrieval_results,
                            })
                        },
                        Err(err) => {
                            Ok(GetCollectionsResponse {
                                result: vec![CollectionRetrievalResult{
                                    retrieval_result: Some(types::RetrievalResult::Error(err.into())),
                                }]
                            })
                        }
                    };
                },
                () = &mut timer => {
                    get_blocks_response = Err(Status::internal("Timeout, no result has been received in time"));
                }
            }
        } else if collection_ids.len() >= 1 {
            // Get multiple blocks
            let tx_get_blocks: oneshot::Sender<Result<GetBlocksResponse, BlocksError>>;
            let rx_get_blocks: oneshot::Receiver<Result<GetBlocksResponse, BlocksError>>;
            (tx_get_blocks, rx_get_blocks) = oneshot::channel();

            let ids = collection_ids
                .iter()
                .map(|c_id| CertificateDigest::new(c_id.f_bytes.clone().try_into().unwrap()))
                .collect();

            self.tx_get_block_commands
                .send(BlockCommand::GetBlocks {
                    ids,
                    sender: tx_get_blocks,
                })
                .await
                .unwrap();

            let timer = sleep(Duration::from_millis(5_000));
            tokio::pin!(timer);

            tokio::select! {
                Ok(result) = rx_get_blocks => {
                    get_blocks_response = match result {
                        Ok(blocks_response) => {
                            let mut retrieval_results = vec![];
                            for block_result in blocks_response.blocks {
                                match block_result {
                                    Ok(block_response) => {
                                        for batch in block_response.batches {
                                            retrieval_results.push(CollectionRetrievalResult{
                                                retrieval_result: Some(types::RetrievalResult::Batch(BatchMessageProto::from(batch)))
                                            });
                                        }
                                    }
                                    Err(block_error) => {
                                        retrieval_results.push(CollectionRetrievalResult{
                                                retrieval_result: Some(types::RetrievalResult::Error(block_error.into())),
                                            });
                                    }
                                }
                            }
                            Ok(GetCollectionsResponse {
                                result: retrieval_results,
                            })
                        },
                        Err(err) => {
                            Err(Status::internal(format!("Expected to receive a successful get blocks result, instead got error: {:?}", err)))
                        }
                    }
                },
                () = &mut timer => {
                    get_blocks_response = Err(Status::internal("Timeout, no result has been received in time"));
                }
            }
        } else {
            get_blocks_response = Err(Status::invalid_argument(
                "Attemped fetch of no collections!",
            ));
        }

        get_blocks_response.map(|response| Response::new(response))
    }
}

pub struct GrpcServer {
    tx_get_block_commands: Sender<BlockCommand>,
}

impl GrpcServer {
    pub fn spawn(tx_get_block_commands: Sender<BlockCommand>) {
        tokio::spawn(async move {
            let _ = Self {
                tx_get_block_commands,
            }
            .run()
            .await
            .map_err(|e| error!("{:?}", e));
        });
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = "127.0.0.1:50052".parse()?;
        let narwhal = Narwhal::new(self.tx_get_block_commands.to_owned());

        Server::builder()
            .add_service(ValidatorServer::new(narwhal))
            .serve(addr)
            .await?;

        Ok(())
    }
}
