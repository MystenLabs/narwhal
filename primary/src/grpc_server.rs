use std::time::Duration;

use crate::block_waiter::{BlockError, GetBlockResponse};
use crate::BlockCommand;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tonic::{transport::Server, Request, Response, Status};
use tracing::error;
use types::{
    CertificateDigest, GetCollectionsRequest, GetCollectionsResponse, Validator, ValidatorServer,
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
        println!("Got a request: {:?}", request);

        let collection_ids = request.into_inner().collection_id;

        let message;

        if collection_ids.len() == 1 {
            // Get a single block
            let tx_get_block: oneshot::Sender<Result<GetBlockResponse, BlockError>>;
            let rx_get_block: oneshot::Receiver<Result<GetBlockResponse, BlockError>>;
            (tx_get_block, rx_get_block) = oneshot::channel();

            // Convert proto CertificateDigest to rust CertificateDigest
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
                    message = match result {
                        Ok(block) => format!("Retreived Block {:?}", block),
                        Err(err) => format!("Expected to receive a successful get blocks result, instead got error: {:?}", err)
                    };
                },
                () = &mut timer => {
                    message = format!("Timeout, no result has been received in time");
                }
            }
        } else if collection_ids.len() >= 1 {
            // Get multiple blocks
            message = format!(
                "Attemped multi fetch of {:?}, but service is not implemented!",
                collection_ids
            );
        } else {
            // no collection ids requested.
            message = format!("Attemped fetch of no collections!");
        }

        let reply = GetCollectionsResponse { message };

        Ok(Response::new(reply))
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
