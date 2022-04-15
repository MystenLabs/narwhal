use tonic::{transport::Server, Request, Response, Status};

use mempool::validator_server::{Validator, ValidatorServer};
use mempool::{GetCollectionsRequest, GetCollectionsResponse};
use tracing::error;

pub mod mempool {
    tonic::include_proto!("mempool"); // The string specified here must match the proto package name
}

#[derive(Debug, Default)]
pub struct Mempool {}

#[tonic::async_trait]
impl Validator for Mempool {
    async fn get_collections(
        &self,
        request: Request<GetCollectionsRequest>,
    ) -> Result<Response<GetCollectionsResponse>, Status> {
        println!("Got a request: {:?}", request);

        let reply = mempool::GetCollectionsResponse {
            message: format!(
                "Attemped fetch of {:?}, but service is not implemented!",
                request.into_inner().collection_id
            )
            .into(),
        };

        Ok(Response::new(reply))
    }
}

pub struct GrpcServer {}

impl GrpcServer {
    pub fn spawn() {
        tokio::spawn(async move {
            let _ = Self {}.run().await.map_err(|e| error!("{:?}", e));
        });
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = "127.0.0.1:50052".parse()?;
        let mempool = Mempool::default();

        Server::builder()
            .add_service(ValidatorServer::new(mempool))
            .serve(addr)
            .await?;

        Ok(())
    }
}
