use crate::grpc_server::public_key_mapper::PublicKeyMapper;
use consensus::dag::Dag;
use crypto::traits::VerifyingKey;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use types::{Proposer, RoundsRequest, RoundsResponse};

pub struct NarwhalProposer<PublicKey: VerifyingKey, KeyMapper: PublicKeyMapper<PublicKey>> {
    dag: Option<Arc<Dag<PublicKey>>>,

    public_key_mapper: KeyMapper,
}

impl<PublicKey: VerifyingKey, KeyMapper: PublicKeyMapper<PublicKey>>
    NarwhalProposer<PublicKey, KeyMapper>
{
    pub fn new(dag: Option<Arc<Dag<PublicKey>>>, public_key_mapper: KeyMapper) -> Self {
        Self {
            dag,
            public_key_mapper,
        }
    }
}

#[tonic::async_trait]
impl<PublicKey: VerifyingKey, KeyMapper: PublicKeyMapper<PublicKey>> Proposer
    for NarwhalProposer<PublicKey, KeyMapper>
{
    /// Retrieves the min & max rounds that contain collections available for
    /// block proposal for the dictated validator.
    /// by the provided public key.
    async fn rounds(
        &self,
        request: Request<RoundsRequest>,
    ) -> Result<Response<RoundsResponse>, Status> {
        // convert key
        let key = self
            .public_key_mapper
            .map(
                request
                    .into_inner()
                    .public_key
                    .ok_or_else(|| Status::invalid_argument("Not public key provided"))?,
            )
            .map_err(|_| Status::invalid_argument("Couldn't parse provided key"))?;

        // call the dag to retrieve the rounds
        if let Some(dag) = &self.dag {
            let result = match dag.rounds(key).await {
                Ok(r) => Ok(RoundsResponse {
                    oldest_round: *r.start() as u64,
                    newest_round: *r.end() as u64,
                }),
                Err(err) => Err(Status::internal(format!("Couldn't retrieve rounds: {err}"))),
            };
            return result.map(Response::new);
        }

        Err(Status::internal("Can not serve request"))
    }
}
