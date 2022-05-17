use crate::grpc_server::public_key_mapper::PublicKeyMapper;
use config::Committee;
use consensus::dag::Dag;
use crypto::traits::VerifyingKey;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use types::{Proposer, RoundsRequest, RoundsResponse};

pub struct NarwhalProposer<PublicKey: VerifyingKey, KeyMapper: PublicKeyMapper<PublicKey>> {
    /// The dag that holds the available certificates to propose
    dag: Option<Arc<Dag<PublicKey>>>,

    /// The mapper to use to convert the PublicKeyProto to the
    /// corresponding PublicKey type.
    public_key_mapper: KeyMapper,

    /// The committee
    committee: Committee<PublicKey>,
}

impl<PublicKey: VerifyingKey, KeyMapper: PublicKeyMapper<PublicKey>>
    NarwhalProposer<PublicKey, KeyMapper>
{
    pub fn new(
        dag: Option<Arc<Dag<PublicKey>>>,
        public_key_mapper: KeyMapper,
        committee: Committee<PublicKey>,
    ) -> Self {
        Self {
            dag,
            public_key_mapper,
            committee,
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
        let key =
            self.public_key_mapper
                .map(request.into_inner().public_key.ok_or_else(|| {
                    Status::invalid_argument("Invalid public key: no key provided")
                })?)
                .map_err(|_| Status::invalid_argument("Invalid public key: couldn't parse"))?;

        // ensure provided key is part of the committee
        if self.committee.primary(&key).is_err() {
            return Err(Status::invalid_argument(
                "Invalid public key: not found amongst committee",
            ));
        }

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
