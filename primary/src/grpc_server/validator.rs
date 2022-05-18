use std::sync::Arc;
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use std::time::Duration;

use crate::{block_waiter::GetBlockResponse, BlockCommand, BlockRemoverCommand, PublicKeyMapper};
use config::Committee;
use consensus::dag::Dag;
use crypto::traits::VerifyingKey;
use tokio::{
    sync::{
        mpsc::{channel, Sender},
        oneshot,
    },
    time::timeout,
};
use tonic::{Request, Response, Status};
use types::{
    BatchMessageProto, BlockError, BlockRemoverErrorKind, CertificateDigest,
    CertificateDigestProto, CollectionRetrievalResult, Empty, GetCollectionsRequest,
    GetCollectionsResponse, RemoveCollectionsRequest, RoundsRequest, RoundsResponse, Validator,
};

pub struct NarwhalValidator<PublicKey: VerifyingKey, KeyMapper: PublicKeyMapper<PublicKey>> {
    /// The channel to send the commands to the block waiter
    tx_get_block_commands: Sender<BlockCommand>,

    /// The channel to send the commands to the block remover
    tx_block_removal_commands: Sender<BlockRemoverCommand>,

    /// Timeout when processing the get_collections request
    get_collections_timeout: Duration,

    /// Timeout when processing the remove_collections request
    remove_collections_timeout: Duration,

    /// The dag that holds the available certificates to propose
    dag: Option<Arc<Dag<PublicKey>>>,

    /// The mapper to use to convert the PublicKeyProto to the
    /// corresponding PublicKey type.
    public_key_mapper: KeyMapper,

    /// The committee
    committee: Committee<PublicKey>,
}

impl<PublicKey: VerifyingKey, KeyMapper: PublicKeyMapper<PublicKey>>
    NarwhalValidator<PublicKey, KeyMapper>
{
    pub fn new(
        tx_get_block_commands: Sender<BlockCommand>,
        tx_block_removal_commands: Sender<BlockRemoverCommand>,
        get_collections_timeout: Duration,
        remove_collections_timeout: Duration,
        dag: Option<Arc<Dag<PublicKey>>>,
        public_key_mapper: KeyMapper,
        committee: Committee<PublicKey>,
    ) -> Self {
        Self {
            tx_get_block_commands,
            tx_block_removal_commands,
            get_collections_timeout,
            remove_collections_timeout,
            dag,
            public_key_mapper,
            committee,
        }
    }

    /// Extracts and verifies the public key provided from the RoundsRequest.
    /// The method will return a result where the OK() will hold the
    /// parsed public key. The Err() will hold a Status message with the
    /// specific error description.
    fn get_public_key(&self, request: RoundsRequest) -> Result<PublicKey, Status> {
        let key =
            self.public_key_mapper
                .map(request.public_key.ok_or_else(|| {
                    Status::invalid_argument("Invalid public key: no key provided")
                })?)
                .map_err(|_| Status::invalid_argument("Invalid public key: couldn't parse"))?;

        // ensure provided key is part of the committee
        if self.committee.primary(&key).is_err() {
            return Err(Status::invalid_argument(
                "Invalid public key: unknown authority",
            ));
        }

        Ok(key)
    }
}

#[tonic::async_trait]
impl<PublicKey: VerifyingKey, KeyMapper: PublicKeyMapper<PublicKey>> Validator
    for NarwhalValidator<PublicKey, KeyMapper>
{
    async fn remove_collections(
        &self,
        request: Request<RemoveCollectionsRequest>,
    ) -> Result<Response<Empty>, Status> {
        let collection_ids = request.into_inner().collection_ids;
        let remove_collections_response = if !collection_ids.is_empty() {
            let (tx_remove_block, mut rx_remove_block) = channel(1);
            let ids = parse_certificate_digests(collection_ids)?;
            self.tx_block_removal_commands
                .send(BlockRemoverCommand::RemoveBlocks {
                    ids,
                    sender: tx_remove_block,
                })
                .await
                .map_err(|err| Status::internal(format!("Send Error: {err:?}")))?;
            match timeout(self.remove_collections_timeout, rx_remove_block.recv())
                .await
                .map_err(|_err| Status::internal("Timeout, no result has been received in time"))?
            {
                Some(result) => match result {
                    Ok(_) => Ok(Empty {}),
                    Err(remove_block_error)
                        if remove_block_error.error == BlockRemoverErrorKind::Timeout =>
                    {
                        Err(Status::internal(
                            "Timeout, no result has been received in time",
                        ))
                    }
                    Err(remove_block_error) => Err(Status::internal(format!(
                        "Removal Error: {:?}",
                        remove_block_error.error
                    ))),
                },
                None => Err(Status::internal(
                    "Removal channel closed, no result has been received.",
                )),
            }
        } else {
            Err(Status::invalid_argument(
                "Attemped to remove no collections!",
            ))
        };
        remove_collections_response.map(Response::new)
    }

    async fn get_collections(
        &self,
        request: Request<GetCollectionsRequest>,
    ) -> Result<Response<GetCollectionsResponse>, Status> {
        let collection_ids = request.into_inner().collection_ids;
        let get_collections_response = if !collection_ids.is_empty() {
            let (tx_get_blocks, rx_get_blocks) = oneshot::channel();
            let ids = parse_certificate_digests(collection_ids)?;
            self.tx_get_block_commands
                .send(BlockCommand::GetBlocks {
                    ids,
                    sender: tx_get_blocks,
                })
                .await
                .map_err(|err| Status::internal(format!("Send Error: {err:?}")))?;
            match timeout(self.get_collections_timeout, rx_get_blocks)
                .await
                .map_err(|_err| Status::internal("Timeout, no result has been received in time"))?
                .map_err(|_err| Status::internal("Fetch Error, no result has been received"))?
            {
                Ok(blocks_response) => {
                    let mut retrieval_results = vec![];
                    for block_result in blocks_response.blocks {
                        retrieval_results.extend(get_collection_retrieval_results(block_result));
                    }
                    Ok(GetCollectionsResponse {
                        result: retrieval_results,
                    })
                }
                Err(err) => Err(Status::internal(format!(
                    "Expected to receive a successful get blocks result, instead got error: {err:?}",
                ))),
            }
        } else {
            Err(Status::invalid_argument(
                "Attemped fetch of no collections!",
            ))
        };
        get_collections_response.map(Response::new)
    }

    /// Retrieves the min & max rounds that contain collections available for
    /// block proposal for the dictated validator.
    /// by the provided public key.
    async fn rounds(
        &self,
        request: Request<RoundsRequest>,
    ) -> Result<Response<RoundsResponse>, Status> {
        let key = self.get_public_key(request.into_inner())?;

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

fn get_collection_retrieval_results(
    block_result: Result<GetBlockResponse, BlockError>,
) -> Vec<CollectionRetrievalResult> {
    match block_result {
        Ok(block_response) => {
            let mut collection_retrieval_results = vec![];
            for batch in block_response.batches {
                collection_retrieval_results.push(CollectionRetrievalResult {
                    retrieval_result: Some(types::RetrievalResult::Batch(BatchMessageProto::from(
                        batch,
                    ))),
                });
            }
            collection_retrieval_results
        }
        Err(block_error) => {
            vec![CollectionRetrievalResult {
                retrieval_result: Some(types::RetrievalResult::Error(block_error.into())),
            }]
        }
    }
}

fn parse_certificate_digests(
    collection_ids: Vec<CertificateDigestProto>,
) -> Result<Vec<CertificateDigest>, Status> {
    let mut ids = vec![];
    for collection_id in collection_ids {
        ids.push(
            collection_id.try_into().map_err(|err| {
                Status::invalid_argument(format!("Could not serialize: {:?}", err))
            })?,
        );
    }
    Ok(ids)
}
