// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[path = "generated/narwhal.rs"]
#[rustfmt::skip]
mod narwhal;

use crate::{Batch, BatchMessage, BlockError, BlockErrorType, CertificateDigest};

pub use narwhal::{
    collection_retrieval_result::RetrievalResult,
    validator_client::ValidatorClient,
    validator_server::{Validator, ValidatorServer},
    Batch as BatchProto, BatchDigest as BatchDigestProto, BatchMessage as BatchMessageProto,
    CertificateDigest as CertificateDigestProto, CollectionError, CollectionErrorType,
    CollectionRetrievalResult, GetCollectionsRequest, GetCollectionsResponse,
    Transaction as TransactionProto,
};

impl From<BatchMessage> for BatchMessageProto {
    fn from(message: BatchMessage) -> Self {
        BatchMessageProto {
            id: Some(message.id.into()),
            transactions: Some(message.transactions.into()),
        }
    }
}

impl From<Batch> for BatchProto {
    fn from(batch: Batch) -> Self {
        BatchProto {
            transaction: batch
                .0
                .iter()
                .map(|transaction| TransactionProto {
                    f_bytes: transaction.to_vec(),
                })
                .collect::<Vec<TransactionProto>>(),
        }
    }
}

impl From<BlockError> for CollectionError {
    fn from(error: BlockError) -> Self {
        CollectionError {
            id: Some(error.id.into()),
            error: CollectionErrorType::from(error.error).into(),
        }
    }
}

impl From<BlockErrorType> for CollectionErrorType {
    fn from(error_type: BlockErrorType) -> Self {
        match error_type {
            BlockErrorType::BlockNotFound => CollectionErrorType::CollectionNotFound,
            BlockErrorType::BatchTimeout => CollectionErrorType::CollectionTimeout,
            BlockErrorType::BatchError => CollectionErrorType::CollectionError,
        }
    }
}

impl TryFrom<CertificateDigestProto> for CertificateDigest {
    type Error = Vec<u8>;

    fn try_from(digest: CertificateDigestProto) -> Result<Self, Self::Error> {
        Ok(CertificateDigest::new(digest.f_bytes.try_into()?))
    }
}
