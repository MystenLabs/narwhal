// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[path = "generated/narwhal.rs"]
#[rustfmt::skip]
mod narwhal;
use crypto::{ed25519::Ed25519PublicKey, traits::ToFromBytes};

use std::{array::TryFromSliceError, ops::Deref};

use crate::{
    Batch, BatchDigest, BatchMessage, BlockError, BlockErrorKind, CertificateDigest, Transaction,
};
use bytes::{Buf, Bytes};

pub use narwhal::{
    collection_retrieval_result::RetrievalResult,
    configuration_client::ConfigurationClient,
    configuration_server::{Configuration, ConfigurationServer},
    primary_to_primary_client::PrimaryToPrimaryClient,
    primary_to_primary_server::{PrimaryToPrimary, PrimaryToPrimaryServer},
    primary_to_worker_client::PrimaryToWorkerClient,
    primary_to_worker_server::{PrimaryToWorker, PrimaryToWorkerServer},
    transactions_client::TransactionsClient,
    transactions_server::{Transactions, TransactionsServer},
    validator_client::ValidatorClient,
    validator_server::{Validator, ValidatorServer},
    worker_to_primary_client::WorkerToPrimaryClient,
    worker_to_primary_server::{WorkerToPrimary, WorkerToPrimaryServer},
    worker_to_worker_client::WorkerToWorkerClient,
    worker_to_worker_server::{WorkerToWorker, WorkerToWorkerServer},
    Batch as BatchProto, BatchDigest as BatchDigestProto, BatchMessage as BatchMessageProto,
    BincodeEncodedPayload, CertificateDigest as CertificateDigestProto, CollectionError,
    CollectionErrorType, CollectionRetrievalResult, Empty, GetCollectionsRequest,
    GetCollectionsResponse, MultiAddr as MultiAddrProto, NewNetworkInfoRequest,
    PublicKey as PublicKeyProto, ReadCausalRequest, ReadCausalResponse, RemoveCollectionsRequest,
    Transaction as TransactionProto, ValidatorData,
};

impl From<Ed25519PublicKey> for PublicKeyProto {
    fn from(pub_key: Ed25519PublicKey) -> Self {
        PublicKeyProto {
            bytes: Bytes::from(pub_key.as_ref().to_vec()),
        }
    }
}

impl TryFrom<&PublicKeyProto> for Ed25519PublicKey {
    type Error = crypto::traits::Error;

    fn try_from(pub_key: &PublicKeyProto) -> Result<Self, Self::Error> {
        Ed25519PublicKey::from_bytes(pub_key.bytes.as_ref())
    }
}

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
                .into_iter()
                .map(TransactionProto::from)
                .collect::<Vec<TransactionProto>>(),
        }
    }
}

impl From<Transaction> for TransactionProto {
    fn from(transaction: Transaction) -> Self {
        TransactionProto {
            transaction: Bytes::from(transaction),
        }
    }
}

impl From<BatchProto> for Batch {
    fn from(batch: BatchProto) -> Self {
        let transactions: Vec<Vec<u8>> = batch
            .transaction
            .into_iter()
            .map(|t| t.transaction.to_vec())
            .collect();
        Batch(transactions)
    }
}

impl From<BatchDigestProto> for BatchDigest {
    fn from(batch_digest: BatchDigestProto) -> Self {
        let mut result: [u8; crypto::DIGEST_LEN] = [0; crypto::DIGEST_LEN];
        batch_digest.digest.as_ref().copy_to_slice(&mut result);
        BatchDigest::new(result)
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

impl From<BlockErrorKind> for CollectionErrorType {
    fn from(error_type: BlockErrorKind) -> Self {
        match error_type {
            BlockErrorKind::BlockNotFound => CollectionErrorType::CollectionNotFound,
            BlockErrorKind::BatchTimeout => CollectionErrorType::CollectionTimeout,
            BlockErrorKind::BatchError => CollectionErrorType::CollectionError,
        }
    }
}

impl TryFrom<CertificateDigestProto> for CertificateDigest {
    type Error = TryFromSliceError;

    fn try_from(digest: CertificateDigestProto) -> Result<Self, Self::Error> {
        Ok(CertificateDigest::new(digest.digest.deref().try_into()?))
    }
}

impl BincodeEncodedPayload {
    pub fn deserialize<T: serde::de::DeserializeOwned>(&self) -> Result<T, bincode::Error> {
        bincode::deserialize(self.payload.as_ref())
    }

    pub fn try_from<T: serde::Serialize>(value: &T) -> Result<Self, bincode::Error> {
        let payload = bincode::serialize(value)?.into();
        Ok(Self { payload })
    }
}

impl From<Bytes> for BincodeEncodedPayload {
    fn from(payload: Bytes) -> Self {
        Self { payload }
    }
}
