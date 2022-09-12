// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
mod narwhal {
    #![allow(clippy::derive_partial_eq_without_eq)]
    tonic::include_proto!("narwhal");

    include!(concat!(env!("OUT_DIR"), "/narwhal.PrimaryToPrimary.rs"));
    include!(concat!(env!("OUT_DIR"), "/narwhal.WorkerToWorker.rs"));
}

use std::{array::TryFromSliceError, ops::Deref};

use crate::{BlockError, BlockErrorKind, CertificateDigest, Transaction};
use bytes::Bytes;
use crypto::PublicKey;

pub use narwhal::{
    collection_error::CollectionErrorType,
    collection_retrieval_result::RetrievalResult,
    configuration_client::ConfigurationClient,
    configuration_server::{Configuration, ConfigurationServer},
    primary_to_primary_client::PrimaryToPrimaryClient,
    primary_to_primary_server::{PrimaryToPrimary, PrimaryToPrimaryServer},
    primary_to_worker_client::PrimaryToWorkerClient,
    primary_to_worker_server::{PrimaryToWorker, PrimaryToWorkerServer},
    proposer_client::ProposerClient,
    proposer_server::{Proposer, ProposerServer},
    transactions_client::TransactionsClient,
    transactions_server::{Transactions, TransactionsServer},
    validator_client::ValidatorClient,
    validator_server::{Validator, ValidatorServer},
    worker_to_primary_client::WorkerToPrimaryClient,
    worker_to_primary_server::{WorkerToPrimary, WorkerToPrimaryServer},
    worker_to_worker_client::WorkerToWorkerClient,
    worker_to_worker_server::{WorkerToWorker, WorkerToWorkerServer},
    BincodeEncodedPayload, CertificateDigest as CertificateDigestProto, Collection,
    CollectionError, CollectionRetrievalResult, Empty, GetCollectionsRequest,
    GetCollectionsResponse, GetPrimaryAddressResponse, MultiAddr as MultiAddrProto,
    NewEpochRequest, NewNetworkInfoRequest, NodeReadCausalRequest, NodeReadCausalResponse,
    PrimaryAddresses as PrimaryAddressesProto, PublicKey as PublicKeyProto, ReadCausalRequest,
    ReadCausalResponse, RemoveCollectionsRequest, RoundsRequest, RoundsResponse,
    Transaction as TransactionProto, ValidatorData,
};

impl From<PublicKey> for PublicKeyProto {
    fn from(pub_key: PublicKey) -> Self {
        PublicKeyProto {
            bytes: Bytes::from(pub_key.as_ref().to_vec()),
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

impl From<TransactionProto> for Transaction {
    fn from(transaction: TransactionProto) -> Self {
        transaction.transaction.to_vec()
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
