// Copyright(C) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0
use crate::primary::Round;
use crypto::{CryptoError, Digest};
use store::StoreError;
use thiserror::Error;

#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($e);
    };
}

#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!($e);
        }
    };
}

pub type DagResult<T> = Result<T, DagError>;

#[derive(Debug, Error)]
pub enum DagError {
    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),

    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Invalid header id")]
    InvalidHeaderId,

    #[error("Malformed header {0}")]
    MalformedHeader(Digest),

    #[error("Received message from unknown authority {0}")]
    UnknownAuthority(String),

    #[error("Authority {0} appears in quorum more than once")]
    AuthorityReuse(String),

    #[error("Received unexpected vote fo header {0}")]
    UnexpectedVote(Digest),

    #[error("Received certificate without a quorum")]
    CertificateRequiresQuorum,

    #[error("Parents of header {0} are not a quorum")]
    HeaderRequiresQuorum(Digest),

    #[error("Message {0} (round {1}) too old")]
    TooOld(Digest, Round),

    #[error("Collection with id {0} not found")]
    CollectionNotFound(Digest),

    #[error("Batch {0} can not be retrieved")]
    BatchCanNotBeRetrieved(Digest),

    #[error("Failed to gather batches for collection")]
    FailedToGatherBatchesForCollection
}
