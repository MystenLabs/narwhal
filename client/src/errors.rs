// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use config::WorkerId;
use consensus::SequenceNumber;
use store::StoreError;
use thiserror::Error;

pub type SubscriberResult<T> = Result<T, SubscriberError>;

#[derive(Debug, Error)]
pub enum SubscriberError {
    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Consensus referenced unexpected worker id {0}")]
    UnexpectedWorkerId(WorkerId),

    #[error("Unexpected consensus index number {0}")]
    UnexpectedConsensusIndex(SequenceNumber),

    #[error("Deserialization of consensus message failed: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Received unexpected protocol message from consensus")]
    UnexpectedProtocolMessage,

    #[error("There can only be a single consensus client at the time")]
    OnlyOneConsensusClientPermitted,

    #[error("Execution engine failed: {0}")]
    NodeExecutionError(String),

    #[error("Client transaction invalid: {0}")]
    ClientExecutionError(String),
}

/// Trait do separate execution errors in two categories: (i) errors caused by a bad client, (ii)
/// errors caused by a fault in the authority.
pub trait AuthorityStateError {
    /// Whether the error is due to a fault in the authority (eg. internal storage error).
    fn node_error(&self) -> bool;

    /// Convert the error message in to a string.
    fn to_string(&self) -> String;
}

impl<T: AuthorityStateError> From<T> for SubscriberError {
    fn from(e: T) -> Self {
        match e.node_error() {
            true => SubscriberError::NodeExecutionError(e.to_string()),
            false => SubscriberError::ClientExecutionError(e.to_string()),
        }
    }
}
