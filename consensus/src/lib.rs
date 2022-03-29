// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

pub mod dag;
mod store;
pub mod subscriber;
pub mod tusk;

pub use crate::{subscriber::SubscriberHandler, tusk::Consensus};

use crypto::traits::VerifyingKey;
use primary::Certificate;
use serde::{Deserialize, Serialize};

/// The default channel size used in the consensus and subscriber logic.
pub const DEFAULT_CHANNEL_SIZE: usize = 1_000;

/// A global sequence number assigned to every certificate.
pub type SequenceNumber = u64;

/// The output format of the consensus.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(bound(deserialize = "PublicKey: VerifyingKey"))]
pub struct ConsensusOutput<PublicKey: VerifyingKey> {
    /// The sequenced certificate.
    pub certificate: Certificate<PublicKey>,
    /// The (global) index associated with this certificate.
    pub consensus_index: SequenceNumber,
}

/// The message sent by the client to sync missing chunks of the output sequence.
#[derive(Serialize, Deserialize, Debug)]
pub struct ConsensusSyncRequest {
    /// The latest index known by the client.
    pub start: SequenceNumber,
    /// The latest index the client would like to sync.
    pub stop: SequenceNumber,
}
