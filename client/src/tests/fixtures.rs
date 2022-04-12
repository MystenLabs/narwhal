// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use blake2::digest::Update;
use config::WorkerId;
use crypto::ed25519::Ed25519PublicKey;
use primary::{Batch, BatchDigest, Certificate, Header};
use serde::Serialize;
use std::collections::BTreeMap;
use worker::{SerializedBatchMessage, WorkerMessage};

/// A test batch containing specific transactions.
pub fn test_batch<T: Serialize>(transactions: Vec<T>) -> (BatchDigest, SerializedBatchMessage) {
    let batch = transactions
        .iter()
        .map(|x| bincode::serialize(x).unwrap())
        .collect();
    let message = WorkerMessage::<Ed25519PublicKey>::Batch(Batch(batch));
    let serialized = bincode::serialize(&message).unwrap();
    let digest = BatchDigest::new(crypto::blake2b_256(|hasher| hasher.update(&serialized)));
    (digest, serialized)
}

/// A test certificate with a specific payload.
pub fn test_certificate(payload: BTreeMap<BatchDigest, WorkerId>) -> Certificate<Ed25519PublicKey> {
    Certificate {
        header: Header {
            payload,
            ..Header::default()
        },
        ..Certificate::default()
    }
}