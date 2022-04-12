// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use blake2::digest::Update;
use config::WorkerId;
use crypto::ed25519::Ed25519PublicKey;
use primary::{Batch, BatchDigest, Certificate, Header};
use serde::Serialize;
use std::collections::BTreeMap;
use worker::{SerializedBatchMessage, WorkerMessage};

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

pub fn test_certificate(payload: BTreeMap<BatchDigest, WorkerId>) -> Certificate<Ed25519PublicKey> {
    Certificate {
        header: Header {
            payload,
            ..Header::default()
        },
        ..Certificate::default()
    }
}

/*
pub fn mock_certificate(
    origin: Ed25519PublicKey,
    round: Round,
    parents: BTreeSet<CertificateDigest>,
) -> (CertificateDigest, Certificate<Ed25519PublicKey>) {
    let digest_0 = BatchDigest([0u8; DIGEST_LEN]);
    let digest_1 = BatchDigest([1u8; DIGEST_LEN]);
    let payload = [(digest_0, 0), (digest_1, 1)].iter().cloned().collect();
    let header = Header {
        payload:,
        ..Header::default()
    };

    let certificate = Certificate {
        header: Header {
            author: origin,
            round,
            parents,
            ..Header::default()
        },
        ..Certificate::default()
    };
    (certificate.digest(), certificate)
}
*/
