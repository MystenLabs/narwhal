// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    zero_crypto::{
        ZeroAggregateSignature, ZeroKeyPair, ZeroPrivateKey, ZeroPublicKey,
        ZeroPublicKeyBytes, ZeroSignature,
    },
    hkdf::hkdf_generate_from_ikm,
    traits::{AggregateAuthenticator, EncodeDecodeBase64, KeyPair, ToFromBytes, VerifyingKey},
};

use rand::{rngs::StdRng, SeedableRng as _};
use sha3::Sha3_256;
use signature::{Signer, Verifier};

pub fn keys() -> Vec<ZeroKeyPair> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| ZeroKeyPair::generate(&mut rng)).collect()
}

#[test]
fn serialize_deserialize() {
    let kpref = keys().pop().unwrap();
    let public_key = kpref.public();

    let bytes = bincode::serialize(&public_key).unwrap();
    let pk2 = bincode::deserialize::<ZeroPublicKey>(&bytes).unwrap();
    assert_eq!(*public_key, pk2);

    let private_key = kpref.private();
    let bytes = bincode::serialize(&private_key).unwrap();
    let privkey = bincode::deserialize::<ZeroPublicKey>(&bytes).unwrap();
    let bytes2 = bincode::serialize(&privkey).unwrap();
    assert_eq!(bytes, bytes2);
}

#[test]
fn test_serde_signatures_non_human_readable() {
    let message = b"hello, narwhal";
    // Test populated aggregate signature
    let sig = keys().pop().unwrap().sign(message);
    let serialized = bincode::serialize(&sig).unwrap();
    let deserialized: ZeroSignature = bincode::deserialize(&serialized).unwrap();
    assert_eq!(deserialized, sig);
}

#[test]
fn test_serde_signatures_human_readable() {
    let kp = keys().pop().unwrap();
    let message: &[u8] = b"Hello, world!";
    let signature = kp.sign(message);

    let serialized = serde_json::to_string(&signature).unwrap();
    println!("{:?}", serialized);
    assert_eq!(
        "{}",
        serialized
    );
    let deserialized: ZeroSignature = serde_json::from_str(&serialized).unwrap();
    assert_eq!(deserialized, signature);
}

#[test]
fn import_export_public_key() {
    let kpref = keys().pop().unwrap();
    let public_key = kpref.public();
    let export = public_key.encode_base64();
    let import = ZeroPublicKey::decode_base64(&export);
    assert!(import.is_ok());
    assert_eq!(&import.unwrap(), public_key);
}

#[test]
fn import_export_secret_key() {
    let kpref = keys().pop().unwrap();
    let secret_key = kpref.private();
    let export = secret_key.encode_base64();
    let import = ZeroPrivateKey::decode_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap().as_ref(), secret_key.as_ref());
}
#[test]
fn to_from_bytes_signature() {
    let kpref = keys().pop().unwrap();
    let signature = kpref.sign(b"Hello, world");
    let sig_bytes = signature.as_ref();
    let rebuilt_sig = <ZeroSignature as ToFromBytes>::from_bytes(sig_bytes).unwrap();
    assert_eq!(rebuilt_sig, signature);
}

#[test]
fn verify_valid_signature() {
    // Get a keypair.
    let kp = keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();

    let signature = kp.sign(&digest.0);

    // Verify the signature.
    assert!(kp.public().verify(&digest.0, &signature).is_ok());
}

#[test]
fn verify_valid_batch() {
    // Make signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let (pubkeys, signatures): (Vec<ZeroPublicKey>, Vec<ZeroSignature>) = keys()
        .into_iter()
        .take(3)
        .map(|kp| {
            let sig = kp.sign(&digest.0);
            (kp.public().clone(), sig)
        })
        .unzip();

    // Verify the batch.
    let res = ZeroPublicKey::verify_batch(&digest.0, &pubkeys, &signatures);
    assert!(res.is_ok(), "{:?}", res);
}

#[test]
fn verify_valid_aggregate_signature() {
    // Make signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let (pubkeys, signatures): (Vec<ZeroPublicKey>, Vec<ZeroSignature>) = keys()
        .into_iter()
        .take(3)
        .map(|kp| {
            let sig = kp.sign(&digest.0);
            (kp.public().clone(), sig)
        })
        .unzip();

    let aggregated_signature = ZeroAggregateSignature::aggregate(signatures).unwrap();

    // // Verify the batch.
    let res = aggregated_signature.verify(&pubkeys[..], &digest.0);
    assert!(res.is_ok(), "{:?}", res);
}


#[test]
fn verify_batch_aggregate_signature() {
    // Make signatures.
    let message1: &[u8] = b"Hello, world!";
    let digest1 = message1.digest();
    let (pubkeys1, signatures1): (Vec<ZeroPublicKey>, Vec<ZeroSignature>) = keys()
        .into_iter()
        .take(3)
        .map(|kp| {
            let sig = kp.sign(&digest1.0);
            (kp.public().clone(), sig)
        })
        .unzip();
    let aggregated_signature1 = ZeroAggregateSignature::aggregate(signatures1).unwrap();

    // Make signatures.
    let message2: &[u8] = b"Hello, world!";
    let digest2 = message2.digest();
    let (pubkeys2, signatures2): (Vec<ZeroPublicKey>, Vec<ZeroSignature>) = keys()
        .into_iter()
        .take(2)
        .map(|kp| {
            let sig = kp.sign(&digest2.0);
            (kp.public().clone(), sig)
        })
        .unzip();

    let aggregated_signature2 = ZeroAggregateSignature::aggregate(signatures2).unwrap();

    assert!(ZeroAggregateSignature::batch_verify(
        &[aggregated_signature1, aggregated_signature2],
        &[&pubkeys1[..], &pubkeys2[..]],
        &[&digest1.0[..], &digest2.0[..]]
    )
    .is_ok());
}

#[test]
fn test_serialize_deserialize_aggregate_signatures() {
    // Test empty aggregate signature
    let sig = ZeroAggregateSignature::default();
    let serialized = bincode::serialize(&sig).unwrap();
    let _deserialized: ZeroAggregateSignature = bincode::deserialize(&serialized).unwrap();

    let message = b"hello, narwhal";
    // Test populated aggregate signature
    let (_, signatures): (Vec<ZeroPublicKey>, Vec<ZeroSignature>) = keys()
        .into_iter()
        .take(3)
        .map(|kp| {
            let sig = kp.sign(message);
            (kp.public().clone(), sig)
        })
        .unzip();

    let sig = ZeroAggregateSignature::aggregate(signatures).unwrap();
    let serialized = bincode::serialize(&sig).unwrap();
    let _deserialized: ZeroAggregateSignature = bincode::deserialize(&serialized).unwrap();
}

#[test]
fn test_add_signatures_to_aggregate() {
    let pks: Vec<ZeroPublicKey> = keys()
        .into_iter()
        .take(3)
        .map(|kp| kp.public().clone())
        .collect();
    let message = b"hello, narwhal";

    // Test 'add signature'
    let mut sig1 = ZeroAggregateSignature::default();
    // Test populated aggregate signature
    keys().into_iter().take(3).for_each(|kp| {
        let sig = kp.sign(message);
        sig1.add_signature(sig).unwrap();
    });

    assert!(sig1.verify(&pks, message).is_ok());

    // Test 'add aggregate signature'
    let mut sig2 = ZeroAggregateSignature::default();

    let kp = &keys()[0];
    let sig = ZeroAggregateSignature::aggregate(vec![kp.sign(message)]).unwrap();
    sig2.add_aggregate(sig).unwrap();

    assert!(sig2.verify(&pks[0..1], message).is_ok());

    let aggregated_signature = ZeroAggregateSignature::aggregate(
        keys()
            .into_iter()
            .take(3)
            .skip(1)
            .map(|kp| kp.sign(message))
            .collect(),
    )
    .unwrap();

    sig2.add_aggregate(aggregated_signature).unwrap();

    assert!(sig2.verify(&pks, message).is_ok());
}

#[test]
fn test_hkdf_generate_from_ikm() {
    let seed = &[
        0, 0, 1, 1, 2, 2, 4, 4, 8, 2, 0, 9, 3, 2, 4, 1, 1, 1, 2, 0, 1, 1, 3, 4, 1, 2, 9, 8, 7, 6,
        5, 4,
    ];
    let salt = &[3, 2, 1];
    let kp = hkdf_generate_from_ikm::<Sha3_256, ZeroKeyPair>(seed, salt, None).unwrap();
    let kp2 = hkdf_generate_from_ikm::<Sha3_256, ZeroKeyPair>(seed, salt, None).unwrap();
    assert_eq!(kp.private().as_bytes(), kp2.private().as_bytes());
}

#[test]
fn test_public_key_bytes_conversion() {
    let kp = keys().pop().unwrap();
    let pk_bytes: ZeroPublicKeyBytes = kp.public().into();
    let rebuilded_pk: ZeroPublicKey = pk_bytes.try_into().unwrap();
    assert_eq!(kp.public().as_bytes(), rebuilded_pk.as_bytes());
}

#[test]
fn test_copy_key_pair() {
    let kp = keys().pop().unwrap();
    let kp_copied = kp.copy();

    assert_eq!(kp.public().as_bytes(), kp_copied.public().as_bytes());
    assert_eq!(kp.private().as_bytes(), kp_copied.private().as_bytes());
}

#[tokio::test]
async fn signature_service() {
    // Get a keypair.
    let kp = keys().pop().unwrap();
    let pk = kp.public().clone();

    // Spawn the signature service.
    let mut service = SignatureService::new(kp);

    // Request signature from the service.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = service.request_signature(digest).await;

    // Verify the signature we received.
    assert!(pk.verify(digest.as_ref(), &signature).is_ok());
}
