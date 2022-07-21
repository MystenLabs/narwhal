// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use base64ct::Encoding as _;
use blst::min_sig as blst;
use serde::{
    de::{Deserializer, Error},
    ser::Serializer,
    Deserialize, Serialize,
};
use serde_with::{Bytes, DeserializeAs, SerializeAs};
use signature::Signature;
use std::fmt::Debug;

use crate::{ed25519::Ed25519KeyPair, bls12381::BLS12381KeyPair, secp256k1::Secp256k1KeyPair, traits::KeyPair};

fn to_custom_error<'de, D, E>(e: E) -> D::Error
where
    E: Debug,
    D: Deserializer<'de>,
{
    D::Error::custom(format!("byte deserialization failed, cause by: {:?}", e))
}

pub struct BlsSignature;

impl SerializeAs<blst::Signature> for BlsSignature {
    fn serialize_as<S>(source: &blst::Signature, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            base64ct::Base64::encode_string(source.to_bytes().as_ref()).serialize(serializer)
        } else {
            // Serialise to Bytes
            Bytes::serialize_as(&source.serialize(), serializer)
        }
    }
}

impl<'de> DeserializeAs<'de, blst::Signature> for BlsSignature {
    fn deserialize_as<D>(deserializer: D) -> Result<blst::Signature, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            base64ct::Base64::decode_vec(&s).map_err(to_custom_error::<'de, D, _>)?
        } else {
            Bytes::deserialize_as(deserializer)?
        };
        blst::Signature::deserialize(&bytes).map_err(to_custom_error::<'de, D, _>)
    }
}

pub struct Ed25519Signature;

impl SerializeAs<ed25519_dalek::Signature> for Ed25519Signature {
    fn serialize_as<S>(source: &ed25519_dalek::Signature, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            // Serialise to Base64 encoded String
            base64ct::Base64::encode_string(source.to_bytes().as_ref()).serialize(serializer)
        } else {
            // Serialise to Bytes
            Bytes::serialize_as(&source.to_bytes(), serializer)
        }
    }
}

impl<'de> DeserializeAs<'de, ed25519_dalek::Signature> for Ed25519Signature {
    fn deserialize_as<D>(deserializer: D) -> Result<ed25519_dalek::Signature, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            base64ct::Base64::decode_vec(&s).map_err(to_custom_error::<'de, D, _>)?
        } else {
            Bytes::deserialize_as(deserializer)?
        };
        <ed25519_dalek::Signature as Signature>::from_bytes(&bytes)
            .map_err(to_custom_error::<'de, D, _>)
    }
}

// 
// Encoding Helpers
//

mod borrow_private_key {
    #[cfg(test)]
    use crate::bls12377::BLS12377KeyPair;
    use crate::{ed25519::Ed25519KeyPair, bls12381::BLS12381KeyPair, secp256k1::Secp256k1KeyPair};

    pub trait SealedBorrowPrivateKey {
        fn private_key_bytes(&self) -> &[u8];
    } 

    impl SealedBorrowPrivateKey for Ed25519KeyPair {
        fn private_key_bytes(&self) -> &[u8] {
            &self.secret.as_bytes()
        }
    }
    
    impl SealedBorrowPrivateKey for BLS12381KeyPair {
        fn private_key_bytes(&self) -> &[u8] {
            &self.secret.as_bytes()
        }
    }
    
    impl SealedBorrowPrivateKey for Secp256k1KeyPair {
        fn private_key_bytes(&self) -> &[u8] {
            &self.secret.as_bytes()
        }
    }
    
    #[cfg(test)]
    impl SealedBorrowPrivateKey for BLS12377KeyPair {
        fn private_key_bytes(&self) -> &[u8] {
            &self.secret.as_bytes()
        }
    }
}


use self::borrow_private_key::SealedBorrowPrivateKey;

fn keypair_encode_base64<T: SealedBorrowPrivateKey + KeyPair>(kp: &T) {
    let mut bytes: Vec<u8> = Vec::new();
    bytes.extend_from_slice(kp.private_key_bytes().as_ref());
    bytes.extend_from_slice(&kp.name.as_ref());
    base64ct::Base64::encode_string(&bytes[..])
}

fn keypair_decode_base64<const SK_LEN: usize, const PK_LEN: usize>(kp: T) {
    let mut bytes = [0u8; SK_LEN + PK_LEN];
    base64ct::Base64::decode(value, &mut bytes).map_err(|e| eyre!("{}", e.to_string()))?;
    let secret = T::from_bytes(&bytes[..SK_LEN])?;
    Ok(BLS12381KeyPair { name, secret })
}