// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use base64ct::{Base64, Encoding};
use serde::{de, Deserialize, Serialize};
use signature::{Signature, Signer, Verifier};
use std::fmt::{self, Display};

use crate::traits::{
    Authenticator, EncodeDecodeBase64, KeyPair, SigningKey, ToFromBytes, VerifyingKey,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ed25519PublicKey(pub ed25519_dalek::PublicKey);
#[derive(Debug)]
pub struct Ed25519PrivateKey(pub ed25519_dalek::SecretKey);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ed25519Signature(pub ed25519_dalek::Signature);

impl VerifyingKey for Ed25519PublicKey {
    type PrivKey = Ed25519PrivateKey;

    type Sig = Ed25519Signature;
}

impl Verifier<Ed25519Signature> for Ed25519PublicKey {
    fn verify(&self, msg: &[u8], signature: &Ed25519Signature) -> Result<(), signature::Error> {
        self.0.verify(msg, &signature.0)
    }
}

impl ToFromBytes for Ed25519PublicKey {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        ed25519_dalek::PublicKey::from_bytes(bytes).map(Ed25519PublicKey)
    }
}

impl AsRef<[u8]> for Ed25519PublicKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Default for Ed25519PublicKey {
    fn default() -> Self {
        Ed25519PublicKey::from_bytes(&[0u8; 32]).unwrap()
    }
}

impl Display for Ed25519PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", Base64::encode_string(self.0.as_bytes()))
    }
}

/// Things sorely lacking in upstream Dalek
#[allow(clippy::derive_hash_xor_eq)] // ed25519_dalek's PartialEq is compatible
impl std::hash::Hash for Ed25519PublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_bytes().hash(state);
    }
}

impl PartialOrd for Ed25519PublicKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.as_bytes().partial_cmp(other.0.as_bytes())
    }
}

impl Ord for Ed25519PublicKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl Serialize for Ed25519PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let str = self.encode_base64();
        serializer.serialize_newtype_struct("Ed25519PublicKey", &str)
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl<'de> Deserialize<'de> for Ed25519PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl SigningKey for Ed25519PrivateKey {
    type PubKey = Ed25519PublicKey;

    type Sig = Ed25519Signature;
}

impl ToFromBytes for Ed25519PrivateKey {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        ed25519_dalek::SecretKey::from_bytes(bytes).map(Ed25519PrivateKey)
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl Serialize for Ed25519PrivateKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let str = self.encode_base64();
        serializer.serialize_newtype_struct("Ed25519PublicKey", &str)
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl<'de> Deserialize<'de> for Ed25519PrivateKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl Authenticator for Ed25519Signature {
    type PubKey = Ed25519PublicKey;

    type PrivKey = Ed25519PrivateKey;
}

impl AsRef<[u8]> for Ed25519PrivateKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Signature for Ed25519Signature {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        ed25519_dalek::Signature::from_bytes(bytes).map(Ed25519Signature)
    }
}

impl AsRef<[u8]> for Ed25519Signature {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Display for Ed25519Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", Base64::encode_string(self.as_ref()))
    }
}

// see [#34](https://github.com/MystenLabs/narwhal/issues/34)
impl Default for Ed25519Signature {
    fn default() -> Self {
        let sig = ed25519_dalek::Signature::from_bytes(&[0u8; 64]).unwrap();
        Ed25519Signature(sig)
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")] // necessary so as not to deser under a != type
pub struct Ed25519KeyPair {
    pub name: Ed25519PublicKey,
    pub secret: Ed25519PrivateKey,
}

impl KeyPair for Ed25519KeyPair {
    type PubKey = Ed25519PublicKey;

    type PrivKey = Ed25519PrivateKey;

    fn public(&'_ self) -> &'_ Self::PubKey {
        &self.name
    }

    fn private(self) -> Self::PrivKey {
        self.secret
    }

    fn generate<R: rand::CryptoRng + rand::RngCore>(rng: &mut R) -> Self {
        let kp = ed25519_dalek::Keypair::generate(rng);
        Ed25519KeyPair {
            name: Ed25519PublicKey(kp.public),
            secret: Ed25519PrivateKey(kp.secret),
        }
    }
}

impl From<ed25519_dalek::Keypair> for Ed25519KeyPair {
    fn from(dalek_kp: ed25519_dalek::Keypair) -> Self {
        Ed25519KeyPair {
            name: Ed25519PublicKey(dalek_kp.public),
            secret: Ed25519PrivateKey(dalek_kp.secret),
        }
    }
}

impl Signer<Ed25519Signature> for Ed25519KeyPair {
    fn try_sign(&self, msg: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        let privkey: &ed25519_dalek::SecretKey = &self.secret.0;
        let pubkey: &ed25519_dalek::PublicKey = &self.name.0;
        let expanded_privkey: ed25519_dalek::ExpandedSecretKey = (privkey).into();
        Ok(Ed25519Signature(expanded_privkey.sign(msg, pubkey)))
    }
}
