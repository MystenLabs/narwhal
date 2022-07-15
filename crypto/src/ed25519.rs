// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use base64ct::{Base64, Encoding};
use ed25519_dalek::SECRET_KEY_LENGTH;

use hkdf::Hkdf;
use serde::{de, Deserialize, Serialize};
use serde_with::serde_as;
use sha3::Sha3_256;
use signature::{Signature, Signer, Verifier};
use std::fmt::{self, Display};
use std::str::FromStr;

use crate::traits::{
    AggregateAuthenticator, Authenticator, EncodeDecodeBase64, KeyPair, SigningKey, ToFromBytes,
    VerifyingKey, VerifyingKeyBytes,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ed25519PublicKey(pub ed25519_dalek::PublicKey);

#[derive(Debug)]
pub struct Ed25519PrivateKey(pub ed25519_dalek::SecretKey);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde_as]
pub struct Ed25519Signature(#[serde_as(as = "Ed25519Signature")] pub ed25519_dalek::Signature);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde_as]
pub struct Ed25519AggregateSignature(
    #[serde_as(as = "Vec<Ed25519Signature>")] pub Vec<ed25519_dalek::Signature>,
);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Copy, Hash)]
pub struct Ed25519PublicKeyBytes([u8; ed25519_dalek::PUBLIC_KEY_LENGTH]);

impl VerifyingKey for Ed25519PublicKey {
    type PrivKey = Ed25519PrivateKey;
    type Sig = Ed25519Signature;
    type Bytes = Ed25519PublicKeyBytes;
    const LENGTH: usize = ed25519_dalek::PUBLIC_KEY_LENGTH;

    fn verify_batch(msg: &[u8], pks: &[Self], sigs: &[Self::Sig]) -> Result<(), signature::Error> {
        let msgs = vec![msg; pks.len()];
        // TODO: replace this with some unsafe - but faster & non-alloc - implementation
        let sigs: Vec<_> = sigs.iter().map(|s| s.0).collect();
        let pks: Vec<_> = pks.iter().map(|p| p.0).collect();

        ed25519_dalek::verify_batch(&msgs, &sigs, &pks)
    }
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
    const LENGTH: usize = ed25519_dalek::SECRET_KEY_LENGTH;
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
    type AggregateSig = Ed25519AggregateSignature;
    const LENGTH: usize = ed25519_dalek::SIGNATURE_LENGTH;
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

impl Default for Ed25519Signature {
    fn default() -> Self {
        let sig = ed25519_dalek::Signature::from_bytes(&[0u8; 64]).unwrap();
        Ed25519Signature(sig)
    }
}

impl Display for Ed25519AggregateSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{:?}",
            self.0
                .iter()
                .map(|x| Base64::encode_string(x.as_ref()))
                .collect::<Vec<_>>()
        )
    }
}

impl AggregateAuthenticator for Ed25519AggregateSignature {
    type Sig = Ed25519Signature;
    type PrivKey = Ed25519PrivateKey;
    type PubKey = Ed25519PublicKey;

    /// Parse a key from its byte representation
    fn aggregate(signatures: Vec<Self::Sig>) -> Result<Self, signature::Error> {
        Ok(Self(signatures.iter().map(|s| s.0).collect()))
    }

    fn add_signature(&mut self, signature: Self::Sig) -> Result<(), signature::Error> {
        self.0.push(signature.0);
        Ok(())
    }

    fn add_aggregate(&mut self, mut signature: Self) -> Result<(), signature::Error> {
        self.0.append(&mut signature.0);
        Ok(())
    }

    fn verify(
        &self,
        pks: &[<Self::Sig as Authenticator>::PubKey],
        message: &[u8],
    ) -> Result<(), signature::Error> {
        ed25519_dalek::verify_batch(
            &vec![message; pks.len()][..],
            &self.0[..],
            &pks.iter().map(|x| x.0).collect::<Vec<_>>()[..],
        )
        .map_err(|_| signature::Error::new())?;
        Ok(())
    }

    fn batch_verify(
        sigs: &[Self],
        pks: &[&[Self::PubKey]],
        messages: &[&[u8]],
    ) -> Result<(), signature::Error> {
        if pks.len() != messages.len() || messages.len() != sigs.len() {
            return Err(signature::Error::new());
        }
        let mut inner_messages: Vec<&[u8]> = Vec::new();
        for i in 0..messages.len() {
            for _ in 0..pks[i].len() {
                inner_messages.push(messages[i]);
            }
        }

        ed25519_dalek::verify_batch(
            &inner_messages.iter().map(|x| &x[..]).collect::<Vec<_>>()[..],
            &sigs
                .iter()
                .flat_map(|x| x.0.iter().copied())
                .collect::<Vec<_>>()[..],
            &pks.iter()
                .flat_map(|x| x.iter().map(|y| y.0).collect::<Vec<_>>())
                .collect::<Vec<_>>()[..],
        )
        .map_err(|_| signature::Error::new())?;
        Ok(())
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")] // necessary so as not to deser under a != type
pub struct Ed25519KeyPair {
    name: Ed25519PublicKey,
    secret: Ed25519PrivateKey,
}

impl KeyPair for Ed25519KeyPair {
    type PubKey = Ed25519PublicKey;
    type PrivKey = Ed25519PrivateKey;
    type Sig = Ed25519Signature;

    fn public(&'_ self) -> &'_ Self::PubKey {
        &self.name
    }

    fn private(self) -> Self::PrivKey {
        self.secret
    }

    #[cfg(feature = "copy_key")]
    fn copy(&self) -> Self {
        Self {
            name: Ed25519PublicKey::from_bytes(self.name.as_ref()).unwrap(),
            secret: Ed25519PrivateKey::from_bytes(self.name.as_ref()).unwrap(),
        }
    }

    fn generate<R: rand::CryptoRng + rand::RngCore>(rng: &mut R) -> Self {
        let kp = ed25519_dalek::Keypair::generate(rng);
        Ed25519KeyPair {
            name: Ed25519PublicKey(kp.public),
            secret: Ed25519PrivateKey(kp.secret),
        }
    }

    fn generate_from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let kp = ed25519_dalek::Keypair::from_bytes(bytes).map_err(|_| signature::Error::new())?;
        let keypair = Ed25519KeyPair {
            name: Ed25519PublicKey(kp.public),
            secret: Ed25519PrivateKey(kp.secret),
        };
        Ok(keypair)
    }

    fn hkdf_generate_from_ikm(
        ikm: &[u8],
        salt: &[u8],
        info: &[u8],
    ) -> Result<Self, signature::Error> {
        let hk = Hkdf::<Sha3_256>::new(Some(salt), ikm);
        let mut okm = [0u8; SECRET_KEY_LENGTH];
        hk.expand(info, &mut okm)
            .map_err(|_| signature::Error::new())?;

        let ed25519_secret_key =
            Self::PrivKey::from_bytes(ikm).map_err(|_| signature::Error::new())?;
        let ed25519_public_key = ed25519_dalek::PublicKey::from(&ed25519_secret_key.0);

        let keypair = Ed25519KeyPair {
            name: Ed25519PublicKey(ed25519_public_key),
            secret: ed25519_secret_key,
        };
        Ok(keypair)
    }

    fn public_key_bytes(&self) -> Ed25519PublicKeyBytes {
        let pk_arr: [u8; ed25519_dalek::PUBLIC_KEY_LENGTH] = self.name.0.to_bytes();
        Ed25519PublicKeyBytes(pk_arr)
    }
}

impl FromStr for Ed25519KeyPair {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = Base64::decode_vec(s).map_err(|e| anyhow::anyhow!("{}", e.to_string()))?;
        let kp = ed25519_dalek::Keypair::from_bytes(&value)
            .map_err(|e| anyhow::anyhow!("{}", e.to_string()))?;
        Ok(Ed25519KeyPair {
            name: Ed25519PublicKey(kp.public),
            secret: Ed25519PrivateKey(kp.secret),
        })
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

impl Default for Ed25519PublicKeyBytes {
    fn default() -> Self {
        Ed25519PublicKeyBytes([0; ed25519_dalek::PUBLIC_KEY_LENGTH])
    }
}

impl AsRef<[u8]> for Ed25519PublicKeyBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Display for Ed25519PublicKeyBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let s = hex::encode(&self.0);
        write!(f, "k#{}", s)?;
        Ok(())
    }
}

impl TryInto<Ed25519PublicKey> for Ed25519PublicKeyBytes {
    type Error = signature::Error;

    fn try_into(self) -> Result<Ed25519PublicKey, Self::Error> {
        // TODO(https://github.com/MystenLabs/sui/issues/101): Do better key validation
        // to ensure the bytes represent a poin on the curve.
        Ed25519PublicKey::from_bytes(self.as_ref()).map_err(|_| Self::Error::new())
    }
}

impl ToFromBytes for Ed25519PublicKeyBytes {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let arr: [u8; ed25519_dalek::PUBLIC_KEY_LENGTH] =
            bytes.try_into().map_err(|_| signature::Error::new())?;

        Ok(Ed25519PublicKeyBytes(arr))
    }
}

impl VerifyingKeyBytes for Ed25519PublicKeyBytes {
    type PubKey = Ed25519PublicKey;
}
