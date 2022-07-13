// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{self, Display};

use crate::traits::{AggregateAuthenticator, EncodeDecodeBase64, ToFromBytes, VerifyingKeyBytes};
use ::ark_serialize::{CanonicalDeserialize, CanonicalSerialize};
use ark_bls12_377::{Fr, G1Affine, G1Projective, G2Affine, G2Projective};
use ark_ec::{AffineCurve, ProjectiveCurve};
use ark_ff::{
    bytes::{FromBytes, ToBytes},
    Zero,
};
use ark_std::rand::rngs::OsRng;
use base64ct::{Base64, Encoding};
use celo_bls::{hash_to_curve::try_and_increment, PublicKey};
use once_cell::sync::OnceCell;
use serde::{de, Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::Bytes;
use signature::{Signer, Verifier};

use crate::traits::{Authenticator, KeyPair, SigningKey, VerifyingKey};

mod ark_serialize;

mod rng_wrapper;

pub const CELO_BLS_PRIVATE_KEY_LENGTH: usize = 32;
pub const CELO_BLS_PUBLIC_KEY_LENGTH: usize = 96;
pub const CELO_BLS_SIGNATURE_LENGTH: usize = 48;

#[readonly::make]
#[derive(Debug, Clone)]
pub struct BLS12377PublicKey {
    pub pubkey: celo_bls::PublicKey,
    pub bytes: OnceCell<[u8; CELO_BLS_PUBLIC_KEY_LENGTH]>,
}

#[readonly::make]
#[derive(Debug)]
pub struct BLS12377PrivateKey {
    pub privkey: celo_bls::PrivateKey,
    pub bytes: OnceCell<[u8; CELO_BLS_PRIVATE_KEY_LENGTH]>,
}

#[readonly::make]
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BLS12377Signature {
    #[serde_as(as = "ark_serialize::SerdeAs")]
    pub sig: celo_bls::Signature,
    #[serde(skip)]
    #[serde(default = "OnceCell::new")]
    pub bytes: OnceCell<[u8; CELO_BLS_SIGNATURE_LENGTH]>,
}

#[readonly::make]
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BLS12377AggregateSignature {
    #[serde_as(as = "ark_serialize::SerdeAs")]
    pub sig: Option<celo_bls::Signature>,
    #[serde(skip)]
    #[serde(default = "OnceCell::new")]
    pub bytes: OnceCell<[u8; CELO_BLS_SIGNATURE_LENGTH]>,
}

#[readonly::make]
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Ord, PartialOrd, Copy, Hash)]
pub struct BLS12377PublicKeyBytes(#[serde_as(as = "Bytes")] [u8; CELO_BLS_PUBLIC_KEY_LENGTH]);

impl signature::Signature for BLS12377Signature {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let g1 = <G1Affine as CanonicalDeserialize>::deserialize(bytes)
            .map_err(|_| signature::Error::new())?;
        Ok(BLS12377Signature {
            sig: g1.into_projective().into(),
            bytes: OnceCell::new(),
        })
    }
}
// see [#34](https://github.com/MystenLabs/narwhal/issues/34)
impl Default for BLS12377Signature {
    fn default() -> Self {
        let g1 = G1Projective::zero();
        BLS12377Signature {
            sig: g1.into(),
            bytes: OnceCell::new(),
        }
    }
}

impl AsRef<[u8]> for BLS12377Signature {
    fn as_ref(&self) -> &[u8] {
        self.bytes
            .get_or_try_init::<_, eyre::Report>(|| {
                let mut bytes = [0u8; CELO_BLS_SIGNATURE_LENGTH];
                self.sig.as_ref().into_affine().serialize(&mut bytes[..])?;
                Ok(bytes)
            })
            .expect("OnceCell invariant violated")
    }
}

impl Display for BLS12377Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", Base64::encode_string(self.as_ref()))
    }
}

impl PartialEq for BLS12377Signature {
    fn eq(&self, other: &Self) -> bool {
        self.sig == other.sig
    }
}

impl Eq for BLS12377Signature {}

impl ToFromBytes for BLS12377Signature {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let g1 = <G1Projective as CanonicalDeserialize>::deserialize(bytes)
            .map_err(|_| signature::Error::new())?;
        Ok(BLS12377Signature {
            sig: g1.into(),
            bytes: OnceCell::new(),
        })
    }
}

impl Authenticator for BLS12377Signature {
    type PubKey = BLS12377PublicKey;
    type PrivKey = BLS12377PrivateKey;
    type AggregateSig = BLS12377AggregateSignature;
    const LENGTH: usize = CELO_BLS_SIGNATURE_LENGTH;
}

impl Default for BLS12377PublicKey {
    // eprint.iacr.org/2021/323 should incite us to remove our usage of Default,
    // see https://github.com/MystenLabs/narwhal/issues/34
    fn default() -> Self {
        let public: PublicKey = G2Projective::zero().into();
        BLS12377PublicKey {
            pubkey: public,
            bytes: OnceCell::new(),
        }
    }
}

impl std::hash::Hash for BLS12377PublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pubkey.hash(state);
    }
}

impl PartialEq for BLS12377PublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.pubkey == other.pubkey
    }
}

impl Eq for BLS12377PublicKey {}

impl AsRef<[u8]> for BLS12377PublicKey {
    fn as_ref(&self) -> &[u8] {
        self.bytes
            .get_or_try_init::<_, eyre::Report>(|| {
                let mut bytes = [0u8; CELO_BLS_PUBLIC_KEY_LENGTH];
                self.pubkey
                    .as_ref()
                    .into_affine()
                    .serialize(&mut bytes[..])
                    .unwrap();
                Ok(bytes)
            })
            .expect("OnceCell invariant violated")
    }
}

impl PartialOrd for BLS12377PublicKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}

impl Ord for BLS12377PublicKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl ToFromBytes for BLS12377PublicKey {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let g2 = <G2Affine as CanonicalDeserialize>::deserialize(bytes)
            .map_err(|_| signature::Error::new())?
            .into_projective();
        Ok(BLS12377PublicKey {
            pubkey: g2.into(),
            bytes: OnceCell::new(),
        })
    }
}

impl Display for BLS12377PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", Base64::encode_string(self.as_ref()))
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl Serialize for BLS12377PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl<'de> Deserialize<'de> for BLS12377PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl Verifier<BLS12377Signature> for BLS12377PublicKey {
    fn verify(&self, msg: &[u8], signature: &BLS12377Signature) -> Result<(), signature::Error> {
        let hash_to_g1 = &*celo_bls::hash_to_curve::try_and_increment::COMPOSITE_HASH_TO_G1;

        self.pubkey
            .verify(msg, &[], &signature.sig, hash_to_g1)
            .map_err(|_| signature::Error::new())
    }
}

impl VerifyingKey for BLS12377PublicKey {
    type PrivKey = BLS12377PrivateKey;
    type Sig = BLS12377Signature;
    type Bytes = BLS12377PublicKeyBytes;
    const LENGTH: usize = CELO_BLS_PUBLIC_KEY_LENGTH;

    fn verify_batch(msg: &[u8], pks: &[Self], sigs: &[Self::Sig]) -> Result<(), signature::Error> {
        if pks.len() != sigs.len() {
            return Err(signature::Error::new());
        }
        let mut batch = celo_bls::bls::Batch::new(msg, &[]);
        pks.iter()
            .zip(sigs)
            .for_each(|(pk, sig)| batch.add(pk.pubkey.clone(), sig.sig.clone()));
        let hash_to_g1 = &*celo_bls::hash_to_curve::try_and_increment::COMPOSITE_HASH_TO_G1;
        batch
            .verify(hash_to_g1)
            .map_err(|_| signature::Error::new())
    }
}

impl AsRef<[u8]> for BLS12377PrivateKey {
    fn as_ref(&self) -> &[u8] {
        self.bytes
            .get_or_try_init::<_, eyre::Report>(|| {
                let mut bytes = [0u8; CELO_BLS_PRIVATE_KEY_LENGTH];
                self.privkey.as_ref().write(&mut bytes[..])?;
                Ok(bytes)
            })
            .expect("OnceCell invariant violated")
    }
}

impl ToFromBytes for BLS12377PrivateKey {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let fr = <Fr as FromBytes>::read(bytes).map_err(|_| signature::Error::new())?;
        Ok(BLS12377PrivateKey {
            privkey: fr.into(),
            bytes: OnceCell::new(),
        })
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl Serialize for BLS12377PrivateKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl<'de> Deserialize<'de> for BLS12377PrivateKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl SigningKey for BLS12377PrivateKey {
    type PubKey = BLS12377PublicKey;
    type Sig = BLS12377Signature;
    const LENGTH: usize = CELO_BLS_PRIVATE_KEY_LENGTH;
}

impl Signer<BLS12377Signature> for BLS12377PrivateKey {
    fn try_sign(&self, msg: &[u8]) -> Result<BLS12377Signature, signature::Error> {
        let hash_to_g1 = &*celo_bls::hash_to_curve::try_and_increment::COMPOSITE_HASH_TO_G1;

        let celo_bls_sig = self
            .privkey
            .sign(msg, &[], hash_to_g1)
            .map_err(|_| signature::Error::new())?;

        Ok(BLS12377Signature {
            sig: celo_bls_sig,
            bytes: OnceCell::new(),
        })
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")] // necessary so as not to deser under a != type
pub struct BLS12377KeyPair {
    name: BLS12377PublicKey,
    secret: BLS12377PrivateKey,
}

impl KeyPair for BLS12377KeyPair {
    type PubKey = BLS12377PublicKey;
    type PrivKey = BLS12377PrivateKey;
    type Sig = BLS12377Signature;

    #[cfg(feature = "copy_key")]
    fn copy(&self) -> Self {
        BLS12377KeyPair {
            name: self.name.clone(),
            secret: BLS12377PrivateKey::from_bytes(self.secret.as_ref()).unwrap(),
        }
    }

    fn public(&'_ self) -> &'_ Self::PubKey {
        &self.name
    }

    fn private(self) -> Self::PrivKey {
        self.secret
    }

    fn generate<R: rand::CryptoRng + rand::RngCore>(rng: &mut R) -> Self {
        let celo_privkey = celo_bls::PrivateKey::generate(&mut rng_wrapper::RngWrapper(rng));
        let celo_pubkey = PublicKey::from(&celo_privkey);
        BLS12377KeyPair {
            name: BLS12377PublicKey {
                pubkey: celo_pubkey,
                bytes: OnceCell::new(),
            },
            secret: BLS12377PrivateKey {
                privkey: celo_privkey,
                bytes: OnceCell::new(),
            },
        }
    }

    fn generate_from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let public_key_bytes = &bytes[..CELO_BLS_PUBLIC_KEY_LENGTH];
        let secret_key_bytes = &bytes[CELO_BLS_PUBLIC_KEY_LENGTH..];
        Ok(BLS12377KeyPair {
            name: BLS12377PublicKey::from_bytes(public_key_bytes)?,
            secret: BLS12377PrivateKey::from_bytes(secret_key_bytes)?,
        })
    }

    fn hkdf_generate_from_ikm(
        _ikm: &[u8],
        _salt: &[u8],
        _info: &[u8],
    ) -> Result<Self, signature::Error> {
        // Not yet implemented!
        let celo_privkey = celo_bls::PrivateKey::generate(&mut OsRng);
        let celo_pubkey = PublicKey::from(&celo_privkey);
        Ok(BLS12377KeyPair {
            name: BLS12377PublicKey {
                pubkey: celo_pubkey,
                bytes: OnceCell::new(),
            },
            secret: BLS12377PrivateKey {
                privkey: celo_privkey,
                bytes: OnceCell::new(),
            },
        })
    }

    fn public_key_bytes(&self) -> BLS12377PublicKeyBytes {
        let bytes = self
            .name
            .bytes
            .get_or_try_init::<_, eyre::Report>(|| {
                let mut bytes = [0u8; CELO_BLS_PUBLIC_KEY_LENGTH];
                self.name
                    .pubkey
                    .as_ref()
                    .into_affine()
                    .serialize(&mut bytes[..])
                    .map_err(|_| eyre::eyre!("Failed to serialize public key"))?;
                Ok(bytes)
            })
            .expect("OnceCell invariant violated");

        BLS12377PublicKeyBytes::from_bytes(bytes).unwrap()
    }
}

impl Signer<BLS12377Signature> for BLS12377KeyPair {
    fn try_sign(&self, msg: &[u8]) -> Result<BLS12377Signature, signature::Error> {
        let hash_to_g1 = &*celo_bls::hash_to_curve::try_and_increment::COMPOSITE_HASH_TO_G1;

        let celo_bls_sig = self
            .secret
            .privkey
            .sign(msg, &[], hash_to_g1)
            .map_err(|_| signature::Error::new())?;

        Ok(BLS12377Signature {
            sig: celo_bls_sig,
            bytes: OnceCell::new(),
        })
    }
}

impl ToFromBytes for BLS12377AggregateSignature {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let g1 = <G1Projective as CanonicalDeserialize>::deserialize(bytes)
            .map_err(|_| signature::Error::new())?;
        Ok(BLS12377AggregateSignature {
            sig: Some(g1.into()),
            bytes: OnceCell::new(),
        })
    }
}

impl AsRef<[u8]> for BLS12377AggregateSignature {
    fn as_ref(&self) -> &[u8] {
        match &self.sig {
            Some(sig) => self
                .bytes
                .get_or_try_init::<_, eyre::Report>(|| {
                    let mut bytes = [0u8; CELO_BLS_SIGNATURE_LENGTH];
                    sig.as_ref().into_affine().serialize(&mut bytes[..])?;
                    Ok(bytes)
                })
                .expect("OnceCell invariant violated"),
            None => &[],
        }
    }
}

impl Display for BLS12377AggregateSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", Base64::encode_string(self.as_ref()))
    }
}

// see [#34](https://github.com/MystenLabs/narwhal/issues/34)
impl Default for BLS12377AggregateSignature {
    fn default() -> Self {
        BLS12377AggregateSignature {
            sig: None,
            bytes: OnceCell::new(),
        }
    }
}

impl AggregateAuthenticator for BLS12377AggregateSignature {
    type PrivKey = BLS12377PrivateKey;
    type PubKey = BLS12377PublicKey;
    type Sig = BLS12377Signature;

    /// Parse a key from its byte representation
    fn aggregate(signatures: Vec<Self::Sig>) -> Result<Self, signature::Error> {
        let sig = celo_bls::Signature::aggregate(signatures.iter().map(|x| &x.sig));
        Ok(BLS12377AggregateSignature {
            sig: Some(sig),
            bytes: OnceCell::new(),
        })
    }

    fn add_signature(&mut self, signature: Self::Sig) -> Result<(), signature::Error> {
        match self.sig {
            Some(ref mut sig) => {
                let raw_sig = celo_bls::Signature::aggregate([signature.sig, sig.clone()]);
                self.sig = Some(raw_sig);
                Ok(())
            }
            None => {
                self.sig = Some(signature.sig);
                Ok(())
            }
        }
    }

    fn add_aggregate(&mut self, signature: Self) -> Result<(), signature::Error> {
        match self.sig {
            Some(ref mut sig) => {
                let raw_sig = celo_bls::Signature::aggregate([
                    signature.sig.ok_or_else(signature::Error::new)?,
                    sig.clone(),
                ]);
                self.sig = Some(raw_sig);
                Ok(())
            }
            None => {
                self.sig = signature.sig;
                Ok(())
            }
        }
    }

    fn verify(
        &self,
        pks: &[<Self::Sig as Authenticator>::PubKey],
        message: &[u8],
    ) -> Result<(), signature::Error> {
        let mut cache = celo_bls::PublicKeyCache::new();
        let apk = cache.aggregate(pks.iter().map(|pk| pk.pubkey.clone()).collect());
        apk.verify(
            message,
            &[],
            &self.sig.clone().ok_or_else(signature::Error::new)?,
            &*try_and_increment::COMPOSITE_HASH_TO_G1,
        )
        .map_err(|_| signature::Error::new())?;

        Ok(())
    }

    fn batch_verify(
        signatures: &[Self],
        pks: &[&[Self::PubKey]],
        messages: &[&[u8]],
    ) -> Result<(), signature::Error> {
        if pks.len() != messages.len() || messages.len() != signatures.len() {
            return Err(signature::Error::new());
        }
        for i in 0..signatures.len() {
            let sig = signatures[i].sig.clone();
            let mut cache = celo_bls::PublicKeyCache::new();
            let apk = cache.aggregate(pks[i].iter().map(|pk| pk.pubkey.clone()).collect());
            apk.verify(
                messages[i],
                &[],
                &sig.ok_or_else(signature::Error::new)?,
                &*try_and_increment::COMPOSITE_HASH_TO_G1,
            )
            .map_err(|_| signature::Error::new())?;
        }
        Ok(())
    }
}

impl Default for BLS12377PublicKeyBytes {
    fn default() -> Self {
        BLS12377PublicKeyBytes([0; CELO_BLS_PUBLIC_KEY_LENGTH])
    }
}

impl AsRef<[u8]> for BLS12377PublicKeyBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Display for BLS12377PublicKeyBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let s = hex::encode(&self.0);
        write!(f, "k#{}", s)?;
        Ok(())
    }
}

impl TryInto<BLS12377PublicKey> for BLS12377PublicKeyBytes {
    type Error = signature::Error;

    fn try_into(self) -> Result<BLS12377PublicKey, Self::Error> {
        // TODO(https://github.com/MystenLabs/sui/issues/101): Do better key validation
        // to ensure the bytes represent a poin on the curve.
        BLS12377PublicKey::from_bytes(self.as_ref()).map_err(|_| Self::Error::new())
    }
}

impl ToFromBytes for BLS12377PublicKeyBytes {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let arr: [u8; CELO_BLS_PUBLIC_KEY_LENGTH] =
            bytes.try_into().map_err(|_| signature::Error::new())?;

        Ok(BLS12377PublicKeyBytes(arr))
    }
}

impl VerifyingKeyBytes for BLS12377PublicKeyBytes {
    type PubKey = BLS12377PublicKey;
}
