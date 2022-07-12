// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::traits::{
    Authenticator, EncodeDecodeBase64, KeyPair, SigningKey, ToFromBytes, VerifyingKey,
};
use base64ct::{Base64, Encoding};
use once_cell::sync::OnceCell;
use serde::de::Error as SerdeError;
use serde::{de, Deserialize, Serialize};
use serde_bytes::ByteBuf as SerdeByteBuf;
use signature::{Signature, Signer, Verifier};
use std::fmt::{self, Debug, Display};

pub const SECP256K1_PRIVATE_KEY_LENGTH: usize = 32;
pub const SECP256K1_PUBLIC_KEY_LENGTH: usize = 33;
pub const SECP256K1_SIGNATURE_LENGTH: usize = 64;

#[readonly::make]
#[derive(Debug, Clone)]
pub struct Secp256k1PublicKey {
    pub pubkey: k256::ecdsa::VerifyingKey,
    pub bytes: OnceCell<[u8; SECP256K1_PUBLIC_KEY_LENGTH]>,
}

#[readonly::make]
pub struct Secp256k1PrivateKey {
    pub privkey: k256::ecdsa::SigningKey,
    pub bytes: OnceCell<[u8; SECP256K1_PRIVATE_KEY_LENGTH]>,
}

#[readonly::make]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Secp256k1Signature {
    pub sig: k256::ecdsa::Signature,
    pub bytes: OnceCell<[u8; SECP256K1_SIGNATURE_LENGTH]>,
}

impl std::hash::Hash for Secp256k1PublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl PartialEq for Secp256k1PublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.pubkey == other.pubkey
    }
}

impl Eq for Secp256k1PublicKey {}

impl PartialOrd for Secp256k1PublicKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.pubkey.to_bytes().partial_cmp(&other.pubkey.to_bytes())
    }
}

impl Ord for Secp256k1PublicKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pubkey.to_bytes().cmp(&other.pubkey.to_bytes())
    }
}

impl VerifyingKey for Secp256k1PublicKey {
    type PrivKey = Secp256k1PrivateKey;

    type Sig = Secp256k1Signature;
}

impl Verifier<Secp256k1Signature> for Secp256k1PublicKey {
    fn verify(&self, msg: &[u8], signature: &Secp256k1Signature) -> Result<(), signature::Error> {
        self.pubkey.verify(msg, &signature.sig)
    }
}

impl AsRef<[u8]> for Secp256k1PublicKey {
    fn as_ref(&self) -> &[u8] {
        self.bytes
            .get_or_try_init::<_, eyre::Report>(|| Ok(self.pubkey.to_bytes()))
            .expect("OnceCell invariant violated")
    }
}

impl ToFromBytes for Secp256k1PublicKey {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let pubkey = k256::ecdsa::VerifyingKey::from_sec1_bytes(bytes)
            .map_err(|_e| signature::Error::new())?;
        Ok(Secp256k1PublicKey {
            pubkey,
            bytes: OnceCell::new(),
        })
    }
}

impl Default for Secp256k1PublicKey {
    fn default() -> Self {
        Secp256k1PublicKey::from_bytes(&[0u8; 32]).unwrap()
    }
}

impl Display for Secp256k1PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pubkey.fmt(f)
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl Serialize for Secp256k1PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for Secp256k1PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl Debug for Secp256k1PrivateKey {
    fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
        write!(f, "{:?}", self.privkey.to_bytes())
    }
}

impl SigningKey for Secp256k1PrivateKey {
    type PubKey = Secp256k1PublicKey;

    type Sig = Secp256k1Signature;
}

impl ToFromBytes for Secp256k1PrivateKey {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let privkey =
            k256::ecdsa::SigningKey::from_bytes(bytes).map_err(|_e| signature::Error::new())?;
        Ok(Secp256k1PrivateKey {
            privkey,
            bytes: OnceCell::new(),
        })
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl Serialize for Secp256k1PrivateKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let str = self.encode_base64();
        serializer.serialize_newtype_struct("Secp256k1PublicKey", &str)
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
impl<'de> Deserialize<'de> for Secp256k1PrivateKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl AsRef<[u8]> for Secp256k1PrivateKey {
    fn as_ref(&self) -> &[u8] {
        let mut result = [0u8; 32];
        result.copy_from_slice(&self.privkey.to_bytes());

        self.bytes
            .get_or_try_init::<_, eyre::Report>(|| Ok(result))
            .expect("OnceCell invariant violated")
    }
}

impl Serialize for Secp256k1Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_ref().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Secp256k1Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = <SerdeByteBuf>::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(SerdeError::custom)
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Default implementation just delegates to `deserialize` impl.
        *place = Deserialize::deserialize(deserializer)?;
        Ok(())
    }
}

impl Authenticator for Secp256k1Signature {
    type PubKey = Secp256k1PublicKey;

    type PrivKey = Secp256k1PrivateKey;
}

impl Signature for Secp256k1Signature {
    fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let sig =
            k256::ecdsa::Signature::from_bytes(bytes).map_err(|_e| signature::Error::new())?;
        Ok(Secp256k1Signature {
            sig,
            bytes: OnceCell::new(),
        })
    }
}

impl AsRef<[u8]> for Secp256k1Signature {
    fn as_ref(&self) -> &[u8] {
        let mut result = [0u8; 64];
        result.copy_from_slice(self.sig.as_bytes());
        self.bytes
            .get_or_try_init::<_, eyre::Report>(|| Ok(result))
            .expect("OnceCell invariant violated")
    }
}

impl Display for Secp256k1Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", Base64::encode_string(self.as_ref()))
    }
}

impl Default for Secp256k1Signature {
    fn default() -> Self {
        let sig = k256::ecdsa::Signature::from_asn1(&[0u8, 64]).unwrap();
        Secp256k1Signature {
            sig,
            bytes: OnceCell::new(),
        }
    }
}

// There is a strong requirement for this specific impl. in Fab benchmarks
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")] // necessary so as not to deser under a != type
pub struct Secp256k1KeyPair {
    pub name: Secp256k1PublicKey,
    pub secret: Secp256k1PrivateKey,
}

impl KeyPair for Secp256k1KeyPair {
    type PubKey = Secp256k1PublicKey;

    type PrivKey = Secp256k1PrivateKey;

    fn public(&'_ self) -> &'_ Self::PubKey {
        &self.name
    }

    fn private(self) -> Self::PrivKey {
        self.secret
    }

    fn generate<R: rand::CryptoRng + rand::RngCore>(rng: &mut R) -> Self {
        let privkey = k256::ecdsa::SigningKey::random(rng);
        let pubkey = k256::ecdsa::VerifyingKey::from(&privkey); // Serialize with `::to_encoded_point()`

        Secp256k1KeyPair {
            name: Secp256k1PublicKey {
                pubkey,
                bytes: OnceCell::new(),
            },
            secret: Secp256k1PrivateKey {
                privkey,
                bytes: OnceCell::new(),
            },
        }
    }
}

impl Signer<Secp256k1Signature> for Secp256k1KeyPair {
    fn try_sign(&self, msg: &[u8]) -> Result<Secp256k1Signature, signature::Error> {
        let res = &self.secret.privkey.try_sign(msg);
        match *res {
            Ok(sig) => Ok(Secp256k1Signature {
                sig,
                bytes: OnceCell::new(),
            }),
            Err(_) => Err(signature::Error::new()),
        }
    }
}
