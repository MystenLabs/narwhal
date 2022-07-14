// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::anyhow;
use base64ct::Encoding as _;
use blst::min_sig as blst;
use schemars::JsonSchema;
use serde::de::{Deserializer, Error};
use serde::ser::Serializer;
use serde::Deserialize;
use serde::Serialize;
use serde_with::{Bytes, DeserializeAs, SerializeAs};
use std::fmt::Debug;

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
            // Serialise to Base64 encoded String
            Base64::encode(source.to_bytes()).serialize(serializer)
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
            Base64::decode(&s).map_err(to_custom_error::<'de, D, _>)?
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
            Base64::encode(source.to_bytes()).serialize(serializer)
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
            Base64::decode(&s).map_err(to_custom_error::<'de, D, _>)?
        } else {
            Bytes::deserialize_as(deserializer)?
        };
        ed25519_dalek::Signature::from_bytes(&bytes).map_err(to_custom_error::<'de, D, _>)
    }
}

pub trait Encoding {
    fn decode(s: &str) -> Result<Vec<u8>, anyhow::Error>;
    fn encode<T: AsRef<[u8]>>(data: T) -> String;
}

#[derive(Serialize, Deserialize, Debug, JsonSchema)]
pub struct Hex(String);
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, JsonSchema)]
#[serde(try_from = "String")]
pub struct Base64(String);

impl TryFrom<String> for Base64 {
    type Error = anyhow::Error;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        // Make sure the value is valid base64 string.
        Base64::decode(&value)?;
        Ok(Self(value))
    }
}

impl Base64 {
    pub fn to_vec(&self) -> Result<Vec<u8>, anyhow::Error> {
        Self::decode(&self.0)
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(Self::encode(bytes))
    }

    pub fn encoded(&self) -> String {
        self.0.clone()
    }
}

impl Encoding for Base64 {
    fn decode(s: &str) -> Result<Vec<u8>, anyhow::Error> {
        base64ct::Base64::decode_vec(s).map_err(|e| anyhow!(e))
    }

    fn encode<T: AsRef<[u8]>>(data: T) -> String {
        base64ct::Base64::encode_string(data.as_ref())
    }
}

impl<'de> DeserializeAs<'de, Vec<u8>> for Base64 {
    fn deserialize_as<D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::decode(&s).map_err(to_custom_error::<'de, D, _>)
    }
}

impl<T> SerializeAs<T> for Base64
where
    T: AsRef<[u8]>,
{
    fn serialize_as<S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Self::encode(value).serialize(serializer)
    }
}
