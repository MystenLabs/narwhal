// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use base64ct::Encoding;
use eyre::eyre;
use hkdf::hmac::Hmac;
use rand::{CryptoRng, RngCore};

use digest::{
    block_buffer::Eager,
    consts::U256,
    core_api::{BlockSizeUser, BufferKindUser, CoreProxy, FixedOutputCore, UpdateCore},
    typenum::{IsLess, Le, NonZero, Unsigned},
    HashMarker, OutputSizeUser,
};
use serde::{de::DeserializeOwned, Serialize};
pub use signature::{Error, Signer};
use std::fmt::{Debug, Display};

pub const DEFAULT_DOMAIN: [u8; 16] = [0u8; 16];

/// Trait impl'd by concrete types that represent digital cryptographic material
/// (keys). For signatures, we rely on `signature::Signature`, which may be more widely implemented.
///
/// Key types *must* (as mandated by the `AsRef<[u8]>` bound) be a thin
/// wrapper around the "bag-of-bytes" serialized form of a key which can
/// be directly parsed from or written to the "wire".
///
/// The [`ToFromBytes`] trait aims to provide similar simplicity by minimizing
/// the number of steps involved to obtain a serializable key and
/// ideally ensuring there is one signature type for any given signature system
/// shared by all "provider" crates.
///
/// For signature systems which require a more advanced internal representation
/// (e.g. involving decoded scalars or decompressed elliptic curve points) it's
/// recommended that "provider" libraries maintain their own internal signature
/// type and use `From` bounds to provide automatic conversions.
///
// This is essentially a copy of signature::Signature:
// - we can't implement signature::Signature on Pubkeys / PrivKeys w/o violating the orphan rule,
// - and we need a trait to base the definition of EncodeDecodeBase64 as an extension trait on.
pub trait ToFromBytes: AsRef<[u8]> + Debug + Sized {
    /// Parse a key from its byte representation
    fn from_bytes(bytes: &[u8]) -> Result<Self, Error>;

    /// Borrow a byte slice representing the serialized form of this key
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

impl<T: signature::Signature> ToFromBytes for T {
    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        <Self as signature::Signature>::from_bytes(bytes)
    }
}

/// Cryptographic material with an immediate conversion to/from Base64 strings.
///
/// This is an [extension trait](https://rust-lang.github.io/rfcs/0445-extension-trait-conventions.html) of `ToFromBytes` above.
///
pub trait EncodeDecodeBase64: Sized {
    fn encode_base64(&self) -> String;
    fn decode_base64(value: &str) -> Result<Self, eyre::Report>;
}

// The Base64ct is not strictly necessary for (PubKey|Signature), but this simplifies things a lot
impl<T: ToFromBytes> EncodeDecodeBase64 for T {
    fn encode_base64(&self) -> String {
        base64ct::Base64::encode_string(self.as_bytes())
    }

    fn decode_base64(value: &str) -> Result<Self, eyre::Report> {
        let bytes = base64ct::Base64::decode_vec(value).map_err(|e| eyre!("{}", e.to_string()))?;
        <T as ToFromBytes>::from_bytes(&bytes).map_err(|e| e.into())
    }
}

/// Trait impl'd by public keys in asymmetric cryptography.
///
/// The trait bounds are implemented so as to be symmetric and equivalent
/// to the ones on its associated types for private and signature material.
///
pub trait VerifyingKey:
    Serialize
    + DeserializeOwned
    + std::hash::Hash
    + Display
    + Eq  // required to make some cached bytes representations explicit
    + Ord // required to put keys in BTreeMap
    + Default // see [#34](https://github.com/MystenLabs/narwhal/issues/34)
    + ToFromBytes
    + signature::Verifier<Self::Sig>
    + for <'a> From<&'a Self::PrivKey> // conversion PrivateKey -> PublicKey
    + Send
    + Sync
    + 'static
    + Clone
{
    type PrivKey: SigningKey<PubKey = Self>;
    type Sig: Authenticator<PubKey = Self>;
    type Bytes: VerifyingKeyBytes<PubKey = Self>;
    const LENGTH: usize;

    // Expected to be overridden by implementations
    fn verify_batch(msg: &[u8], pks: &[Self], sigs: &[Self::Sig]) -> Result<(), signature::Error> {
        if pks.len() != sigs.len() {
            return Err(signature::Error::new());
        }
        pks.iter()
            .zip(sigs)
            .try_for_each(|(pk, sig)| pk.verify(msg, sig))
    }
}

/// Trait impl'd by private (secret) keys in asymmetric cryptography.
///
/// The trait bounds are implemented so as to be symmetric and equivalent
/// to the ones on its associated types for public key and signature material.
///
pub trait SigningKey: ToFromBytes + Serialize + DeserializeOwned + Send + Sync + 'static {
    type PubKey: VerifyingKey<PrivKey = Self>;
    type Sig: Authenticator<PrivKey = Self>;
    const LENGTH: usize;
}

/// Trait impl'd by signatures in asymmetric cryptography.
///
/// The trait bounds are implemented so as to be symmetric and equivalent
/// to the ones on its associated types for private key and public key material.
///
pub trait Authenticator:
    signature::Signature
    + Display
    + Default
    + Serialize
    + DeserializeOwned
    + Send
    + Sync
    + 'static
    + Clone
{
    type PubKey: VerifyingKey<Sig = Self>;
    type PrivKey: SigningKey<Sig = Self>;
    type AggregateSig: AggregateAuthenticator<Sig = Self>;
    const LENGTH: usize;
}

/// Trait impl'd by a public / private key pair in asymmetric cryptography.
///
pub trait KeyPair: Sized + From<Self::PrivKey> {
    type PubKey: VerifyingKey<PrivKey = Self::PrivKey>;
    type PrivKey: SigningKey<PubKey = Self::PubKey>;
    type Sig: Authenticator<PubKey = Self::PubKey>;

    fn public(&'_ self) -> &'_ Self::PubKey;
    fn private(self) -> Self::PrivKey;

    #[cfg(feature = "copy_key")]
    fn copy(&self) -> Self;

    fn generate<R: CryptoRng + RngCore>(rng: &mut R) -> Self;
    fn public_key_bytes(&self) -> <Self::PubKey as VerifyingKey>::Bytes;
}

/// Creation of a keypair using the [RFC 5869](https://tools.ietf.org/html/rfc5869) HKDF specification.
/// This requires choosing an HMAC function of the correct length (conservatively, the size of a private key for this curve).
/// Despite the unsightly generics (which aim to ensure this works for a wide range of hash functions), this is straightforward to use.
///
/// Example:
/// ```rust
/// use sha3::Sha3_256;
/// use crypto::ed25519::Ed25519KeyPair;
/// use crypto::traits::hkdf_generate_from_ikm;
/// # fn main() {
///     let ikm = b"some_ikm";
///     let domain = b"my_app";
///     let salt = b"some_salt";
///     let my_keypair = hkdf_generate_from_ikm::<Sha3_256, Ed25519KeyPair>(ikm, salt, Some(domain));
///
///     let my_keypair_default_domain = hkdf_generate_from_ikm::<Sha3_256, Ed25519KeyPair>(ikm, salt, None);
/// # }
/// ```
pub fn hkdf_generate_from_ikm<'a, H: OutputSizeUser, K: KeyPair>(
    ikm: &[u8],             // IKM (32 bytes)
    salt: &[u8],            // Optional salt
    info: Option<&'a [u8]>, // Optional domain
) -> Result<K, signature::Error>
where
    // This is a tad tedious, because of hkdf's use of a sealed trait. But mostly harmless.
    H: CoreProxy + OutputSizeUser,
    H::Core: HashMarker
        + UpdateCore
        + FixedOutputCore
        + BufferKindUser<BufferKind = Eager>
        + Default
        + Clone,
    <H::Core as BlockSizeUser>::BlockSize: IsLess<U256>,
    Le<<H::Core as BlockSizeUser>::BlockSize, U256>: NonZero,
{
    let info = info.unwrap_or(&DEFAULT_DOMAIN);
    let hk = hkdf::Hkdf::<H, Hmac<H>>::new(Some(salt), ikm);
    // we need the HKDF to be able to expand precisely to the byte length of a Private key for the chosen KeyPair parameter.
    // This check is a tad over constraining (check Hkdf impl for a more relaxed variant) but is always correct.
    if H::OutputSize::USIZE != K::PrivKey::LENGTH {
        return Err(signature::Error::from_source(hkdf::InvalidLength));
    }
    let mut okm = vec![0u8; K::PrivKey::LENGTH];
    hk.expand(info, &mut okm)
        .map_err(|_| signature::Error::new())?;

    let secret_key = K::PrivKey::from_bytes(&okm[..]).map_err(|_| signature::Error::new())?;

    let keypair = K::from(secret_key);
    Ok(keypair)
}

/// Trait impl'd by aggregated signatures in asymmetric cryptography.
///
/// The trait bounds are implemented to allow the aggregation of multiple signatures,
/// and to verify it against multiple, unaggregated public keys. For signature schemes
/// where aggregation is not possible, a trivial implementation is provided.
///
pub trait AggregateAuthenticator:
    Display + Default + Serialize + DeserializeOwned + Send + Sync + 'static + Clone
{
    type Sig: Authenticator<PubKey = Self::PubKey>;
    type PubKey: VerifyingKey<Sig = Self::Sig>;
    type PrivKey: SigningKey<Sig = Self::Sig>;

    /// Parse a key from its byte representation
    fn aggregate(signatures: Vec<Self::Sig>) -> Result<Self, Error>;
    fn add_signature(&mut self, signature: Self::Sig) -> Result<(), Error>;
    fn add_aggregate(&mut self, signature: Self) -> Result<(), Error>;

    fn verify(
        &self,
        pks: &[<Self::Sig as Authenticator>::PubKey],
        message: &[u8],
    ) -> Result<(), Error>;

    fn batch_verify(
        sigs: &[Self],
        pks: &[&[Self::PubKey]],
        messages: &[&[u8]],
    ) -> Result<(), Error>;
}

/// Trait impl'd byte representations of public keys in asymmetric cryptography.
///
pub trait VerifyingKeyBytes:
    Display
    + Default
    + AsRef<[u8]>
    + TryInto<Self::PubKey>
    + Eq
    + std::hash::Hash
    + Copy
    + Ord
    + PartialOrd
    + ToFromBytes
{
    type PubKey: VerifyingKey<Bytes = Self>;
}
