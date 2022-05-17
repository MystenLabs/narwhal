use crypto::{
    ed25519::Ed25519PublicKey,
    traits::{ToFromBytes, VerifyingKey},
};
use types::PublicKeyProto;

type Error = crypto::traits::Error;

/// The interface to map a PublicKeyProto to the provided format
/// of the type PublicKey.
pub trait PublicKeyMapper<PublicKey: VerifyingKey>: Clone + Send + Sync + 'static {
    fn map(&self, key: PublicKeyProto) -> Result<PublicKey, Error>;
}

#[derive(Clone)]
pub struct Ed25519PublicKeyMapper;

/// The implementation of he mapper to allow us convert a
/// PublicKeyProto struct to an Ed25519PublicKey.
impl PublicKeyMapper<Ed25519PublicKey> for Ed25519PublicKeyMapper {
    fn map(&self, key: PublicKeyProto) -> Result<Ed25519PublicKey, Error> {
        Ed25519PublicKey::from_bytes(key.bytes.as_ref())
    }
}
