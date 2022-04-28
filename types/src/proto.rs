#[path = "generated/narwhal.rs"]
#[rustfmt::skip]
mod narwhal;

pub use narwhal::{
    validator_client::ValidatorClient,
    validator_server::{Validator, ValidatorServer},
    CertificateDigest as CertificateDigestProto, GetCollectionsRequest, GetCollectionsResponse,
};
