#[path = "generated/narwhal.rs"]
#[rustfmt::skip]
mod narwhal;

pub use narwhal::{
    collection_retrieval_result::RetrievalResult,
    validator_client::ValidatorClient,
    validator_server::{Validator, ValidatorServer},
    Batch as BatchProto, BatchDigest as BatchDigestProto, BatchMessage as BatchMessageProto,
    BlockError as BlockErrorProto, BlockErrorType as BlockErrorTypeProto,
    CertificateDigest as CertificateDigestProto, CollectionRetrievalResult, GetCollectionsRequest,
    GetCollectionsResponse, Transaction as TransactionProto,
};
