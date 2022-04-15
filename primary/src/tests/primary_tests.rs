use std::time::Duration;

use crate::common::{committee_with_base_port, keys, temp_dir};

use super::*;
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use store::rocks;

use mempool::validator_client::ValidatorClient;
use mempool::GetCollectionsRequest;

pub mod mempool {
    tonic::include_proto!("mempool"); // The string specified here must match the proto package name
}

#[tokio::test]
async fn test_primary_grpc_server() {
    let keypair = keys().pop().unwrap();
    let name = keypair.public().clone();
    let signer = keypair;
    let committee = committee_with_base_port(11_000);
    let parameters = Parameters {
        batch_size: 200, // Two transactions.
        ..Parameters::default()
    };

    // Create a new test header store.
    let header_map = rocks::DBMap::<HeaderDigest, Header<Ed25519PublicKey>>::open(
        temp_dir(),
        None,
        Some("headers"),
    )
    .unwrap();
    let header_store = Store::new(header_map);

    // Create a new test certificate store.
    let certificate_map = rocks::DBMap::<CertificateDigest, Certificate<Ed25519PublicKey>>::open(
        temp_dir(),
        None,
        Some("certificates"),
    )
    .unwrap();
    let certificate_store = Store::new(certificate_map);

    // Create a new test payload store.
    let payload_map = rocks::DBMap::<(BatchDigest, WorkerId), PayloadToken>::open(
        temp_dir(),
        None,
        Some("payloads"),
    )
    .unwrap();
    let payload_store = Store::new(payload_map);

    let (tx_new_certificates, _rx_new_certificates) = channel(CHANNEL_CAPACITY);
    let (_tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);

    // Spawn a `Primary` instance.
    Primary::spawn(
        name,
        signer,
        committee.clone(),
        parameters,
        header_store,
        certificate_store,
        payload_store,
        /* tx_consensus */ tx_new_certificates,
        /* rx_consensus */ rx_feedback,
    );

    // Wait for primary to start all components (including grpc server)
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Test grpc server with client call
    let mut client = ValidatorClient::connect("http://127.0.0.1:50052")
        .await
        .unwrap();

    let request = tonic::Request::new(GetCollectionsRequest {
        collection_id: vec![1, 2, 3],
    });

    let response = client.get_collections(request).await.unwrap();

    assert_eq!(
        "Attemped fetch of [1, 2, 3], but service is not implemented!",
        response.into_inner().message
    );
}
