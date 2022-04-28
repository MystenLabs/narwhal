use std::collections::HashMap;
use std::time::Duration;

use config::{Parameters, WorkerId};
use crypto::Hash;
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use primary::primary::CHANNEL_CAPACITY;
use primary::{BatchMessage, PayloadToken, Primary};
use store::{rocks, Store};
use tokio::sync::mpsc::channel;
use types::test_utils::{
    certificate, committee_with_base_port, fixture_header_with_payload, keys, temp_dir,
};
use types::{
    Batch, BatchDigest, Certificate, CertificateDigest, GetCollectionsRequest, Header,
    HeaderDigest, ValidatorClient,
};
use worker::{SerializedBatchMessage, Worker};

#[tokio::test]
async fn test_get_collections() {
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

    // Store certificate
    let header = fixture_header_with_payload(2);
    let certificate = certificate(&header);
    certificate_store
        .write(certificate.digest(), certificate.clone())
        .await;

    let collection_id = certificate.digest();

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

    Primary::spawn(
        name.clone(),
        signer,
        committee.clone(),
        parameters.clone(),
        header_store,
        certificate_store,
        payload_store,
        /* tx_consensus */ tx_new_certificates,
        /* rx_consensus */ rx_feedback,
    );

    // Wait for primary to start all components (including grpc server)
    tokio::time::sleep(Duration::from_secs(10)).await;

    // "mock" the batch responses
    let mut expected_batch_messages = HashMap::new();
    for (batch_id, _) in header.payload {
        expected_batch_messages.insert(
            batch_id,
            BatchMessage {
                id: batch_id,
                transactions: Batch(vec![vec![10u8, 5u8, 2u8], vec![8u8, 2u8, 3u8]]),
            },
        );
    }

    // Spin up a worker node for actual data (WORK IN PROGRESS)

    let worker_id = 0;

    // Create a new test store.
    let worker_map = rocks::DBMap::<BatchDigest, SerializedBatchMessage>::open(
        temp_dir(),
        None,
        Some("batches"),
    )
    .unwrap();
    let worker_store = Store::new(worker_map);

    // Spawn a `Worker` instance.
    Worker::spawn(
        name.clone(),
        worker_id,
        committee.clone(),
        parameters.clone(),
        worker_store,
    );

    /*
    let worker_address = committee
        .worker(&name, &worker_id)
        .unwrap()
        .primary_to_worker;
    let handle = worker_listener::<Ed25519PublicKey>(
        worker_address,
        expected_batch_messages.clone(),
        tx_batch_messages,
    );
    */

    // Test grpc server with client call
    let mut client = ValidatorClient::connect("http://127.0.0.1:50052")
        .await
        .unwrap();

    let request = tonic::Request::new(GetCollectionsRequest {
        collection_id: vec![collection_id.into()],
    });

    let response = client.get_collections(request).await.unwrap();

    let actual_message = response.into_inner().message;
    println!("{}", actual_message);

    // No data so expecting a BatchError for now.
    assert_eq!(true, actual_message.contains("BatchError"));
}
