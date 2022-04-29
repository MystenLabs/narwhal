// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use config::{Parameters, WorkerId};
use crypto::traits::Signer;
use crypto::Hash;
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use primary::CHANNEL_CAPACITY;
use primary::{PayloadToken, Primary};
use std::time::Duration;
use store::{rocks, Store};
use tokio::sync::mpsc::channel;
use types::test_utils::{
    certificate, committee_with_base_port, fixture_batch_with_transactions, fixture_header_builder,
    keys, temp_dir,
};
use types::{
    BatchDigest, Certificate, CertificateDigest, CertificateDigestProto, CollectionRetrievalResult,
    GetCollectionsRequest, Header, HeaderDigest, ValidatorClient,
};
use worker::{SerializedBatchMessage, Worker, WorkerMessage};

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

    // Create a new test payload store.
    let payload_map = rocks::DBMap::<(BatchDigest, WorkerId), PayloadToken>::open(
        temp_dir(),
        None,
        Some("payloads"),
    )
    .unwrap();
    let payload_store = Store::new(payload_map);

    // Create a new test batch store.
    let batch_map = rocks::DBMap::<BatchDigest, SerializedBatchMessage>::open(
        temp_dir(),
        None,
        Some("batches"),
    )
    .unwrap();
    let batch_store = Store::new(batch_map);

    let worker_id = 0;
    // Headers/Collections
    let mut header_ids = Vec::new();
    // Blocks/Certficates
    let mut block_ids = Vec::new();
    let key = keys().pop().unwrap();
    let mut missing_batch = CertificateDigest::new([0; 32]);

    // Generate headers
    for n in 0..5 {
        let batch = fixture_batch_with_transactions(10);

        let header = fixture_header_builder()
            .with_payload_batch(batch.clone(), worker_id)
            .build(|payload| key.sign(payload));

        let certificate = certificate(&header);
        let block_id = certificate.digest();

        // Write the certificate
        certificate_store
            .write(certificate.digest(), certificate.clone())
            .await;

        // Write the header
        header_store.write(header.clone().id, header.clone()).await;

        header_ids.push(header.clone().id);

        // Write the batches to payload store
        payload_store
            .write_all(vec![((batch.clone().digest(), worker_id), 0)])
            .await
            .expect("couldn't store batches");

        block_ids.push(block_id);

        if n != 4 {
            // Add a batch to the workers store
            let message = WorkerMessage::<Ed25519PublicKey>::Batch(batch.clone());
            let serialized_batch = bincode::serialize(&message).unwrap();
            batch_store.write(batch.digest(), serialized_batch).await;
        } else {
            missing_batch = block_id;
        }
    }

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
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Spawn a `Worker` instance.
    Worker::spawn(
        name.clone(),
        worker_id,
        committee.clone(),
        parameters.clone(),
        batch_store,
    );

    // Test gRPC server with client call
    let mut client = ValidatorClient::connect("http://127.0.0.1:50052")
        .await
        .unwrap();

    let collection_ids = block_ids;

    // Test get no collections
    let request = tonic::Request::new(GetCollectionsRequest {
        collection_id: vec![],
    });

    let status = client.get_collections(request).await.unwrap_err();

    assert!(status
        .message()
        .contains("Attemped fetch of no collections!"));

    // Test get 1 collection
    let request = tonic::Request::new(GetCollectionsRequest {
        collection_id: vec![collection_ids[0].into()],
    });

    let response = client.get_collections(request).await.unwrap();

    let actual_result = response.into_inner().result;

    assert_eq!(1, actual_result.len());

    matches!(
        actual_result[0].retrieval_result,
        Some(types::RetrievalResult::Batch(_))
    );

    // Test get 5 collections
    let request = tonic::Request::new(GetCollectionsRequest {
        collection_id: collection_ids.iter().map(|&c_id| c_id.into()).collect(),
    });

    let response = client.get_collections(request).await.unwrap();

    let actual_result = response.into_inner().result;

    assert_eq!(5, actual_result.len());

    // One batch was intentionally left missing from the worker batch store.
    // Assert 4 Batches are returned
    assert_eq!(
        4,
        actual_result
            .iter()
            .filter(|&r| matches!(r.retrieval_result, Some(types::RetrievalResult::Batch(_))))
            .count()
    );

    // And 1 Error is returned
    let errors: Vec<&CollectionRetrievalResult> = actual_result
        .iter()
        .filter(|&r| matches!(r.retrieval_result, Some(types::RetrievalResult::Error(_))))
        .collect::<Vec<_>>();
    assert_eq!(1, errors.len());

    // And check missing batch id is correct
    let actual_missing_batch = match errors[0].retrieval_result.as_ref().unwrap() {
        types::RetrievalResult::Error(e) => e.id.as_ref(),
        _ => panic!("Should never hit this branch."),
    };
    assert_eq!(
        &CertificateDigestProto::from(missing_batch),
        actual_missing_batch.unwrap()
    );
}
