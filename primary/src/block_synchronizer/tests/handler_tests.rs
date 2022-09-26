// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{
    block_synchronizer::{
        handler::{BlockSynchronizerHandler, Error, Handler},
        BlockSynchronizeResult, SyncError,
    },
    common::create_db_stores,
    metrics::PrimaryMetrics,
    BlockHeader, MockBlockSynchronizer,
};
use fastcrypto::Hash;
use prometheus::Registry;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};
use test_utils::{fixture_payload, CommitteeFixture};
use types::{Certificate, CertificateDigest, PrimaryMessage, Round};

#[tokio::test]
async fn test_get_and_synchronize_block_headers_when_fetched_from_storage() {
    // GIVEN
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let (_, certificate_store, _) = create_db_stores();
    let (tx_block_synchronizer, rx_block_synchronizer) = test_utils::test_channel!(1);
    let (tx_core, _rx_core) = test_utils::test_channel!(1);

    let synchronizer = BlockSynchronizerHandler {
        tx_block_synchronizer,
        tx_core,
        certificate_store: certificate_store.clone(),
        certificate_deliver_timeout: Duration::from_millis(2_000),
        committee: committee.clone(),
        metrics: Arc::new(PrimaryMetrics::new(&Registry::new())),
    };

    let author = fixture.authorities().next().unwrap();

    // AND dummy certificate
    let header = author
        .header_builder(&committee)
        .payload(fixture_payload(1))
        .build(author.keypair())
        .unwrap();
    let certificate = fixture.certificate(&header);

    // AND
    let block_ids = vec![CertificateDigest::default()];

    // AND mock the block_synchronizer
    let mock_synchronizer = MockBlockSynchronizer::new(rx_block_synchronizer);
    let expected_result = vec![Ok(BlockHeader {
        certificate: certificate.clone(),
        fetched_from_storage: true,
    })];
    mock_synchronizer
        .expect_synchronize_block_headers(block_ids.clone(), expected_result, 1)
        .await;

    // WHEN
    let result = synchronizer
        .get_and_synchronize_block_headers(block_ids)
        .await;

    // THEN
    assert_eq!(result.len(), 1);

    // AND
    if let Ok(result_certificate) = result.first().unwrap().to_owned() {
        assert_eq!(result_certificate, certificate, "Certificates do not match");
    } else {
        panic!("Should have received the certificate successfully");
    }

    // AND
    mock_synchronizer.assert_expectations().await;
}

#[tokio::test]
async fn test_get_and_synchronize_block_headers_when_fetched_from_peers() {
    // GIVEN
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let (_, certificate_store, _) = create_db_stores();
    let (tx_block_synchronizer, rx_block_synchronizer) = test_utils::test_channel!(1);
    let (tx_core, mut rx_core) = test_utils::test_channel!(1);

    let synchronizer = BlockSynchronizerHandler {
        tx_block_synchronizer,
        tx_core,
        certificate_store: certificate_store.clone(),
        certificate_deliver_timeout: Duration::from_millis(2_000),
        committee: committee.clone(),
        metrics: Arc::new(PrimaryMetrics::new(&Registry::new())),
    };

    let author = fixture.authorities().next().unwrap();

    // AND a certificate stored
    let header = author
        .header_builder(&committee)
        .payload(fixture_payload(1))
        .build(author.keypair())
        .unwrap();
    let cert_stored = fixture.certificate(&header);
    certificate_store.write(cert_stored.clone()).unwrap();

    // AND a certificate NOT stored
    let header = author
        .header_builder(&committee)
        .payload(fixture_payload(2))
        .build(author.keypair())
        .unwrap();
    let cert_missing = fixture.certificate(&header);

    // AND
    let mut block_ids = HashSet::new();
    block_ids.insert(cert_stored.digest());
    block_ids.insert(cert_missing.digest());

    // AND mock the block_synchronizer where the certificate is fetched
    // from peers (fetched_from_storage = false)
    let mock_synchronizer = MockBlockSynchronizer::new(rx_block_synchronizer);
    let expected_result = vec![
        Ok(BlockHeader {
            certificate: cert_stored.clone(),
            fetched_from_storage: true,
        }),
        Ok(BlockHeader {
            certificate: cert_missing.clone(),
            fetched_from_storage: false,
        }),
    ];
    mock_synchronizer
        .expect_synchronize_block_headers(
            block_ids
                .clone()
                .into_iter()
                .collect::<Vec<CertificateDigest>>(),
            expected_result,
            1,
        )
        .await;

    // AND mock the "core" module. We assume that the certificate will be
    // stored after validated and causally complete the history.
    tokio::spawn(async move {
        match rx_core.recv().await {
            Some(PrimaryMessage::Certificate(c)) => {
                assert_eq!(c.digest(), cert_missing.digest());
                certificate_store.write(c).unwrap();
            }
            _ => panic!("Didn't receive certificate message"),
        }
    });

    // WHEN
    let result = synchronizer
        .get_and_synchronize_block_headers(
            block_ids
                .clone()
                .into_iter()
                .collect::<Vec<CertificateDigest>>(),
        )
        .await;

    // THEN
    assert_eq!(result.len(), 2);

    // AND
    for r in result {
        assert!(r.is_ok());
        assert!(block_ids.contains(&r.unwrap().digest()))
    }

    // AND
    mock_synchronizer.assert_expectations().await;
}

#[tokio::test]
async fn test_get_and_synchronize_block_headers_timeout_on_causal_completion() {
    // GIVEN
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let (_, certificate_store, _) = create_db_stores();
    let (tx_block_synchronizer, rx_block_synchronizer) = test_utils::test_channel!(1);
    let (tx_core, _rx_core) = test_utils::test_channel!(1);

    let synchronizer = BlockSynchronizerHandler {
        tx_block_synchronizer,
        tx_core,
        certificate_store: certificate_store.clone(),
        certificate_deliver_timeout: Duration::from_millis(2_000),
        committee: committee.clone(),
        metrics: Arc::new(PrimaryMetrics::new(&Registry::new())),
    };

    let author = fixture.authorities().next().unwrap();

    // AND a certificate stored
    let header = author
        .header_builder(&committee)
        .payload(fixture_payload(1))
        .build(author.keypair())
        .unwrap();
    let cert_stored = fixture.certificate(&header);
    certificate_store.write(cert_stored.clone()).unwrap();

    // AND a certificate NOT stored
    let header = author
        .header_builder(&committee)
        .payload(fixture_payload(2))
        .build(author.keypair())
        .unwrap();
    let cert_missing = fixture.certificate(&header);

    // AND
    let block_ids = vec![cert_stored.digest(), cert_missing.digest()];

    // AND mock the block_synchronizer where the certificate is fetched
    // from peers (fetched_from_storage = false)
    let mock_synchronizer = MockBlockSynchronizer::new(rx_block_synchronizer);
    let expected_result = vec![
        Ok(BlockHeader {
            certificate: cert_stored.clone(),
            fetched_from_storage: true,
        }),
        Ok(BlockHeader {
            certificate: cert_missing.clone(),
            fetched_from_storage: false,
        }),
    ];
    mock_synchronizer
        .expect_synchronize_block_headers(block_ids.clone(), expected_result, 1)
        .await;

    // Unlike test_get_and_synchronize_block_headers_when_fetched_from_peers above, core module is not mocked.

    // WHEN
    let result = synchronizer
        .get_and_synchronize_block_headers(block_ids)
        .await;

    // THEN
    assert_eq!(result.len(), 2);

    // AND
    for r in result {
        if let Ok(cert) = r {
            assert_eq!(cert_stored.digest(), cert.digest());
        } else {
            match r.err().unwrap() {
                Error::BlockDeliveryTimeout { block_id } => {
                    assert_eq!(cert_missing.digest(), block_id)
                }
                _ => panic!("Unexpected error returned"),
            }
        }
    }

    // AND
    mock_synchronizer.assert_expectations().await;
}

#[tokio::test]
async fn test_synchronize_block_payload() {
    // GIVEN
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let (_, certificate_store, payload_store) = create_db_stores();
    let (tx_block_synchronizer, rx_block_synchronizer) = test_utils::test_channel!(1);
    let (tx_core, _rx_core) = test_utils::test_channel!(1);

    let synchronizer = BlockSynchronizerHandler {
        tx_block_synchronizer,
        tx_core,
        certificate_store: certificate_store.clone(),
        certificate_deliver_timeout: Duration::from_millis(2_000),
        committee: committee.clone(),
        metrics: Arc::new(PrimaryMetrics::new(&Registry::new())),
    };

    let author = fixture.authorities().next().unwrap();

    // AND a certificate with payload already available
    let header = author
        .header_builder(&committee)
        .payload(fixture_payload(1))
        .build(author.keypair())
        .unwrap();
    let cert_stored = fixture.certificate(&header);
    for e in cert_stored.clone().header.payload {
        payload_store.write(e, 1).await;
    }

    // AND a certificate with payload NOT available
    let header = author
        .header_builder(&committee)
        .payload(fixture_payload(2))
        .build(author.keypair())
        .unwrap();
    let cert_missing = fixture.certificate(&header);

    // AND
    let block_ids = vec![cert_stored.digest(), cert_missing.digest()];

    // AND mock the block_synchronizer where the certificate is fetched
    // from peers (fetched_from_storage = false)
    let mock_synchronizer = MockBlockSynchronizer::new(rx_block_synchronizer);
    let expected_result = vec![
        Ok(BlockHeader {
            certificate: cert_stored.clone(),
            fetched_from_storage: true,
        }),
        Err(SyncError::NoResponse {
            block_id: cert_missing.digest(),
        }),
    ];
    mock_synchronizer
        .expect_synchronize_block_payload(block_ids.clone(), expected_result, 1)
        .await;

    // WHEN
    let result = synchronizer
        .synchronize_block_payloads(vec![cert_stored.clone(), cert_missing.clone()])
        .await;

    // THEN
    assert_eq!(result.len(), 2);

    // AND
    for r in result {
        if let Ok(cert) = r {
            assert_eq!(cert_stored.digest(), cert.digest());
        } else {
            match r.err().unwrap() {
                Error::PayloadSyncError { block_id, .. } => {
                    assert_eq!(cert_missing.digest(), block_id)
                }
                _ => panic!("Unexpected error returned"),
            }
        }
    }

    // AND
    mock_synchronizer.assert_expectations().await;
}

#[tokio::test]
async fn test_call_methods_with_empty_input() {
    // GIVEN
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let (_, certificate_store, _) = create_db_stores();
    let (tx_block_synchronizer, _) = test_utils::test_channel!(1);
    let (tx_core, _rx_core) = test_utils::test_channel!(1);

    let synchronizer = BlockSynchronizerHandler {
        tx_block_synchronizer,
        tx_core,
        certificate_store: certificate_store.clone(),
        certificate_deliver_timeout: Duration::from_millis(2_000),
        committee,
        metrics: Arc::new(PrimaryMetrics::new(&Registry::new())),
    };

    let result = synchronizer.synchronize_block_payloads(vec![]).await;
    assert!(result.is_empty());

    let result = synchronizer.get_and_synchronize_block_headers(vec![]).await;
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_synchronize_range() {
    // GIVEN
    let fixture = CommitteeFixture::builder().randomize_ports(true).build();
    let committee = fixture.committee();
    let (_, certificate_store, _) = create_db_stores();
    let (tx_block_synchronizer, rx_block_synchronizer) = test_utils::test_channel!(1);
    // More than 1 certificate can be inflight to core.
    let (tx_core, mut rx_core) = test_utils::test_channel!(100);

    let synchronizer = BlockSynchronizerHandler {
        tx_block_synchronizer,
        tx_core,
        certificate_store: certificate_store.clone(),
        certificate_deliver_timeout: Duration::from_millis(2_000),
        committee: committee.clone(),
        metrics: Arc::new(PrimaryMetrics::new(&Registry::new())),
    };

    let author = fixture.authorities().next().unwrap();

    // AND a certificate stored
    let header = author
        .header_builder(&committee)
        .payload(fixture_payload(1))
        .round(0)
        .build(author.keypair())
        .unwrap();
    let cert_stored = fixture.certificate(&header);
    certificate_store.write(cert_stored.clone()).unwrap();

    // AND a few certificates NOT in local store
    let mut certs_missing = BTreeMap::<Round, Vec<Certificate>>::new();
    let mut digests_missing = BTreeMap::<Round, Vec<CertificateDigest>>::new();
    let mut expected_cert_ids = Vec::<Vec<CertificateDigest>>::new();
    let mut expected_cert_results = Vec::<Vec<BlockSynchronizeResult<BlockHeader>>>::new();
    let mut expected_map = BTreeMap::<CertificateDigest, Certificate>::new();
    for r in 1..5 {
        let mut expected_certs = Vec::new();
        for i in 0..4 {
            let header = author
                .header_builder(&committee)
                .payload(fixture_payload(r))
                .round(r.into())
                .build(author.keypair())
                .unwrap();
            let cert = fixture.certificate(&header);
            if r < 4 && i < 3 {
                digests_missing
                    .entry(r.into())
                    .or_default()
                    .push(cert.digest());
                expected_certs.push(cert.clone());
                expected_map.insert(cert.digest(), cert.clone());
            }
            certs_missing.entry(r.into()).or_default().push(cert);
        }
        if !expected_certs.is_empty() {
            expected_cert_ids.push(expected_certs.iter().map(|c| c.digest()).collect());
            expected_cert_results.push(
                expected_certs
                    .into_iter()
                    .map(|c| {
                        Ok(BlockHeader {
                            certificate: c,
                            fetched_from_storage: false,
                        })
                    })
                    .collect(),
            );
        }
    }
    let total_expected = expected_map.len();

    // AND mock the block_synchronizer
    let mock_synchronizer = MockBlockSynchronizer::new(rx_block_synchronizer);
    mock_synchronizer
        .expect_synchronize_range(digests_missing.clone())
        .await;
    for (cert_ids, cert_results) in expected_cert_ids
        .clone()
        .into_iter()
        .zip(expected_cert_results.into_iter())
    {
        mock_synchronizer
            .expect_synchronize_block_headers(cert_ids, cert_results, 1)
            .await;
    }

    // AND mock the "core" module. We assume that the certificate will be
    // stored after validated and causally complete the history.
    let store = certificate_store.clone();
    tokio::spawn(async move {
        for _ in 0..total_expected {
            match rx_core.recv().await {
                Some(PrimaryMessage::Certificate(c)) => {
                    store.write(c).unwrap();
                }
                _ => panic!("Didn't receive certificate message"),
            }
        }
    });

    // WHEN
    let _ = synchronizer.synchronize_range().await;

    // THEN mock expectations should be satisfied.
    mock_synchronizer.assert_expectations().await;

    // AND missing certificates should be available in store.
    for (digest, cert) in expected_map {
        assert_eq!(certificate_store.read(digest).unwrap().unwrap(), cert);
    }
}
