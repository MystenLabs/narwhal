// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crypto::ed25519::Ed25519PublicKey;
#[allow(unused_imports)] // WT*?
use crypto::traits::KeyPair;
#[allow(unused_imports)] // WT*?
use std::collections::{BTreeSet, VecDeque};
use store::{reopen, rocks, rocks::DBMap};
use test_utils::{
    make_consensus_store, make_optimal_certificates, mock_certificate, mock_committee,
};
#[allow(unused_imports)] // WT*?
use tokio::sync::mpsc::channel;
use types::CertificateDigest;

pub fn make_certificate_store(
    store_path: &std::path::Path,
) -> store::Store<CertificateDigest, Certificate<Ed25519PublicKey>> {
    const CERTIFICATES_CF: &str = "certificates";

    let rocksdb =
        rocks::open_cf(store_path, None, &[CERTIFICATES_CF]).expect("Failed creating database");

    let certificate_map = reopen!(&rocksdb,
        CERTIFICATES_CF;<CertificateDigest, Certificate<Ed25519PublicKey>>
    );

    store::Store::new(certificate_map)
}

// Run for 4 dag rounds in ideal conditions (all nodes reference all other nodes). We should commit
// the leader of round 2.
#[tokio::test]
async fn commit_one() {
    // Make certificates for rounds 1 to 4.
    let keys: Vec<_> = test_utils::keys(None)
        .into_iter()
        .map(|kp| kp.public().clone())
        .collect();
    let genesis = Certificate::genesis(&mock_committee(&keys[..]))
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();
    let (mut certificates, next_parents) = make_optimal_certificates(1, 4, &genesis, &keys);

    // Make one certificate with round 5 to trigger the commits.
    let (_, certificate) = mock_certificate(keys[0].clone(), 5, next_parents);
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    let store_path = test_utils::temp_dir();
    Consensus::spawn(
        mock_committee(&keys[..]),
        make_consensus_store(&store_path),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }

    // Ensure the first 4 ordered certificates are from round 1 (they are the parents of the committed
    // leader); then the leader's certificate should be committed.
    for _ in 1..=4 {
        let output = rx_output.recv().await.unwrap();
        assert_eq!(output.certificate.round(), 1);
    }
    let output = rx_output.recv().await.unwrap();
    assert_eq!(output.certificate.round(), 2);
}

// Run for 8 dag rounds with one dead node node (that is not a leader). We should commit the leaders of
// rounds 2, 4, and 6.
#[tokio::test]
async fn dead_node() {
    // Make the certificates.
    let mut keys: Vec<_> = test_utils::keys(None)
        .into_iter()
        .map(|kp| kp.public().clone())
        .collect();
    keys.sort(); // Ensure we don't remove one of the leaders.
    let _ = keys.pop().unwrap();

    let genesis = Certificate::genesis(&mock_committee(&keys[..]))
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let (mut certificates, _) = make_optimal_certificates(1, 9, &genesis, &keys);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    let store_path = test_utils::temp_dir();
    Consensus::spawn(
        mock_committee(&keys[..]),
        make_consensus_store(&store_path),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            tx_waiter.send(certificate).await.unwrap();
        }
    });

    // We should commit 3 leaders (rounds 2, 4, and 6).
    for i in 1..=15 {
        let output = rx_output.recv().await.unwrap();
        let expected = ((i - 1) / keys.len() as u64) + 1;
        assert_eq!(output.certificate.round(), expected);
    }
    let output = rx_output.recv().await.unwrap();
    assert_eq!(output.certificate.round(), 6);
}

// Run for 6 dag rounds. The leaders of round 2 does not have enough support, but the leader of
// round 4 does. The leader of rounds 2 and 4 should thus be committed upon entering round 6.
#[tokio::test]
async fn not_enough_support() {
    let mut keys: Vec<_> = test_utils::keys(None)
        .into_iter()
        .map(|kp| kp.public().clone())
        .collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee(&keys[..]))
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Round 1: Fully connected graph.
    let nodes: Vec<_> = keys.iter().take(3).cloned().collect();
    let (out, parents) = make_optimal_certificates(1, 1, &genesis, &nodes);
    certificates.extend(out);

    // Round 2: Fully connect graph. But remember the digest of the leader. Note that this
    // round is the only one with 4 certificates.
    let (leader_2_digest, certificate) = mock_certificate(keys[0].clone(), 2, parents.clone());
    certificates.push_back(certificate);

    let nodes: Vec<_> = keys.iter().skip(1).cloned().collect();
    let (out, mut parents) = make_optimal_certificates(2, 2, &parents, &nodes);
    certificates.extend(out);

    // Round 3: Only node 0 links to the leader of round 2.
    let mut next_parents = BTreeSet::new();

    let name = &keys[1];
    let (digest, certificate) = mock_certificate(name.clone(), 3, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    let name = &keys[2];
    let (digest, certificate) = mock_certificate(name.clone(), 3, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    let name = &keys[0];
    parents.insert(leader_2_digest);
    let (digest, certificate) = mock_certificate(name.clone(), 3, parents.clone());
    certificates.push_back(certificate);
    next_parents.insert(digest);

    parents = next_parents.clone();

    // Rounds 4, 5, and 6: Fully connected graph.
    let nodes: Vec<_> = keys.iter().take(3).cloned().collect();
    let (out, parents) = make_optimal_certificates(4, 6, &parents, &nodes);
    certificates.extend(out);

    // Round 7: Send a single certificate to trigger the commits.
    let (_, certificate) = mock_certificate(keys[0].clone(), 7, parents);
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    let store_path = test_utils::temp_dir();
    Consensus::spawn(
        mock_committee(&keys[..]),
        make_consensus_store(&store_path),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus. Only the last certificate should trigger
    // commits, so the task should not block.
    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }

    // We should commit 2 leaders (rounds 2 and 4).
    for _ in 1..=3 {
        let output = rx_output.recv().await.unwrap();
        assert_eq!(output.certificate.round(), 1);
    }
    for _ in 1..=4 {
        let output = rx_output.recv().await.unwrap();
        assert_eq!(output.certificate.round(), 2);
    }
    for _ in 1..=3 {
        let output = rx_output.recv().await.unwrap();
        assert_eq!(output.certificate.round(), 3);
    }
    let output = rx_output.recv().await.unwrap();
    assert_eq!(output.certificate.round(), 4);
}

// Run for 6 dag rounds. Node 0 (the leader of round 2) is missing for rounds 1 and 2,
// and reapers from round 3.
#[tokio::test]
async fn missing_leader() {
    let mut keys: Vec<_> = test_utils::keys(None)
        .into_iter()
        .map(|kp| kp.public().clone())
        .collect();
    keys.sort();

    let genesis = Certificate::genesis(&mock_committee(&keys[..]))
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    let mut certificates = VecDeque::new();

    // Remove the leader for rounds 1 and 2.
    let nodes: Vec<_> = keys.iter().skip(1).cloned().collect();
    let (out, parents) = make_optimal_certificates(1, 2, &genesis, &nodes);
    certificates.extend(out);

    // Add back the leader for rounds 3, 4, 5 and 6.
    let (out, parents) = make_optimal_certificates(3, 6, &parents, &keys);
    certificates.extend(out);

    // Add a certificate of round 7 to commit the leader of round 4.
    let (_, certificate) = mock_certificate(keys[0].clone(), 7, parents.clone());
    certificates.push_back(certificate);

    // Spawn the consensus engine and sink the primary channel.
    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    let store_path = test_utils::temp_dir();
    Consensus::spawn(
        mock_committee(&keys[..]),
        make_consensus_store(&store_path),
        /* gc_depth */ 50,
        rx_waiter,
        tx_primary,
        tx_output,
    );
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus. We should only commit upon receiving the last
    // certificate, so calls below should not block the task.
    while let Some(certificate) = certificates.pop_front() {
        tx_waiter.send(certificate).await.unwrap();
    }

    // Ensure the commit sequence is as expected.
    for _ in 1..=3 {
        let output = rx_output.recv().await.unwrap();
        assert_eq!(output.certificate.round(), 1);
    }
    for _ in 1..=3 {
        let output = rx_output.recv().await.unwrap();
        assert_eq!(output.certificate.round(), 2);
    }
    for _ in 1..=4 {
        let output = rx_output.recv().await.unwrap();
        assert_eq!(output.certificate.round(), 3);
    }
    let output = rx_output.recv().await.unwrap();
    assert_eq!(output.certificate.round(), 4);
}
