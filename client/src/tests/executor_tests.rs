// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use super::*;
use crate::{
    execution_state::{TestState, KILLER_TRANSACTION, MALFORMED_TRANSACTION},
    fixtures::{test_batch, test_certificate, test_store, test_u64_certificates},
};
use crypto::ed25519::Ed25519PublicKey;
use primary::Certificate;
use std::sync::Arc;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn execute_transactions() {
    let (tx_executor, rx_executor) = channel(10);
    let (tx_output, mut rx_output) = channel(10);

    // Spawn the executor.
    let store = test_store();
    let execution_state = Arc::new(TestState::default());
    let execution_indices = ExecutionIndices::default();

    Executor::<TestState, Ed25519PublicKey>::spawn(
        store.clone(),
        execution_state.clone(),
        /* rx_subscriber */ rx_executor,
        tx_output,
        execution_indices,
    );

    // Feed certificates to the mock sequencer and add the transaction data to storage (as if
    // the batch loader downloaded them).
    let certificates = test_u64_certificates(
        /* certificates */ 2, /* batches_per_certificate */ 2,
        /* transactions_per_batch */ 2,
    );
    for (i, (certificate, batches)) in certificates.into_iter().enumerate() {
        for (digest, batch) in batches {
            store.write(digest, batch).await;
        }
        let message = ConsensusOutput {
            certificate,
            consensus_index: i as SequenceNumber,
        };
        tx_executor.send(message).await.unwrap();
    }

    // Ensure the execution state is updated accordingly.
    let _ = rx_output.recv().await;
    let expected = ExecutionIndices {
        next_certificate_index: 2,
        next_batch_index: 0,
        next_transaction_index: 0,
    };
    assert_eq!(execution_state.get_execution_indices().await, expected);
}

#[tokio::test]
async fn execute_empty_certificate() {
    let (tx_executor, rx_executor) = channel(10);
    let (tx_output, mut rx_output) = channel(10);

    // Spawn the executor.
    let store = test_store();
    let execution_state = Arc::new(TestState::default());
    let execution_indices = ExecutionIndices::default();

    Executor::<TestState, Ed25519PublicKey>::spawn(
        store.clone(),
        execution_state.clone(),
        /* rx_subscriber */ rx_executor,
        tx_output,
        execution_indices,
    );

    // Feed empty certificates to the executor.
    let empty_certificates = 2;
    for i in 0..empty_certificates {
        let message = ConsensusOutput {
            certificate: Certificate::default(),
            consensus_index: i as SequenceNumber,
        };
        tx_executor.send(message).await.unwrap();
    }

    // Then feed one non-empty certificate.
    let certificates = test_u64_certificates(
        /* certificates */ 1, /* batches_per_certificate */ 2,
        /* transactions_per_batch */ 2,
    );
    for (i, (certificate, batches)) in certificates.into_iter().enumerate() {
        for (digest, batch) in batches {
            store.write(digest, batch).await;
        }
        let message = ConsensusOutput {
            certificate,
            consensus_index: empty_certificates + i as SequenceNumber,
        };
        tx_executor.send(message).await.unwrap();
    }

    // Ensure the certificate index is updated.
    let _ = rx_output.recv().await;
    let expected = ExecutionIndices {
        next_certificate_index: 3,
        next_batch_index: 0,
        next_transaction_index: 0,
    };
    assert_eq!(execution_state.get_execution_indices().await, expected);
}

#[tokio::test]
async fn execute_malformed_transactions() {
    let (tx_executor, rx_executor) = channel(10);
    let (tx_output, mut rx_output) = channel(10);

    // Spawn the executor.
    let store = test_store();
    let execution_state = Arc::new(TestState::default());
    let execution_indices = ExecutionIndices::default();

    Executor::<TestState, Ed25519PublicKey>::spawn(
        store.clone(),
        execution_state.clone(),
        /* rx_subscriber */ rx_executor,
        tx_output,
        execution_indices,
    );

    // Feed a malformed transaction to the mock sequencer
    let tx0 = MALFORMED_TRANSACTION;
    let tx1 = 10;
    let (digest, batch) = test_batch(vec![tx0, tx1]);

    store.write(digest, batch).await;

    let payload = [(digest, 0)].iter().cloned().collect();
    let certificate = test_certificate(payload);

    let message = ConsensusOutput {
        certificate,
        consensus_index: SequenceNumber::default(),
    };
    tx_executor.send(message).await.unwrap();

    // Feed two certificates with good transactions to the mock sequencer.
    let certificates = test_u64_certificates(
        /* certificates */ 2, /* batches_per_certificate */ 2,
        /* transactions_per_batch */ 2,
    );
    for (i, (certificate, batches)) in certificates.into_iter().enumerate() {
        for (digest, batch) in batches {
            store.write(digest, batch).await;
        }
        let message = ConsensusOutput {
            certificate,
            consensus_index: 1 + i as SequenceNumber,
        };
        tx_executor.send(message).await.unwrap();
    }

    // Ensure the execution state is updated accordingly.
    let _ = rx_output.recv().await;
    let expected = ExecutionIndices {
        next_certificate_index: 3,
        next_batch_index: 0,
        next_transaction_index: 0,
    };
    assert_eq!(execution_state.get_execution_indices().await, expected);
}

#[tokio::test]
async fn internal_error_execution() {
    let (tx_executor, rx_executor) = channel(10);
    let (tx_output, mut rx_output) = channel(10);

    // Spawn the executor.
    let store = test_store();
    let execution_state = Arc::new(TestState::default());
    let execution_indices = ExecutionIndices::default();

    Executor::<TestState, Ed25519PublicKey>::spawn(
        store.clone(),
        execution_state.clone(),
        /* rx_subscriber */ rx_executor,
        tx_output,
        execution_indices,
    );

    // Feed a 'killer' transaction to the mock sequencer. This is a special test transaction that
    // crashes the test executor engine.
    let tx0 = KILLER_TRANSACTION;
    let tx1 = 10;
    let (digest, batch) = test_batch(vec![tx0, tx1]);

    store.write(digest, batch).await;

    let payload = [(digest, 0)].iter().cloned().collect();
    let certificate = test_certificate(payload);

    let message = ConsensusOutput {
        certificate,
        consensus_index: SequenceNumber::default(),
    };
    tx_executor.send(message).await.unwrap();

    // Ensure the execution state does not change.
    let _ = rx_output.recv().await;
    let expected = ExecutionIndices::default();
    assert_eq!(execution_state.get_execution_indices().await, expected);
}
