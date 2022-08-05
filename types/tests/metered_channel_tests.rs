// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use futures::{
    executor::block_on,
    task::{noop_waker, Context, Poll},
    FutureExt,
};
use prometheus::IntGauge;
use tokio::sync::mpsc::error::TrySendError;
use types::metered_channel;

#[test]
fn test_send() {
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = metered_channel::channel(8, &counter);

    assert_eq!(counter.get(), 0);
    let item = 42;
    block_on(tx.send(item)).unwrap();
    assert_eq!(counter.get(), 1);
    let received_item = block_on(rx.recv()).unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 0);
}

#[test]
fn test_empty_closed_channel() {
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = metered_channel::channel(8, &counter);

    assert_eq!(counter.get(), 0);
    let item = 42;
    block_on(tx.send(item)).unwrap();
    assert_eq!(counter.get(), 1);

    let received_item = block_on(rx.recv()).unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 0);

    // channel is empty
    let res = rx.try_recv();
    assert!(res.is_err());
    assert_eq!(counter.get(), 0);

    // channel is closed
    rx.close();
    let res2 = rx.recv().now_or_never().unwrap();
    assert!(res2.is_none());
    assert_eq!(counter.get(), 0);
}

#[test]
fn test_reserve() {
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = metered_channel::channel(8, &counter);

    assert_eq!(counter.get(), 0);
    let item = 42;
    let permit = block_on(tx.reserve()).unwrap();
    assert_eq!(counter.get(), 1);

    permit.send(42);
    let received_item = block_on(rx.recv()).unwrap();

    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 0);
}

#[test]
fn test_send_backpressure() {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = metered_channel::channel(1, &counter);

    assert_eq!(counter.get(), 0);
    block_on(tx.send(1)).unwrap();
    assert_eq!(counter.get(), 1);

    let mut task = Box::pin(tx.send(2));
    assert!(matches!(task.poll_unpin(&mut cx), Poll::Pending));
    let item = block_on(rx.recv()).unwrap();
    assert_eq!(item, 1);
    assert_eq!(counter.get(), 0);
    assert!(task.now_or_never().is_some());
    assert_eq!(counter.get(), 1);
}

#[test]
fn test_reserve_backpressure() {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = metered_channel::channel(1, &counter);

    assert_eq!(counter.get(), 0);
    let permit = block_on(tx.reserve()).unwrap();
    assert_eq!(counter.get(), 1);

    let mut task = Box::pin(tx.send(2));
    assert!(matches!(task.poll_unpin(&mut cx), Poll::Pending));

    permit.send(1);
    let item = block_on(rx.recv()).unwrap();
    assert_eq!(item, 1);
    assert_eq!(counter.get(), 0);
    assert!(task.now_or_never().is_some());
    assert_eq!(counter.get(), 1);
}

#[test]
fn test_send_backpressure_multi_senders() {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx1, mut rx) = metered_channel::channel(1, &counter);

    assert_eq!(counter.get(), 0);
    block_on(tx1.send(1)).unwrap();
    assert_eq!(counter.get(), 1);

    let tx2 = tx1;
    let mut task = Box::pin(tx2.send(2));
    assert!(matches!(task.poll_unpin(&mut cx), Poll::Pending));
    let item = block_on(rx.recv()).unwrap();
    assert_eq!(item, 1);
    assert_eq!(counter.get(), 0);
    assert!(task.now_or_never().is_some());
    assert_eq!(counter.get(), 1);
}

#[test]
fn test_try_send() {
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = metered_channel::channel(1, &counter);

    assert_eq!(counter.get(), 0);
    let item = 42;
    tx.try_send(item).unwrap();
    assert_eq!(counter.get(), 1);
    let received_item = block_on(rx.recv()).unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 0);
}

#[test]
fn test_try_send_full() {
    let counter = IntGauge::new("TEST_COUNTER", "test").unwrap();
    let (tx, mut rx) = metered_channel::channel(2, &counter);

    assert_eq!(counter.get(), 0);
    let item = 42;
    tx.try_send(item).unwrap();
    assert_eq!(counter.get(), 1);
    tx.try_send(item).unwrap();
    assert_eq!(counter.get(), 2);
    if let Err(e) = tx.try_send(item) {
        assert!(matches!(e, TrySendError::Full(_)));
    } else {
        panic!("Expect try_send return channel being full error");
    }

    let received_item = block_on(rx.recv()).unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 1);
    let received_item = block_on(rx.recv()).unwrap();
    assert_eq!(received_item, item);
    assert_eq!(counter.get(), 0);
}
