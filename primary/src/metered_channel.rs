// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)] // TODO: complete tests - This kinda sorta facades the whole tokio::mpsc::{Sender, Receiver}: without tests, this will be fragile to maintain.
use futures::{FutureExt, Stream};
use prometheus::IntGauge;
use std::task::{Context, Poll};
use tokio::sync::mpsc::{
    self,
    error::{SendError, TryRecvError, TrySendError},
    OwnedPermit, Permit,
};

#[cfg(test)]
#[path = "tests/metered_channel_tests.rs"]
mod metered_channel_tests;

/// An [`mpsc::Sender`](tokio::sync::mpsc::Sender) with an [`IntGauge`]
/// counting the number of currently queued items.
#[derive(Debug, Clone)]
pub struct Sender<T> {
    inner: mpsc::Sender<T>,
    gauge: IntGauge,
}

/// An [`mpsc::Receiver`](tokio::sync::mpsc::Receiver) with an [`IntGauge`]
/// counting the number of currently queued items.
#[derive(Debug)]
pub struct Receiver<T> {
    inner: mpsc::Receiver<T>,
    gauge: IntGauge,
}

impl<T> Receiver<T> {
    /// Receives the next value for this receiver.
    /// Decrements the gauge in case of a successful `recv`.
    pub async fn recv(&mut self) -> Option<T> {
        self.inner.recv().inspect(|_| self.gauge.dec()).await
    }

    /// Attempts to receive the next value for this receiver.
    /// Decrements the gauge in case of a successful `try_recv`.
    pub async fn try_recv(&mut self) -> Result<Option<T>, TryRecvError> {
        self.inner
            .recv()
            .map(|val| {
                self.gauge.dec();
                Ok(val)
            })
            .await
    }

    // TODO: facade [`blocking_recv`](tokio::mpsc::Receiver::blocking_recv) under the tokio feature flag "sync"

    /// Closes the receiving half of a channel without dropping it.
    pub fn close(&mut self) {
        self.inner.close()
    }

    /// Polls to receive the next message on this channel.
    ///  Decrements the gauge in case of a successful `poll_recv`.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match self.inner.poll_recv(cx) {
            Poll::Ready(Some(val)) => {
                self.gauge.dec();
                Poll::Ready(Some(val))
            }
            s => s,
        }
    }
}

impl<T> Unpin for Receiver<T> {}

impl<T> Sender<T> {
    /// Sends a value, waiting until there is capacity.
    /// Increments the gauge in case of a successful `send`.
    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        (*self)
            .inner
            .send(value)
            .inspect(|_| self.gauge.inc())
            .await
    }

    /// Completes when the receiver has dropped.
    pub async fn closed(&self) {
        self.inner.closed().await
    }

    /// Attempts to immediately send a message on this `Sender`
    /// Increments the gauge in case of a successful `try_send`.
    pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>> {
        (*self)
            .inner
            .try_send(message)
            // remove this unsightly hack once https://github.com/rust-lang/rust/issues/91345 is resolved
            .map(|val| {
                self.gauge.inc();
                val
            })
    }

    // TODO: facade [`send_timeout`](tokio::mpsc::Sender::send_timeout) under the tokio feature flag "time"
    // TODO: facade [`blocking_send`](tokio::mpsc::Sender::blocking_send) under the tokio feature flag "sync"

    /// Checks if the channel has been closed. This happens when the
    /// [`Receiver`] is dropped, or when the [`Receiver::close`] method is
    /// called.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Waits for channel capacity. Once capacity to send one message is
    /// available, it is reserved for the caller.
    /// Increments the gauge in case of a successful `reserve`.
    pub async fn reserve(&self) -> Result<Permit<'_, T>, SendError<()>> {
        self.inner
            .reserve()
            // remove this unsightly hack once https://github.com/rust-lang/rust/issues/91345 is resolved
            .map(|val| {
                self.gauge.inc();
                val
            })
            .await
    }

    /// Waits for channel capacity, moving the `Sender` and returning an owned
    /// permit. Once capacity to send one message is available, it is reserved
    /// for the caller.
    /// Increments the gauge in case of a successful `reserve_owned`.
    pub async fn reserve_owned(self) -> Result<OwnedPermit<T>, SendError<()>> {
        self.inner
            .reserve_owned()
            // remove this unsightly hack once https://github.com/rust-lang/rust/issues/91345 is resolved
            .map(|val| {
                self.gauge.inc();
                val
            })
            .await
    }

    /// Tries to acquire a slot in the channel without waiting for the slot to become
    /// available.
    /// Increments the gauge in case of a successful `try_reserve`.
    pub fn try_reserve(&self) -> Result<Permit<'_, T>, TrySendError<()>> {
        self.inner.try_reserve().map(|val| {
            // remove this unsightly hack once https://github.com/rust-lang/rust/issues/91345 is resolved
            self.gauge.inc();
            val
        })
    }

    /// Tries to acquire a slot in the channel without waiting for the slot to become
    /// available, returning an owned permit.
    ///      Increments the gauge in case of a successful `try_reserve`.
    pub fn try_reserve_owned(self) -> Result<OwnedPermit<T>, TrySendError<Self>> {
        self.inner
            .try_reserve_owned()
            .map(|val| {
                // remove this unsightly hack once https://github.com/rust-lang/rust/issues/91345 is resolved
                self.gauge.inc();
                val
            })
            .map_err(|err| match err {
                TrySendError::Full(inner) => TrySendError::Full(Sender {
                    inner,
                    gauge: self.gauge.clone(),
                }),
                TrySendError::Closed(inner) => TrySendError::Closed(Sender {
                    inner,
                    gauge: self.gauge.clone(),
                }),
            })
    }

    /// Returns `true` if senders belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        // this works if an only if we never use a constructor that unmoors the gauge from the channel
        self.inner.same_channel(&other.inner)
    }

    /// Returns the current capacity of the channel.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    // We're voluntarily not putting WeakSender under a facade.
}

////////////////////////////////
/// Stream API Wrappers!
////////////////////////////////

/// A wrapper around [`crate::metered_channel::Receiver`] that implements [`Stream`].
///
#[derive(Debug)]
pub struct ReceiverStream<T> {
    inner: Receiver<T>,
}

impl<T> ReceiverStream<T> {
    /// Create a new `ReceiverStream`.
    pub fn new(recv: Receiver<T>) -> Self {
        Self { inner: recv }
    }

    /// Get back the inner `Receiver`.
    pub fn into_inner(self) -> Receiver<T> {
        self.inner
    }

    /// Closes the receiving half of a channel without dropping it.
    pub fn close(&mut self) {
        self.inner.close()
    }
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_recv(cx)
    }
}

impl<T> AsRef<Receiver<T>> for ReceiverStream<T> {
    fn as_ref(&self) -> &Receiver<T> {
        &self.inner
    }
}

impl<T> AsMut<Receiver<T>> for ReceiverStream<T> {
    fn as_mut(&mut self) -> &mut Receiver<T> {
        &mut self.inner
    }
}

impl<T> From<Receiver<T>> for ReceiverStream<T> {
    fn from(recv: Receiver<T>) -> Self {
        Self::new(recv)
    }
}

// TODO: facade PollSender

////////////////////////////////////////////////////////////////
/// Constructor
////////////////////////////////////////////////////////////////

/// Similar to `mpsc::channel`, `channel` creates a pair of `Sender` and `Receiver`
#[track_caller]
pub fn channel<T>(size: usize, gauge: &IntGauge) -> (Sender<T>, Receiver<T>) {
    gauge.set(0);
    let (sender, receiver) = mpsc::channel(size);
    (
        Sender {
            inner: sender,
            gauge: gauge.clone(),
        },
        Receiver {
            inner: receiver,
            gauge: gauge.clone(),
        },
    )
}
