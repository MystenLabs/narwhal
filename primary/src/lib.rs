// Copyright(C) Facebook, Inc. and its affiliates.
// SPDX-License-Identifier: Apache-2.0
#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

#[macro_use]
mod error;
mod aggregators;
mod block_waiter;
mod certificate_waiter;
mod core;
mod garbage_collector;
mod header_waiter;
mod helper;
mod messages;
mod payload_receiver;
mod primary;
mod proposer;
mod synchronizer;

#[cfg(test)]
#[path = "tests/common.rs"]
mod common;

pub use crate::{
    block_waiter::{BatchMessage, BlockCommand, BlockWaiter},
    messages::{Certificate, Header},
    primary::{PayloadToken, Primary, PrimaryWorkerMessage, Round, WorkerPrimaryMessage},
};
