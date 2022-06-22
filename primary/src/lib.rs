// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

mod aggregators;
mod async_proposer;
mod block_remover;
pub mod block_synchronizer;
mod block_waiter;
mod certificate_waiter;
mod core;
mod garbage_collector;
mod grpc_server;
mod header_waiter;
mod helper;
mod part_sync_proposer;
mod payload_receiver;
mod primary;
mod synchronizer;
mod utils;

#[cfg(test)]
#[path = "tests/common.rs"]
mod common;

pub use crate::{
    block_remover::{BlockRemover, BlockRemoverCommand, DeleteBatchMessage},
    block_synchronizer::{
        mock::MockBlockSynchronizer,
        responses::{CertificatesResponse, PayloadAvailabilityResponse},
        BlockHeader,
    },
    block_waiter::{BlockCommand, BlockWaiter},
    primary::{
        NetworkModel, PayloadToken, Primary, PrimaryWorkerMessage, WorkerPrimaryError,
        WorkerPrimaryMessage, CHANNEL_CAPACITY,
    },
};