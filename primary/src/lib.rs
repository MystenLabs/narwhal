// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

#[macro_use]
extern crate derive_builder;

#[macro_use]
mod error;
mod aggregators;
mod block_remover;
mod block_waiter;
mod certificate_waiter;
mod core;
mod garbage_collector;
mod grpc_server;
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
    block_remover::{BlockRemover, BlockRemoverCommand, DeleteBatchMessage},
    block_waiter::{BatchMessage, BlockCommand, BlockWaiter},
    messages::{
        Batch, BatchDigest, Certificate, CertificateDigest, Header, HeaderDigest, Transaction,
    },
    primary::{
        PayloadToken, Primary, PrimaryWorkerMessage, Round, WorkerPrimaryError,
        WorkerPrimaryMessage,
    },
};
