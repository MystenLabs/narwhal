// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
#![warn(
    future_incompatible,
    nonstandard_style,
    rust_2018_idioms,
    rust_2021_compatibility
)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::{Committee, Import, Parameters, WorkerId};
use crypto::{ed25519::Ed25519KeyPair, generate_production_keypair, traits::KeyPair};
use executor::{
    ExecutionIndices, ExecutionState, ExecutionStateError, SerializedTransaction, SubscriberResult,
};
use futures::future::join_all;
use node::{
    metrics::{primary_metrics_registry, start_prometheus_server, worker_metrics_registry},
    Node, NodeStorage,
};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc::{channel, Receiver};
use tracing::{info, subscriber::set_global_default};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Narwhal and Tusk.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .subcommand(SubCommand::with_name("primary")
                    .about("Run a single primary")
                    .args_from_usage("-d, --consensus-disabled 'Provide this flag to run a primary node without Tusk'")
                )
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let tracing_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    // some of the network is very verbose, so we require more 'v's
    let network_tracing_level = match matches.occurrences_of("v") {
        0 | 1 => "error",
        2 => "warn",
        3 => "info",
        4 => "debug",
        _ => "trace",
    };

    // In benchmarks, transactions are not deserializable => many errors at the debug level
    cfg_if::cfg_if! {
        if #[cfg(feature = "benchmark")] {
            let custom_directive = "executor::core=info";
        } else {
            let custom_directive = "";
        }
    }

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .parse(format!(
            "{tracing_level},h2={network_tracing_level},tower={network_tracing_level},hyper={network_tracing_level},tonic::transport={network_tracing_level},{custom_directive}"
        ))?;

    let env_filter = EnvFilter::try_from_default_env().unwrap_or(filter);
    cfg_if::cfg_if! {
        if #[cfg(feature = "benchmark")] {
            let timer = tracing_subscriber::fmt::time::UtcTime::rfc_3339();
            let subscriber_builder = tracing_subscriber::fmt::Subscriber::builder()
                .with_env_filter(env_filter)
                .with_timer(timer).with_ansi(false);
        } else {
            let subscriber_builder = tracing_subscriber::fmt::Subscriber::builder()
                .with_env_filter(env_filter);
        }
    }
    let subscriber = subscriber_builder.with_writer(std::io::stderr).finish();
    set_global_default(subscriber).expect("Failed to set subscriber");

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => {
            let kp = generate_production_keypair::<Ed25519KeyPair>();
            config::Export::export(&kp, sub_matches.value_of("filename").unwrap())
                .context("Failed to generate key pair")?
        }
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

// Runs either a worker or a primary.
async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    // Read the committee and node's keypair from file.
    let keypair = Ed25519KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let committee = Arc::new(
        Committee::import(committee_file).context("Failed to load the committee information")?,
    );

    // Load default parameters if none are specified.
    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // Make the data store.
    let store = NodeStorage::reopen(store_path);

    // The channel returning the result for each transaction's execution.
    let (tx_transaction_confirmation, rx_transaction_confirmation) =
        channel(Node::CHANNEL_CAPACITY);

    let registry;

    // Check whether to run a primary, a worker, or an entire authority.
    let node_handles = match matches.subcommand() {
        // Spawn the primary and consensus core.
        ("primary", Some(sub_matches)) => {
            registry = primary_metrics_registry(keypair.public().clone());

            Node::spawn_primary(
                keypair,
                committee,
                &store,
                parameters.clone(),
                /* consensus */ !sub_matches.is_present("consensus-disabled"),
                /* execution_state */ Arc::new(SimpleExecutionState),
                tx_transaction_confirmation,
                &registry,
            )
            .await?
        }

        // Spawn a single worker.
        ("worker", Some(sub_matches)) => {
            let id = sub_matches
                .value_of("id")
                .unwrap()
                .parse::<WorkerId>()
                .context("The worker id must be a positive integer")?;

            registry = worker_metrics_registry(id, keypair.public().clone());

            Node::spawn_workers(
                /* name */
                keypair.public().clone(),
                vec![id],
                committee,
                &store,
                parameters.clone(),
                &registry,
            )
        }
        _ => unreachable!(),
    };

    // spin up prometheus server exporter
    let prom_address = parameters.prometheus_metrics.socket_addr;
    info!(
        "Starting Prometheus HTTP metrics endpoint at {}",
        prom_address
    );
    let _metrics_server_handle = start_prometheus_server(prom_address, &registry);

    // Analyze the consensus' output.
    analyze(rx_transaction_confirmation).await;

    // Await on the completion handles of all the nodes we have launched
    join_all(node_handles).await;

    // If this expression is reached, the program ends and all other tasks terminate.
    Ok(())
}

/// Receives an ordered list of certificates and apply any application-specific logic.
async fn analyze(mut rx_output: Receiver<(SubscriberResult<Vec<u8>>, SerializedTransaction)>) {
    while let Some(_message) = rx_output.recv().await {
        // NOTE: Notify the user that its transaction has been processed.
    }
}

/// A simple/dumb execution engine.
struct SimpleExecutionState;

#[async_trait]
impl ExecutionState for SimpleExecutionState {
    type Transaction = String;
    type Error = SimpleExecutionError;

    async fn handle_consensus_transaction(
        &self,
        _execution_indices: ExecutionIndices,
        _transaction: Self::Transaction,
    ) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::default())
    }

    fn ask_consensus_write_lock(&self) -> bool {
        true
    }

    fn release_consensus_write_lock(&self) {}

    async fn load_execution_indices(&self) -> Result<ExecutionIndices, Self::Error> {
        Ok(ExecutionIndices::default())
    }
}

/// A simple/dumb execution error.
#[derive(Debug, Error)]
pub enum SimpleExecutionError {
    #[error("Something went wrong in the authority")]
    ServerError,

    #[error("The client made something bad")]
    ClientError,
}

#[async_trait]
impl ExecutionStateError for SimpleExecutionError {
    fn node_error(&self) -> bool {
        match self {
            Self::ServerError => true,
            Self::ClientError => false,
        }
    }

    fn to_string(&self) -> String {
        ToString::to_string(&self)
    }
}