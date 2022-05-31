// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::fmt::{Display, Formatter};
use clap::{crate_name, crate_version, App, AppSettings, Arg, SubCommand};
use tonic::Status;
use narwhal::proposer_client::ProposerClient;
use narwhal::validator_client::ValidatorClient;
use narwhal::{
    CertificateDigest, GetCollectionsRequest, GetCollectionsResponse, NodeReadCausalRequest, PublicKey, ReadCausalRequest,
    RemoveCollectionsRequest, RoundsRequest, RoundsResponse, collection_retrieval_result::RetrievalResult,
    BatchDigest, NodeReadCausalResponse,
};

pub mod narwhal {
    tonic::include_proto!("narwhal");
}

/// Formatting the requests and responses
impl std::fmt::Display for GetCollectionsRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "*** GetCollectionsRequest ***".to_string();
        for id in &self.collection_ids {
            result = format!("{}\nid=\"{}\"", result, id);
        }
        write!(f, "{}", result)
    }
}

impl std::fmt::Display for RemoveCollectionsRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "*** RemoveCollectionsRequest ***".to_string();
        for id in &self.collection_ids {
            result = format!("{}\nid=\"{}\"", result, id);
        }
        write!(f, "{}", result)
    }
}

impl std::fmt::Display for GetCollectionsResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "*** GetCollectionsResponse ***".to_string();

        for r in self.result.clone() {
            match r.retrieval_result.unwrap() {
                RetrievalResult::Batch(message) => {
                    let batch_id = &message.id.unwrap();
                    //let batch_id = &message.id.unwrap();
                    let mut transactions_size = 0;
                    let mut num_of_transactions = 0;

                    for t in message.transactions.unwrap().transaction {
                        transactions_size += t.transaction.len();
                        num_of_transactions += 1;
                    }

                    result = format!("{}\nBatch id {}, transactions {}, size: {} bytes", result, batch_id, num_of_transactions, transactions_size);
                },
                RetrievalResult::Error(error) => {
                    //let certificate_id = base64::encode(&error.id.unwrap().digest);

                    result = format!("{}\nError for certificate id {:?}, error: {}", result, &error.id.unwrap(), error.error);
                }
            }
        }

        write!(f, "{}", result)
    }
}

impl std::fmt::Display for NodeReadCausalResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "*** NodeReadCausalResponse ***".to_string();

        for id in &self.collection_ids {
            result = format!("{}\nid=\"{}\"", result, id);
        }

        write!(f, "{}", result)
    }
}

impl std::fmt::Display for NodeReadCausalRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "**** NodeReadCausalRequest ***".to_string();

        result = format!("{}\nRequest for round {}",result, &self.round);
        result = format!("{}\nAuthority: {}", result, base64::encode(&self.public_key.clone().unwrap().bytes));

        write!(f, "{}", result)
    }
}

impl std::fmt::Display for RoundsRequest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "**** RoundsRequest ***".to_string();

        result = format!("{}\nAuthority: {}", result, base64::encode(&self.public_key.clone().unwrap().bytes));

        write!(f, "{}", result)
    }
}

impl std::fmt::Display for RoundsResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut result = "**** RoundsResponse ***".to_string();
        result = format!("{}\noldest_round: {}, newest_round: {}", result, &self.oldest_round, &self.newest_round);

        write!(f, "{}", result)
    }
}

impl std::fmt::Display for CertificateDigest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", base64::encode(&self.digest))
    }
}

impl std::fmt::Display for BatchDigest {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", base64::encode(&self.digest))
    }
}

fn println_and_into_inner<T>(result: Result<tonic::Response<T>, Status>) -> Option<T> where T: Display {
    if let Ok(response) = result {
        let inner = response.into_inner();
        println!("{}", &inner);

        return Some(inner);
    } else {
        println!("{:?}", result.err().unwrap());
    }

    None
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A gRPC client emulating the Proposer / Validator API")
        .subcommand(
            SubCommand::with_name("docker_demo")
                .about("run the demo with the hardcoded Docker deployment"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run the demo with a local gRPC server")
                .arg(
                    Arg::with_name("keys")
                        .long("keys")
                        .help("The base64-encoded publickey of the node to query")
                        .use_delimiter(true)
                        .min_values(2),
                )
                .arg(
                    Arg::with_name("ports")
                        .long("ports")
                        .help("The ports on localhost where to reach the grpc server")
                        .use_delimiter(true)
                        .min_values(2),
                ),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let mut dsts = Vec::new();
    let mut base64_keys = Vec::new();
    match matches.subcommand() {
        ("docker_demo", Some(_sub_matches)) => {
            dsts.push("http://127.0.0.1:8000".to_owned());
            base64_keys.push("Zy82aSpF8QghKE4wWvyIoTWyLetCuUSfk2gxHEtwdbg=".to_owned());
        }
        ("run", Some(sub_matches)) => {
            let ports = sub_matches
                .values_of("ports")
                .expect("Invalid ports specified");
            // TODO : check this arg is correctly formatted (number < 65536)
            for port in ports {
                dsts.push(format!("http://127.0.0.1:{port}"))
            }
            let keys = sub_matches
                .values_of("keys")
                .expect("Invalid public keys specified");
            // TODO : check this arg is correctly formatted (pk in base64)
            for key in keys {
                base64_keys.push(key.to_owned())
            }
        }
        _ => unreachable!(),
    }

    println!(
        "\n******************************** Proposer Service ********************************\n"
    );
    let mut client = ProposerClient::connect(dsts[0].clone()).await?;
    let public_key = base64::decode(&base64_keys[0]).unwrap();
    let gas_limit = 10;

    println!("\n1) Retrieve the range of rounds you have a collection for\n");
    println!("\n---- Use Rounds endpoint ----\n");

    let request = tonic::Request::new(RoundsRequest {
        public_key: Some(PublicKey {
            bytes: public_key.clone(),
        }),
    });

    println!("RoundsRequest={:?}\n", request);

    let response = client.rounds(request).await;

    let rounds_response = response.unwrap().into_inner();

    // Example 1 - just unwrap and manually print the inner response
    println!("{}", rounds_response);

    let oldest_round = rounds_response.oldest_round;
    let newest_round = rounds_response.newest_round;
    let mut round = oldest_round + 1;
    println!("\n2) Find collections from earliest round and continue to add collections until gas limit is hit\n");
    println!("\n---- Use NodeReadCausal endpoint ----\n");

    let mut collection_ids: Vec<CertificateDigest> = vec![];
    while round < newest_round && collection_ids.len() < gas_limit {
        let request = tonic::Request::new(NodeReadCausalRequest {
            public_key: Some(PublicKey {
                bytes: public_key.clone(),
            }),
            round,
        });

        println!("NodeReadCausalRequest={:?}\n", request);

        let response = client.node_read_causal(request).await;

        // Example 2 - println and get the inner structure
        // in the same time. Handle the OK / error case
        // based on the present of the value
        if let Some(node_read_causal_response) = println_and_into_inner(response) {
            if collection_ids.len() + node_read_causal_response.collection_ids.len() <= gas_limit {
                collection_ids.extend(node_read_causal_response.collection_ids);
            } else {
                println!("Reached gas limit of {gas_limit}, stopping search for more collections\n");
                break;
            }
        }

        round += 1;
    }

    println!(
        "Proposing block with {} collections!\n",
        collection_ids.len()
    );

    println!(
        "\n******************************** Validator Service ********************************\n"
    );
    let other_validator = if dsts.len() > 1 {
        dsts[1].clone()
    } else {
        // we're probably running the docker comamnd with a single endpoint
        dsts[0].clone()
    };
    let mut client = ValidatorClient::connect(other_validator).await?;

    println!("\n3) Find all causal collections from the collections found.\n");
    println!("\n---- Use ReadCausal endpoint ----\n");
    let node_read_causal_cids = collection_ids.clone();
    for collection_id in node_read_causal_cids {
        let request = tonic::Request::new(ReadCausalRequest {
            collection_id: Some(collection_id),
        });

        println!("ReadCausalRequest={:?}\n", request);

        let response = client.read_causal(request).await;

        println!("ReadCausalResponse={:?}\n", response);

        let read_causal_response = response.unwrap().into_inner();

        collection_ids.extend(read_causal_response.collection_ids);
    }

    println!("\n4) Obtain the data payload from collections found.\n");
    println!("\n---- Use GetCollections endpoint ----\n");
    let get_collections_request = GetCollectionsRequest {
        collection_ids: collection_ids.clone(),
    };

    println!("{}", get_collections_request);

    let request = tonic::Request::new(get_collections_request);

    let response = client.get_collections(request).await;

    let get_collection_response = response.unwrap().into_inner();

    println!("{}", get_collection_response);

    // TODO: This doesn't work in Docker yet, figure out why
    println!("Found {} batches", get_collection_response.result.len());

    println!("\n4) Remove collections that have been voted on and committed.\n");
    println!("\n---- Test RemoveCollections endpoint ----\n");
    let request = tonic::Request::new(RemoveCollectionsRequest { collection_ids });

    let response = client.remove_collections(request).await;

    println!("RemoveCollectionsResponse={:?}", response);

    Ok(())
}
