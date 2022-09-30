// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use config::{Authority, Committee, Epoch, WorkerIndex, WorkerInfo};
use crypto::{KeyPair, NetworkKeyPair};
use fastcrypto::{
    traits::{KeyPair as _, Signer},
    Digest, Hash,
};
use multiaddr::Multiaddr;
use primary::PrimaryWorkerMessage;
use rand::{prelude::StdRng, SeedableRng};
use serde_reflection::{Registry, Result, Samples, Tracer, TracerConfig};
use std::{fs::File, io::Write};
use structopt::{clap::arg_enum, StructOpt};
use types::{
    Batch, BatchDigest, Certificate, CertificateDigest, Header, HeaderDigest,
    ReconfigureNotification, WorkerPrimaryError, WorkerPrimaryMessage, WorkerSynchronizeMessage,
};

fn get_registry() -> Result<Registry> {
    let mut tracer = Tracer::new(TracerConfig::default());
    let mut samples = Samples::new();
    // 1. Record samples for types with custom deserializers.
    // We want to call
    // tracer.trace_value(&mut samples, ...)?;
    // with all the base types contained in messages, especially the ones with custom serializers;
    // or involving generics (see [serde_reflection documentation](https://docs.rs/serde-reflection/latest/serde_reflection/)).
    // Trace the corresponding header
    let mut rng = StdRng::from_seed([0; 32]);
    let (keys, network_keys): (Vec<_>, Vec<_>) = (0..4)
        .map(|_| {
            (
                KeyPair::generate(&mut rng),
                NetworkKeyPair::generate(&mut rng),
            )
        })
        .unzip();

    let kp = keys[0].copy();
    let pk = kp.public().clone();

    tracer.trace_value(&mut samples, &pk)?;

    let msg = b"Hello world!";
    let signature = kp.try_sign(msg).unwrap();
    tracer.trace_value(&mut samples, &signature)?;

    let committee = Committee {
        epoch: Epoch::default(),
        authorities: keys
            .iter()
            .zip(network_keys.iter())
            .enumerate()
            .map(|(i, (kp, network_key))| {
                let id = kp.public();
                let primary_address: Multiaddr = format!("/ip4/127.0.0.1/tcp/{}/http", 100 + i)
                    .parse()
                    .unwrap();
                (
                    id.clone(),
                    Authority {
                        stake: 1,
                        primary_address,
                        network_key: network_key.public().clone(),
                    },
                )
            })
            .collect(),
    };

    let certificates: Vec<Certificate> = Certificate::genesis(&committee);

    // The values have to be "complete" in a data-centric sense, but not "correct" cryptographically.
    let mut header = Header {
        author: kp.public().clone(),
        round: 1,
        payload: (0..4u32).map(|wid| (BatchDigest([0u8; 32]), wid)).collect(),
        parents: certificates.iter().map(|x| x.digest()).collect(),
        ..Header::default()
    };

    let header_digest = header.digest();
    header = Header {
        id: header_digest,
        signature: kp.sign(Digest::from(header_digest).as_ref()),
        ..header
    };
    let worker_pk = network_keys[0].public().clone();
    let certificate = Certificate::new_unsigned(&committee, header.clone(), vec![]).unwrap();
    let signature = keys[0].sign(certificate.digest().as_ref());
    let certificate =
        Certificate::new_unsigned(&committee, header.clone(), vec![(pk.clone(), signature)])
            .unwrap();

    tracer.trace_value(&mut samples, &header)?;
    tracer.trace_value(&mut samples, &certificate)?;

    // WorkerIndex & WorkerInfo will be present in a protocol message once dynamic
    // worker integration is complete.
    let worker_index = WorkerIndex(
        vec![(
            0,
            WorkerInfo {
                name: worker_pk,
                worker_address: "/ip4/127.0.0.1/tcp/500/http".to_string().parse().unwrap(),
                transactions: "/ip4/127.0.0.1/tcp/400/http".to_string().parse().unwrap(),
            },
        )]
        .into_iter()
        .collect(),
    );
    tracer.trace_value(&mut samples, &worker_index)?;

    let cleanup = PrimaryWorkerMessage::Cleanup(1u64);
    let request_batch = PrimaryWorkerMessage::RequestBatch(BatchDigest([0u8; 32]));
    let delete_batch = PrimaryWorkerMessage::DeleteBatches(vec![BatchDigest([0u8; 32])]);
    let sync = WorkerSynchronizeMessage {
        digests: vec![BatchDigest([0u8; 32])],
        target: pk,
    };
    let epoch_change =
        PrimaryWorkerMessage::Reconfigure(ReconfigureNotification::NewEpoch(committee.clone()));
    let update_committee =
        PrimaryWorkerMessage::Reconfigure(ReconfigureNotification::NewEpoch(committee));
    let shutdown = PrimaryWorkerMessage::Reconfigure(ReconfigureNotification::Shutdown);
    tracer.trace_value(&mut samples, &cleanup)?;
    tracer.trace_value(&mut samples, &request_batch)?;
    tracer.trace_value(&mut samples, &delete_batch)?;
    tracer.trace_value(&mut samples, &sync)?;
    tracer.trace_value(&mut samples, &epoch_change)?;
    tracer.trace_value(&mut samples, &update_committee)?;
    tracer.trace_value(&mut samples, &shutdown)?;

    // 2. Trace the main entry point(s) + every enum separately.
    tracer.trace_type::<Batch>(&samples)?;
    tracer.trace_type::<BatchDigest>(&samples)?;
    tracer.trace_type::<HeaderDigest>(&samples)?;
    tracer.trace_type::<CertificateDigest>(&samples)?;

    // The final entry points that we must document
    tracer.trace_type::<WorkerPrimaryMessage>(&samples)?;
    tracer.trace_type::<WorkerPrimaryError>(&samples)?;
    tracer.registry()
}

arg_enum! {
#[derive(Debug, StructOpt, Clone, Copy)]
enum Action {
    Print,
    Test,
    Record,
}
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Narwhal format generator",
    about = "Trace serde (de)serialization to generate format descriptions for Narwhal types"
)]
struct Options {
    #[structopt(possible_values = &Action::variants(), default_value = "Print", case_insensitive = true)]
    action: Action,
}

const FILE_PATH: &str = "node/tests/staged/narwhal.yaml";

fn main() {
    let options = Options::from_args();
    let registry = get_registry().unwrap();
    match options.action {
        Action::Print => {
            let content = serde_yaml::to_string(&registry).unwrap();
            println!("{content}");
        }
        Action::Record => {
            let content = serde_yaml::to_string(&registry).unwrap();
            let mut f = File::create(FILE_PATH).unwrap();
            writeln!(f, "{}", content).unwrap();
        }
        Action::Test => {
            // If this test fails, run the following command from the folder `node`:
            // cargo -q run --example generate-format -- print > tests/staged/narwhal.yaml
            let reference = std::fs::read_to_string(FILE_PATH).unwrap();
            let reference: Registry = serde_yaml::from_str(&reference).unwrap();
            pretty_assertions::assert_eq!(reference, registry);
        }
    }
}
