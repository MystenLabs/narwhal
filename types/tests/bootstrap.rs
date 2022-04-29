use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

type Result<T> = ::std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[test]
fn bootstrap() {
    let proto_files = &["proto/narwhal.proto"];
    let dirs = &["proto"];

    println!(std::env!("CARGO_MANIFEST_DIR"));

    let out_dir = PathBuf::from(std::env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("generated");

    let config = prost_build::Config::default();

    tonic_build::configure()
        .out_dir(format!("{}", out_dir.display()))
        .compile_with_config(config, proto_files, dirs)
        .unwrap();

    prepend_license(&out_dir).unwrap();

    let status = Command::new("git")
        .arg("diff")
        .arg("--exit-code")
        .arg("--")
        .arg(format!("{}", out_dir.display()))
        .status()
        .unwrap();

    if !status.success() {
        panic!("You should commit the protobuf files");
    }
}

fn prepend_license(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            prepend_license_to_file(&path)?;
        }
    }
    Ok(())
}

const LICENSE_HEADER: &str = "\
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
";

fn prepend_license_to_file(file: &Path) -> Result<()> {
    let mut contents = fs::read_to_string(file)?;
    contents.insert_str(0, LICENSE_HEADER);
    fs::write(file, &contents)?;
    Ok(())
}
