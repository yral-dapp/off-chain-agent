use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "contracts/projects/warehouse_events/warehouse_events.proto";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("warehouse_events_descriptor.bin"))
        .out_dir(out_dir)
        .compile(&[proto_file], &["proto"])?;

    // off_chain
    let proto_file = "contracts/projects/off_chain/off_chain.proto";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("off_chain_descriptor.bin"))
        .out_dir(out_dir)
        .compile(&[proto_file], &["proto"])?;

    // offchain_canister
    let proto_file = "contracts/projects/off_chain/offchain_canister.proto";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("offchain_canister_descriptor.bin"))
        .out_dir(out_dir)
        .compile(&[proto_file], &["proto"])?;

    let proto_file = "contracts/projects/ml_feed/ml_feed.proto";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .out_dir(out_dir)
        .compile(&[proto_file], &["proto"])?;

    Ok(())
}
