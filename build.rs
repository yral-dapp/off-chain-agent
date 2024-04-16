// fn main() {
//     tonic_build::compile_protos("proto/warehouse_events.proto")
//         .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
// }

use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "contracts/projects/warehouse_events/warehouse_events.proto";
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("warehouse_events_descriptor.bin"))
        .out_dir(out_dir)
        .compile(&[proto_file], &["proto"])?;

    Ok(())
}
