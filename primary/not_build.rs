//REMOVE... USE BOOTSRAP INSTEAD

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/mempool.proto")?;
    Ok(())
}
