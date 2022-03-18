use config::Committee;
use crypto::ed25519::Ed25519PublicKey;
use crypto::traits::KeyPair;
use network::SimpleSender;
use crate::block_remover::BlockRemover;
use crate::common;
use crate::common::{certificate, committee_with_base_port, create_db_stores, keys};

#[tokio::test]
async fn test_setup() {
    // GIVEN
    let (header_store, certificate_store, _) = create_db_stores();

    // AND the necessary keys
    let (name, committee) = resolve_name_and_committee();

    // AND store certificate
    let header = common::fixture_header_with_payload(2);
    let certificate = certificate(&header);

    let mut block_remover = BlockRemover::new(
        name,
        committee,
        certificate_store,
        header_store,
        SimpleSender::new(),
    );

    block_remover.run().await;
}

// helper method to get a name and a committee
fn resolve_name_and_committee() -> (Ed25519PublicKey, Committee<Ed25519PublicKey>) {
    let mut keys = keys();
    let _ = keys.pop().unwrap(); // Skip the header' author.
    let kp = keys.pop().unwrap();
    let name = kp.public().clone();
    let committee = committee_with_base_port(13_000);

    (name, committee)
}