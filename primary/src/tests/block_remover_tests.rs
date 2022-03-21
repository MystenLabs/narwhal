use crate::{
    block_remover::BlockRemover,
    common,
    common::{certificate, committee_with_base_port, create_db_stores, keys},
};
use config::Committee;
use crypto::{ed25519::Ed25519PublicKey, traits::KeyPair};
use network::SimpleSender;
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn test_setup() {
    // GIVEN
    let (header_store, certificate_store, payload_store) = create_db_stores();
    let (_tx_commands, rx_commands) = channel(10);
    let (_tx_delete_batches, rx_delete_batches) = channel(10);

    // AND the necessary keys
    let (name, committee) = resolve_name_and_committee();

    // AND store certificate
    let header = common::fixture_header_with_payload(2);
    let certificate = certificate(&header);

    BlockRemover::spawn(
        name,
        committee,
        certificate_store,
        header_store,
        payload_store,
        SimpleSender::new(),
        rx_commands,
        rx_delete_batches,
    );
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
