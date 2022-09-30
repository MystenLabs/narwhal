var sourcesIndex = JSON.parse('{\
"demo_client":["",[],["demo_client.rs"]],\
"narwhal_config":["",[],["duration_format.rs","lib.rs","utils.rs"]],\
"narwhal_consensus":["",[],["bullshark.rs","consensus.rs","dag.rs","lib.rs","metrics.rs","tusk.rs","utils.rs"]],\
"narwhal_crypto":["",[],["lib.rs"]],\
"narwhal_dag":["",[],["bft.rs","lib.rs","node_dag.rs"]],\
"narwhal_executor":["",[],["errors.rs","lib.rs","metrics.rs","notifier.rs","state.rs","subscriber.rs"]],\
"narwhal_network":["",[],["bounded_executor.rs","lib.rs","metrics.rs","p2p.rs","retry.rs","traits.rs"]],\
"narwhal_node":["",[],["execution_state.rs","lib.rs","metrics.rs","restarter.rs"]],\
"narwhal_primary":["",[["block_synchronizer",[],["handler.rs","mock.rs","mod.rs","peers.rs","responses.rs"]],["grpc_server",[],["configuration.rs","metrics.rs","mod.rs","proposer.rs","validator.rs"]]],["aggregators.rs","block_remover.rs","block_waiter.rs","certificate_waiter.rs","core.rs","header_waiter.rs","helper.rs","lib.rs","metrics.rs","payload_receiver.rs","primary.rs","proposer.rs","state_handler.rs","synchronizer.rs","utils.rs"]],\
"narwhal_storage":["",[],["certificate_store.rs","lib.rs"]],\
"narwhal_test_utils":["",[],["cluster.rs","lib.rs"]],\
"narwhal_types":["",[],["bounded_future_queue.rs","consensus.rs","error.rs","lib.rs","metered_channel.rs","primary.rs","proto.rs","serde.rs","worker.rs"]],\
"narwhal_worker":["",[],["batch_maker.rs","handlers.rs","lib.rs","metrics.rs","primary_connector.rs","processor.rs","quorum_waiter.rs","synchronizer.rs","worker.rs"]],\
"node":["",[],["main.rs"]],\
"workspace_hack":["",[],["lib.rs"]]\
}');
createSourceSidebar();
