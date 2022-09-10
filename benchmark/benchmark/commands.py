# Copyright(C) Facebook, Inc. and its affiliates.
from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r .db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile(mem_profiling):
        if mem_profiling:
            params = ["--profile", "bench-profiling",
                      "--features", "benchmark dhat-heap"]
        else:
            params = ["--release", "--features", "benchmark"]
        return ["cargo", "build", "--quiet"] + params

    @staticmethod
    def generate_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_keys --filename {filename}'

    @staticmethod
    def generate_network_key(filename):
        assert isinstance(filename, str)
        return f'./node generate_network_keys --filename {filename}'

    @staticmethod
    def run_primary(primary_keys, primary_network_keys, worker_keys, committee, workers, store, parameters, debug=False):
        assert isinstance(primary_keys, str)
        assert isinstance(primary_network_keys, str)
        assert isinstance(worker_keys, str)
        assert isinstance(committee, str)
        assert isinstance(workers, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --primary-keys {primary_keys} --primary-network-keys {primary_network_keys} '
                f'--worker-keys {worker_keys} --committee {committee} --workers {workers} --store {store} '
                f'--parameters {parameters} primary')

    @staticmethod
    def run_no_consensus_primary(
        primary_keys,
        primary_network_keys,
        worker_keys,
        committee,
        workers,
        store,
        parameters,
        debug=False
    ):
        assert isinstance(primary_keys, str)
        assert isinstance(primary_network_keys, str)
        assert isinstance(worker_keys, str)
        assert isinstance(committee, str)
        assert isinstance(workers, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --primary-keys {primary_keys} --primary-network-keys {primary_network_keys} '
                f'--worker-keys {worker_keys} --committee {committee} --workers {workers} --store {store} '
                f'--parameters {parameters} primary --consensus-disabled')

    @staticmethod
    def run_worker(primary_keys, primary_network_keys, worker_keys, committee, workers, store, parameters, id, debug=False):
        assert isinstance(primary_keys, str)
        assert isinstance(primary_network_keys, str)
        assert isinstance(worker_keys, str)
        assert isinstance(committee, str)
        assert isinstance(workers, str)
        assert isinstance(parameters, str)
        assert isinstance(debug, bool)
        v = '-vvv' if debug else '-vv'
        return (f'./node {v} run --primary-keys {primary_keys} --primary-network-keys {primary_network_keys} '
                f'--worker-keys {worker_keys} --committee {committee} --workers {workers} --store {store} '
                f'--parameters {parameters} worker --id {id}')

    @staticmethod
    def run_client(address, size, rate, nodes):
        assert isinstance(address, str)
        assert isinstance(size, int) and size > 0
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return f'./benchmark_client {address} --size {size} --rate {rate} {nodes}'

    @staticmethod
    def alias_demo_binaries(origin):
        assert isinstance(origin, str)
        client = join(origin, 'demo_client')
        return f'rm demo_client ; ln -s {client} .'

    @staticmethod
    def run_demo_client(keys, ports):
        assert all(isinstance(x, str) for x in keys)
        assert all(isinstance(x, int) and x > 1024 for x in ports)
        keys_string = ",".join(keys)
        ports_string = ",".join([str(x) for x in ports])
        return f'./demo_client run --keys "{keys_string}" --ports "{ports_string}"'

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'
