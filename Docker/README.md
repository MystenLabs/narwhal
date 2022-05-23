### Introduction

This directory contains all the necessary configuration to allow someone
quickly setup and spin-up a small Narwhal cluster via [docker-compose](https://docs.docker.com/compose/).

Under this directory there will be found the following 2 things:
* The `Docker` file definition for a Narwhal node
* A `docker-compose` file to allow someone quickly spin-up a Narwhal cluster

### Quick start

The following dependencies must be installed before continuing further.

#### Docker
Please find installation info [here](https://docs.docker.com/get-docker/)

#### Docker-compose
Please find installation info [here](https://docs.docker.com/compose/install/)

After having installed `Docker` & `docker-compose`, next step will be to
start the cluster via the following command:
```
docker-compose -f docker-compose.yml up
```
The first time this will run, it will build the narwhal docker image (this can take a few minutes
since the narwhal node binary needs to be built from the source code) and then it will spin up 
a cluster for `4 nodes` by doing the necessary setup for `primary` and `worker` nodes. Each
`primary` node will be connected to `1 worker` node.

### Access primary node gRPC endpoint

The nodes by default are running with the `Tusk` algorithm disabled, which basically allow
to user to treat Narwhal as a pure mempool. When that happens, the gRPC server is bootstrapped
for the primary nodes and that allow someone to interact with the node.

The docker-compose file is exporting for the `primary` nodes the gRPC server's
port so it can be accessible from the host machine. For the default setup of `4 primary`
nodes, the `gRPC` ports are the following:
* `primary_0`: 8000
* `primary_1`: 8001
* `primary_2`: 8002
* `primary_3`: 8003

### Folder structure

Under this folder someone will find the following
```
├── Dockerfile
├── README.md
├── authorities
│   ├── authority-0
│   │   └── key.json
│   ├── authority-1
│   │   └── key.json
│   ├── authority-2
│   │   └── key.json
│   ├── authority-3
│   │   └── key.json
│   ├── committee.json
│   └── parameters.json
├── cluster-generator.py
├── docker-compose.yml
└── entry.sh
```

Under the `authorities` folder will be found the independent configuration
folder for each authority node (it is reminded that each `authority` is 
constituted from one `primary` node and several `worker` nodes).

The `key.json` file contains the private `key` for the corresponding node which
is associated to this node only.

The [parameters.json](authorities/parameters.json) file is shared across all the nodes and contains
the core parameters for a node.

The [committee.json](authorities/committee.json) file is shared across all the nodes and contains
the information about the authorities (primary & worker nodes), like the public keys, addresses and
ports available etc.

### Docker-compose configuration

The following environment variables are available to be used for each service on the
docker-compose.yml file configuration:
* `NODE_TYPE` with values `primary|worker` . Defines the node type to bootstrap
* `AUTHORITY_ID` with decimal numbers, for current setup available values `0..3`. Defines the
id of the authority that the node/service corresponds to. Basically this defines which
configuration to use under the `authorities` folder.
* `LOG_LEVEL` the level of logging for the node defined as number of `v` parameters (e.x `-vvv`). The following
levels are defined according to the number of "v"s provided: `0 | 1 => "error", 2 => "warn", 3 => "info", 
4 => "debug", 5 => "trace"`.
* `CONSENSUS_DISABLED`, this value disables consensus (`Tusk`) for a primary node and enables the
`gRPC` server. The value that should be passed is `--consensus-disabled`
* `WORKER_ID` the id, as integer, for service when it runs as a worker

On the [docker-compose.yml](docker-compose.yml) file will be found the configuration
of the deployment for each node. For a 