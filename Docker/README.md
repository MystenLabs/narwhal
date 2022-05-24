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

The logs for each authority (primary & worker) can be found on the `logs` folder under the corresponding
authority folder. The `logs` folder will be created once the node is bootstrapped via docker-compose. 
For example, for the primary node of the authority-0, the logs will be found under
the folder [logs](authorities/authority-0/logs) and with the name `log-primary.txt`. To monitor the logging
of a node in real time you can just do something like:
```
tail -f authorities/authority-0/logs/log-primary.txt
```

By default, the development version of the Narwhal node will be compiled when the Docker image is being built.
To build the Docker image with the production version of it - which will contain all the Rust optimisations,
you can run the docker-compose command as:
```
docker-compose build --build-arg BUILD_MODE=--release

# and then run as

docker-compose up
```

**Note:** by default each authority's directory will be cleaned up between docker-compose runs when each node
bootstraps. To preserve those between runs please see the usage of the environment variable `CLEANUP_DISABLED` on
the [section](#docker-compose-configuration) bellow.

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

It has to be noted that the current docker-compose setup is mounting the [Docker/authorities](authorities)
folder to the service containers in order to share the folders & files in it. That allow us to experiment/change
configuration without having the need to rebuild the Docker image.

### Docker-compose configuration

The following environment variables are available to be used for each service on the
[docker-compose.yml](docker-compose.yml) file configuration:
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
* `CLEANUP_DISABLED` , when provided with value `true`, it will disable the clean up of the authority folder
from the database & log data. This is useful to preserve the state between multiple docker compose runs.

### Troubleshooting

#### 1. Compile Errors when building Docker image
If come across errors while the Docker image is being build, for example errors like:
```
error: could not compile `tonic`
#9 373.3 
#9 373.3 Caused by:
#9 373.4   process didn't exit successfully: `rustc --crate-name tonic --edition=2018
....
#9 398.4 The following warnings were emitted during compilation:
#9 398.4 
#9 398.4 warning: c++: fatal error: Killed signal terminated program cc1plus
#9 398.4 warning: compilation terminated.
```

it is possible that the Docker engine is running out of memory and there is no capacity to properly
compile the code. In this case please try to increase the available RAM at least to 2GB and retry.

#### 2. Mounts denied or cannot start service errors

If you try to spin up the nodes via docker-compose and you come across errors such as `mounts dened`
or `cannot start service`, please make sure that you allow Docker to share your host's [Docker/authorities](authorities)
folder with the containers. If you are using Docker Desktop you can find more information of how to do
that here: [mac](https://docs.docker.com/desktop/mac/#file-sharing), [linux](https://docs.docker.com/desktop/linux/#file-sharing),
[windows](https://docs.docker.com/desktop/windows/#file-sharing)
