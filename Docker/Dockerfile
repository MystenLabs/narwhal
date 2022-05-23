FROM lukemathwalker/cargo-chef:latest-rust-1.60.0-slim as builder

# Install basic dependencies
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y --no-install-recommends \
    tzdata \
    git \
    ca-certificates \
    curl \
    build-essential \
    libssl-dev \
    pkg-config \
    clang \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Install the fmt
RUN rustup component add rustfmt

# Set the workdir to the build folder
WORKDIR "$WORKDIR/build/narwhal"

# Copy all the files in the workdir excluding everything
# from the .dockerignore
COPY . .

# Build the binary named "node"
RUN cargo build --bin node

# Creat another layer so we can re-use caching
FROM ubuntu:focal

WORKDIR "$WORKDIR/build"

RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl1.1 iputils-ping telnet curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the Narwhal node binary to bin folder
COPY --from=builder /build/narwhal/target/debug/node bin/

# Copy the entry point file
COPY Docker/entry.sh ./

# Now add the entry point
CMD ./entry.sh