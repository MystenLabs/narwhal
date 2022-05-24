#!/bin/bash

# Capture stack trace
export RUST_BACKTRACE=1

# Environment variables to use on the script
NODE_BIN="./bin/node"
KEYS_PATH="/authorities/authority-$AUTHORITY_ID/key.json"
COMMITTEE_PATH="/authorities/committee.json"
PARAMETERS_PATH="/authorities/parameters.json"

if [[ "$CLEANUP_DISABLED" = "true" ]]; then
  echo "Will not clean up existing directories..."
else
  if [[ "$NODE_TYPE" = "primary" ]]; then
    # Clean up only the primary node's data
    rm -r "/authorities/authority-$AUTHORITY_ID/db-primary"
    rm -r "/authorities/authority-$AUTHORITY_ID/logs/log-primary.txt"
  elif [[ "$NODE_TYPE" = "worker" ]]; then
    # Clean up only the specific worker's node data
    rm -r "/authorities/authority-$AUTHORITY_ID/db-worker-${WORKER_ID}"
    rm -r "/authorities/authority-$AUTHORITY_ID/logs/log-worker-${WORKER_ID}.txt"
  fi
fi

# If this is a primary node, then run as primary
if [[ "$NODE_TYPE" = "primary" ]]; then
  echo "Bootstrapping primary node"

  $NODE_BIN $LOG_LEVEL run \
  --keys $KEYS_PATH \
  --committee $COMMITTEE_PATH \
  --store "/authorities/authority-$AUTHORITY_ID/db-primary" \
  --parameters $PARAMETERS_PATH \
  primary $CONSENSUS_DISABLED >> "/home/logs/log-primary.txt" 2>&1
elif [[ "$NODE_TYPE" = "worker" ]]; then
  echo "Bootstrapping new worker node with id $WORKER_ID"

  $NODE_BIN $LOG_LEVEL run \
  --keys $KEYS_PATH \
  --committee $COMMITTEE_PATH \
  --store "/authorities/authority-$AUTHORITY_ID/db-worker-$WORKER_ID" \
  --parameters $PARAMETERS_PATH \
  worker --id $WORKER_ID >> "/home/logs/log-worker-$WORKER_ID.txt" 2>&1
else
  echo "Unknown provided value for parameter: NODE_TYPE=$NODE_TYPE"
  exit 1
fi