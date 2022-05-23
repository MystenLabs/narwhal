#!/bin/bash

# Environment variables to use on the script
LOG_LEVEL="-vvv"
NODE_BIN="./bin/node"
KEYS_PATH="/authorities/authority-$AUTHORITY_ID/key.json"
COMMITTEE_PATH="/authorities/committee.json"
PARAMETERS_PATH="/authorities/parameters.json"

# If this is a primary node, then run as primary
if [[ "$NODE_TYPE" = "primary" ]]; then
  echo "Bootstrapping primary node"

  $NODE_BIN $LOG_LEVEL run \
  --keys $KEYS_PATH \
  --committee $COMMITTEE_PATH \
  --store "/authorities/authority-$AUTHORITY_ID/db-primary" \
  --parameters $PARAMETERS_PATH \
  primary $CONSENSUS_DISABLED
elif [[ "$NODE_TYPE" = "worker" ]]; then
  echo "Bootstrapping new worker node with id $WORKER_ID"

  $NODE_BIN $LOG_LEVEL run \
  --keys $KEYS_PATH \
  --committee $COMMITTEE_PATH \
  --store "/authorities/authority-$AUTHORITY_ID/db-worker" \
  --parameters $PARAMETERS_PATH \
  worker --id $WORKER_ID
else
  echo "Unknown provided value for parameter: NODE_TYPE=$NODE_TYPE"
  exit 1
fi