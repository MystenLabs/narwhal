#!/bin/bash
# Copyright (c) 2022, Mysten Labs, Inc.
# SPDX-License-Identifier: Apache-2.0

# This script attempts to update the Narwhal pointer in Sui
# It is expected to fail in cases 
set -e
set -eo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOPLEVEL="${DIR}/../"
GREP=${GREP:=grep}

# Crutch for old bash versions
# Very minimal readarray implementation using read. 
readarray() {
	while IFS= read -r var; do
        MAPFILE+=("$var")
	done
}


# check for the presence of needed executables:
# - we use GNU grep in perl re mode
# - we use cargo-hakari
function check_gnu_grep() {
	GNUSTRING=$($GREP --version|head -n1| grep 'GNU grep')
	if [[ -z $GNUSTRING ]];
	then 
		echo "Could not find GNU grep. This requires GNU grep 3.7 with PCRE expressions"; exit 1
	else 
		return 0
	fi
}

function check_cargo_hakari() {
	cargo hakari --version > /dev/null 2>&1
	if [[ $? -ne 0 ]]; then
		echo "Could not find cargo hakari. Please install"; exit 1		
	else
		return 0
	fi
}


function latest_fc_revision() {
	FC_CHECKOUT=$(mktemp -d)
	cd "$FC_CHECKOUT"
	git clone --depth 1 https://github.com/mystenlabs/fastcrypto
	cd fastcrypto
	git rev-parse HEAD
}

function current_fc_revision() {
	cd "$TOPLEVEL"
	readarray -t <<< "$(find ./ -iname '*.toml' -exec $GREP -oPe 'git = "https://github.com/MystenLabs/fastcrypto", *rev *= *\"\K[0-9a-fA-F]+' '{}' \;)"
	watermark=${MAPFILE[0]}
	for i in "${MAPFILE[@]}"; do
	    if [[ "$watermark" != "$i" ]]; then
        	not_equal=true
	        break
	    fi
	done

	[[ -n "$not_equal" ]] && echo "Different values found for the current NW revision in Sui, aborting" && exit 1
	echo "$watermark"
}

# Check for tooling
check_gnu_grep
check_cargo_hakari

# Debug prints
CURRENT_NW=$(current_fc_revision)
LATEST_NW=$(latest_fc_revision)
if [[ "$CURRENT_NW" != "$LATEST_NW" ]]; then
	echo "About to replace $CURRENT_NW with $LATEST_NW as the Fastcrypto pointer in Narwhal"
else
	exit 0
fi

# Edit the source & run hakari
find ./ -iname "*.toml"  -execdir sed -i '' -re "s/$CURRENT_NW/$LATEST_NW/" '{}' \;
cargo hakari generate
