# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


#!/bin/bash

set -e

# TODO (SM): properly wrap this script with help messages, error and sanity
# checks.

# dad-add-source-dep SOURCE TARGET
#
# Adds a source dependency link from SOURCE to TARGET
# dependencies are specified as <kind>/<package>

SRC=$1
TGT=$2

LINK_DIR=$SRC/$(dirname $TGT)

echo "mkdir -p $TGT"
echo "mkdir -p $LINK_DIR"
echo "ln -i -r -s -t $LINK_DIR $TGT"

mkdir -p $TGT
mkdir -p $LINK_DIR
ln -f -r -s -t $LINK_DIR $TGT




