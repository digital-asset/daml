#!/usr/bin/env bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
DIR="$(cd $(dirname $(readlink -f "$BASH_SOURCE[0]")); pwd)"
"$DIR/daml/daml" install "$DIR" --activate $@
