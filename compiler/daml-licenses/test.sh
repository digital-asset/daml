#!/usr/bin/env bash
# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# Test that licenses are up-to-date
#
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
set -ex
cd $DIR/licenses
./extract.py check
./extract-js.py check
