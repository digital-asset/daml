#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# expect to be run from root of repo

t=tmp # fixed temp dir local to invoking dir, it'll do!
rm -rf $t

repo=/home/nic/daml #discover automtically from here
jar=$repo/bazel-bin/xbc/xbc.jar
class=xbc/ByHandScala$.class

mkdir $t
(cd $t; cat $jar | jar -x $class)
find $t -name *.class | xargs javap -p -v > xbc/bytecode.text
rm -rf $t

