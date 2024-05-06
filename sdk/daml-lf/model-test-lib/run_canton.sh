#!/bin/bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


set -e

bazel build canton:community_app_deploy.jar
java -jar bazel-bin/canton/community_app_deploy.jar daemon -c daml-lf/model-test-lib/canton.conf --bootstrap daml-lf/model-test-lib/bootstrap.canton --debug --log-file-name=/tmp/canton.log
