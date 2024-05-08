#!/bin/bash
# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

￼
set -e

bazel build canton:community_app_deploy.jar

cd upgrade-issue/repro
java -jar ../../bazel-bin/canton/community_app_deploy.jar daemon -c canton.config --auto-connect-local --debug 

