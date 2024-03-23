#!/bin/bash
# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

cd /Users/builder/daml/infra/macos/3-running-box

export latestFile=`find ~/images -name 'init*' | head -1`
echo $latestFile

vagrant box add azure-ci-node $latestFile --force

# Assumes physical host has one VSTS node and take physical host name
GUEST_NAME=$(HOSTNAME) VSTS_TOKEN=<VSTS-PAT-token-with-agent-right> vagrant up
