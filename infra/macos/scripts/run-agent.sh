#!/bin/bash
# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

cd /Users/builder/daml/infra/macos/2-vagrant-files

# Assumes physical host has one VSTS node and take physical host name
GUEST_NAME=$(HOSTNAME) VSTS_TOKEN=<VSTS-PAT-token-with-agent-right> vagrant up
