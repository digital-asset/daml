#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

cd daml/infra/macos/1-create-box

sudo macinbox --box-format vmware_desktop --disk 250 --memory 57344 --cpu 10 --user-script user-script.sh

