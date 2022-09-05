#!/usr/bin/env bash
# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
  
cd ~/daml/infra/macos/2-common-box

GUEST_NAME=$(HOSTNAME) vagrant up

vagrant package --output ~/images/initialized-$(date +%Y%m%d).box

vagrant destroy -f

cd ~
./copyfile.sh images/initialized-$(date +%Y%m%d).box

