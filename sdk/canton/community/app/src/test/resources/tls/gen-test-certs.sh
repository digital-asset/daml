#!/bin/bash

# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

source "../../../../../../community/app/src/pack/config/tls/gen-test-certs.sh"

# create a couple of certificates for our sequencer with different SANs to test that the
# correct service authority is being used and verified when load balancing between multiple sequencers
create_key "sequencer2-localhost"
create_csr "sequencer2-localhost" "/O=TESTING/OU=SEQUENCER2/CN=localhost/emailAddress=canton@digitalasset.com" "DNS:localhost"
sign_csr "sequencer2-localhost" "root-ca"

create_key "sequencer3-127.0.0.1"
create_csr "sequencer3-127.0.0.1" "/O=TESTING/OU=SEQUENCER3/CN=localhost/emailAddress=canton@digitalasset.com" "IP:127.0.0.1"
sign_csr "sequencer3-127.0.0.1" "root-ca"

# create self-signed certificate (used to test that mutual client auth works by refusing to connect with not-authorized certificate)
create_key "some"
create_certificate "some" "/O=TESTING/OU=DISCONNECTED CERT/emailAddress=canton@digitalasset.com"
