#!/bin/bash

# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# architecture-handbook-entry-begin: GenTestCerts

# include certs-common.sh from config/tls
. "$(dirname "${BASH_SOURCE[0]}")/certs-common.sh"

# create root certificate such that we can issue self-signed certs
create_key "root-ca"
create_certificate "root-ca" "/O=TESTING/OU=ROOT CA/emailAddress=canton@digitalasset.com"
#print_certificate "root-ca"

# create public api certificate
create_key "public-api"
create_csr "public-api" "/O=TESTING/OU=DOMAIN/CN=localhost/emailAddress=canton@digitalasset.com" "DNS:localhost,IP:127.0.0.1"
sign_csr "public-api" "root-ca"
print_certificate "public-api"

# create participant ledger-api certificate
create_key "ledger-api"
create_csr "ledger-api" "/O=TESTING/OU=PARTICIPANT/CN=localhost/emailAddress=canton@digitalasset.com" "DNS:localhost,IP:127.0.0.1"
sign_csr "ledger-api" "root-ca"

# create participant admin-api certificate
create_key "admin-api"
create_csr "admin-api" "/O=TESTING/OU=PARTICIPANT ADMIN/CN=localhost/emailAddress=canton@digitalasset.com" "DNS:localhost,IP:127.0.0.1"
sign_csr "admin-api" "root-ca"

# create participant client key and certificate
create_key "admin-client"
create_csr "admin-client" "/O=TESTING/OU=PARTICIPANT ADMIN CLIENT/CN=localhost/emailAddress=canton@digitalasset.com"
sign_csr "admin-client" "root-ca"
print_certificate "admin-client"
# architecture-handbook-entry-end: GenTestCerts

