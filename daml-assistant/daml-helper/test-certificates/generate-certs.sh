#!/bin/sh
# Copyright (c) 2020 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


# Note (MK) We check in the certs to avoid incurring a dependency on openssl
# so this script is not used on CI

# Generate CA key and crt
openssl genrsa -out ca.key 4096
openssl req -new -x509 -key ca.key -out ca.crt -subj '/CN=0.0.0.0.ca' -days 3650

# Generate server key, csr and crt
openssl genrsa -out server.key 4096
openssl pkey -in server.key -out server.pem
openssl req -new -key server.key -out server.csr -subj '/CN=0.0.0.0.server'
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -extfile openssl-extensions.cnf -extensions req_ext -days 3650

# Generate client key, csr and crt
openssl genrsa -out client.key 4096
openssl pkey -in client.key -out client.pem
openssl req -new -key client.key -out client.csr -subj '/CN=0.0.0.0.client'
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -extfile openssl-extensions.cnf -extensions req_ext -days 3650

