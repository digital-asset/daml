#!/bin/bash

# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -eoux pipefail

mkdir test-certificates
cd test-certificates

mkdir newcerts

ABSOLUTE_OUT=$(pwd)
DAYS=7300

# Generate SSL config from the template
cat ../openssl-template.cnf | sed -e "s;<ROOTDIR>;$ABSOLUTE_OUT;g" > openssl.cnf

# Setup directories
touch -- index.txt
echo 1000 > serial

function create_key {
  local name=$1
  openssl genrsa -out "${name}.key" 4096
}

function create_pem {
  local name=$1
  openssl pkey -in "${name}.key" -out "${name}.pem"
}

function create_key_and_pem {
  local name=$1
  create_key "$name"
  create_pem "$name"
}

function create_certificate {
  local conf=$1
  local name=$2
  local subj=$3
  openssl req -config "$conf" \
    -key "${name}.key" \
    -new -x509 -days $DAYS -sha256 -extensions v3_ca \
    -subj "$subj" \
    -out "${name}.crt"
}

function create_csr {
  local name=$1
  local subj=$2
  local san=$3
  local conf="${4:-""}"

  args=(
    -subj "$subj"
    -addext "subjectAltName=${san}"
    -key "${name}.pem"
    -new
    -out "${name}.csr"
  )
  if [[ -n "$conf" ]]; then
    args+=(-config "$conf")
    args+=(-sha256)
  fi

  openssl req "${args[@]}"
}

function print_certificate {
  local name=$1
  openssl x509 -in "${name}.crt" -text -noout
}

# Generate Root CA private key
create_key "ca"
chmod 400 "ca.key"
# Create Root Certificate (self-signed)
create_certificate "openssl.cnf" "ca" "/CN=0.0.0.0.ca"
print_certificate "ca"

# Generate server key, csr and crt
create_key_and_pem "server"
create_csr "server" "/CN=0.0.0.0.server" "DNS:localhost, IP:127.0.0.1" "openssl.cnf"
openssl ca -batch -config "openssl.cnf" \
    -extensions server_cert -days $DAYS -notext -md sha256 -days ${DAYS}  \
    -in "server.csr" \
    -out "server.crt"
chmod 444 "server.crt"

# Encrypt server's key and dump encryption parameters to a JSON file.
# NOTE: Encryption details used to encrypt the private must be kept in sync with `test-common/files/server-pem-decryption-parameters.json`
openssl enc -aes-128-cbc -base64 \
    -in "server.pem" \
    -out "server.pem.enc" \
    -K 0034567890abcdef1234567890abcdef \
    -iv 1134567890abcdef1234567890abcdef

# Generate Client CA private key
create_key_and_pem "client"
openssl req -new -key "client.pem" \
    -subj "/CN=0.0.0.0.client" \
    -addext "subjectAltName = DNS:localhost, IP:127.0.0.1" \
    -out "client.csr"
# Sign Client Cert
openssl ca -batch -config "openssl.cnf" \
    -extensions usr_cert -notext -md sha256 -days ${DAYS} \
    -in "client.csr" \
    -out "client.crt"
# Validate cert is correct
openssl verify -CAfile "ca.crt" "client.crt"

# Generate OCSP Server private key
openssl genrsa -out "ocsp.key.pem" 4096
# Sign OCSP Server certificate
openssl req -config "openssl.cnf" -new -sha256 \
    -subj "/CN=ocsp.127.0.0.1" \
    -key "ocsp.key.pem" \
    -out "ocsp.csr"

openssl ca -batch -config "openssl.cnf" \
    -extensions ocsp -days $DAYS -notext -md sha256 \
    -in "ocsp.csr" \
    -out "ocsp.crt"
# Validate extensions
openssl x509 -noout -text \
    -in "ocsp.crt"


# Generate Client-Revoked CA private key
openssl genpkey -out "client-revoked.key" -algorithm RSA -pkeyopt rsa_keygen_bits:2048
create_pem "client-revoked"
create_csr "client-revoked" "/CN=0.0.0.0.clientrevoked" "DNS:localhost, IP:127.0.0.1"
# Sign Client Cert
openssl ca -batch -config "openssl.cnf" \
    -extensions usr_cert -notext -md sha256 -days ${DAYS} \
    -in "client-revoked.csr" \
    -out "client-revoked.crt"
# Validate cert is correct
openssl verify -CAfile "ca.crt" "client-revoked.crt"
# Revoke
openssl ca -batch -config "openssl.cnf" -revoke "client-revoked.crt"


## Configure alternative CA for 'invalid certificate' scenarios
NEWCERTS_ALTERNATIVE_DIR=$ABSOLUTE_OUT/newcerts_alternative

# Generate SSL config from the template
cat ../openssl-alternative-template.cnf | sed -e "s;<ROOTDIR>;$ABSOLUTE_OUT;g" > openssl-alternative.cnf

# Setup directories
mkdir -- $NEWCERTS_ALTERNATIVE_DIR
touch -- index_alternative.txt
echo 1000 > serial_alternative

# Generate Root Alternative CA private key
create_key "ca_alternative"
chmod 400 ca_alternative.key
create_pem "ca_alternative"
create_certificate "openssl-alternative.cnf" "ca_alternative" "/CN=0.0.0.0.ca"
print_certificate "ca_alternative"
