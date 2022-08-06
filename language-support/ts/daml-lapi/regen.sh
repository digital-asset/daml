#!/usr/bin/env bash

set -eou pipefail

SDK_VERSION=2.4.0-snapshot.20220801.10312.0.d2c7be9d
rm -rf com

PROTO_DIR=$(mktemp -d)
trap "rm -rf $PROTO_DIR" EXIT

cp -r ../../../ledger-api/grpc-definitions/com $PROTO_DIR/com
cp -r ../../../3rdparty/protobuf/google $PROTO_DIR/google

PROTOS="com/daml/ledger/api/v1/value.proto"

# We only generate sources for the subset we use.
protoc -I $PROTO_DIR $PROTOS \
       --js_out=import_style=commonjs:. \
       --grpc-web_out=import_style=typescript,mode=grpcwebtext:.
