#!/bin/bash

# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -e -o pipefail

# Setup paths

# If we're running from the repo, use the protobuf from the repo
if git rev-parse --is-inside-work-tree &>/dev/null || false; then
    ROOT_PATH=$(git rev-parse --show-toplevel)
    COMMUNITY_PROTO_PATH=$ROOT_PATH/community/base/src/main/protobuf
    LEDGER_API_PROTO_PATH=$ROOT_PATH/community/ledger-api/src/main/protobuf
    ADMIN_API_PROTO_PATH=$ROOT_PATH/community/admin-api/src/main/protobuf
else
    # Otherwise assume we're running from the release artifact, in which case the protobuf folder is a few levels above
    ROOT_PATH=../../../protobuf
    COMMUNITY_PROTO_PATH=$ROOT_PATH/community
    LEDGER_API_PROTO_PATH=$ROOT_PATH/ledger-api
    ADMIN_API_PROTO_PATH=$ROOT_PATH/admin-api
fi

COMMUNITY_CANTON_PROTO_PATH=$COMMUNITY_PROTO_PATH/com/digitalasset/canton
ADMIN_API_CANTON_PROTO_PATH=$ADMIN_API_PROTO_PATH/com/digitalasset/canton
PROTOCOL_PROTO_PATH=$COMMUNITY_CANTON_PROTO_PATH/protocol/v30
CRYPTO_PROTO_PATH=$COMMUNITY_CANTON_PROTO_PATH/crypto/v30
TOPOLOGY_ADMIN_PROTO_PATH=$COMMUNITY_CANTON_PROTO_PATH/topology/admin/v30
LEDGER_API_V2_PATH=$LEDGER_API_PROTO_PATH/com/daml/ledger/api/v2

# Simple utility method
download_if_not_exists() {
  local url=$1
  local file_path=$2

  if [ ! -f "$file_path" ]; then
    echo "Downloading $file_path"
    mkdir -p "$(dirname "$file_path")"
    curl -s "$url" -o "$file_path"
  fi
}


# Get the daml version so we can download the correct value.proto from the repo
DAML_COMMIT_FILE="daml_commit"

# In the release artifact there will be a file with the commit hash in it
if [[ -f "$DAML_COMMIT_FILE" ]]; then
    echo "Using Daml commit $DAML_COMMIT read from $DAML_COMMIT_FILE"
    DAML_COMMIT=$(<"$DAML_COMMIT_FILE")
else
    source "$ROOT_PATH/scripts/daml_version_util.sh"
    # When run from the repo, we extract the commit from DamlVersions.scala
    DAML_COMMIT=$(cd "$ROOT_PATH" && echo $(get_daml_commit))
    echo "Using Daml commit $DAML_COMMIT extracted from DamlVersions.scala"
fi

# Proto for LF Values, not packaged directly in canton so we need to get it from the daml repo
download_if_not_exists "https://raw.githubusercontent.com/digital-asset/daml/$DAML_COMMIT/sdk/daml-lf/ledger-api-value/src/main/protobuf/com/daml/ledger/api/v2/value.proto" "com/daml/ledger/api/v2/value.proto"
# Google and scalapb common protos also not directly packaged in canton
download_if_not_exists "https://raw.githubusercontent.com/googleapis/googleapis/3597f7db2191c00b100400991ef96e52d62f5841/google/rpc/status.proto" "google/rpc/status.proto"
download_if_not_exists "https://raw.githubusercontent.com/protocolbuffers/protobuf/407aa2d9319f5db12964540810b446fecc22d419/src/google/protobuf/empty.proto" "google/protobuf/empty.proto"
download_if_not_exists "https://raw.githubusercontent.com/scalapb/ScalaPB/6291978a7ca8b48bd69cc98aa04cb28bc18a44a9/protobuf/scalapb/scalapb.proto" "scalapb/scalapb.proto"
download_if_not_exists "https://raw.githubusercontent.com/googleapis/googleapis/9415ba048aa587b1b2df2b96fc00aa009c831597/google/rpc/error_details.proto" "google/rpc/error_details.proto"

# Generate python code for protobuf messages
generate_grpc_code() {
  local proto_path=$1
  local proto_file=$2
  local grpc_out_flag=${3:-false}

  echo "Generating python code for $proto_file"

  if [ "$grpc_out_flag" = true ]; then
    python -m grpc_tools.protoc -I"$proto_path" -I. --python_out=. --pyi_out=. --grpc_python_out=. "$proto_file"
  else
    python -m grpc_tools.protoc -I"$proto_path" -I. --python_out=. --pyi_out=. "$proto_file"
  fi
}

# Generate python code for protobuf messages + gRPC services
generate_grpc_service() {
  local proto_path=$1
  local proto_file=$2

  generate_grpc_code "$proto_path" "$proto_file" true
}

# Generate gRPC client code and proto classes for python
echo "Generating python code from protobuf definitions"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/commands.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/completion.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/event.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/offset_checkpoint.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/reassignment.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/trace_context.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/transaction.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/transaction_filter.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/topology_transaction.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/package_reference.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/interactive/transaction/v1/interactive_submission_data.proto"
generate_grpc_code "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/interactive/interactive_submission_common_data.proto"
generate_grpc_code "$COMMUNITY_PROTO_PATH" "$PROTOCOL_PROTO_PATH/topology.proto"
generate_grpc_code "$COMMUNITY_PROTO_PATH" "$TOPOLOGY_ADMIN_PROTO_PATH/common.proto"
generate_grpc_code "$COMMUNITY_PROTO_PATH" "$PROTOCOL_PROTO_PATH/synchronizer_parameters.proto"
generate_grpc_code "$COMMUNITY_PROTO_PATH" "$COMMUNITY_CANTON_PROTO_PATH/version/v1/untyped_versioned_message.proto"
generate_grpc_code "$COMMUNITY_PROTO_PATH" "$PROTOCOL_PROTO_PATH/traffic_control_parameters.proto"
generate_grpc_code "$COMMUNITY_PROTO_PATH" "$PROTOCOL_PROTO_PATH/sequencing_parameters.proto"
generate_grpc_code "$COMMUNITY_PROTO_PATH" "$CRYPTO_PROTO_PATH/crypto.proto"
generate_grpc_service "$ADMIN_API_PROTO_PATH" "$ADMIN_API_CANTON_PROTO_PATH/admin/health/v30/status_service.proto"
generate_grpc_code "." "scalapb/scalapb.proto"
generate_grpc_code "." "google/rpc/status.proto"
generate_grpc_code "." "google/protobuf/empty.proto"
generate_grpc_code "." "google/rpc/error_details.proto"
generate_grpc_code "." "com/daml/ledger/api/v2/value.proto"

# gRPC services
generate_grpc_service "$COMMUNITY_PROTO_PATH" "$TOPOLOGY_ADMIN_PROTO_PATH/topology_manager_write_service.proto"
generate_grpc_service "$COMMUNITY_PROTO_PATH" "$TOPOLOGY_ADMIN_PROTO_PATH/topology_manager_read_service.proto"
generate_grpc_service "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/interactive/interactive_submission_service.proto"
generate_grpc_service "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/update_service.proto"
generate_grpc_service "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/command_completion_service.proto"
generate_grpc_service "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/state_service.proto"
generate_grpc_service "$LEDGER_API_PROTO_PATH" "$LEDGER_API_V2_PATH/event_query_service.proto"
generate_grpc_service "$ADMIN_API_PROTO_PATH" "$ADMIN_API_CANTON_PROTO_PATH/admin/participant/v30/participant_status_service.proto"

echo "Done"