# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def nodejs_proto_compile(name):
    native.genrule(
        name = name,
        srcs = [
            "//ledger-api/grpc-definitions:ledger-api-protos",
            "//daml-lf/archive:daml-lf-archive-srcs",
        ],
        outs = [
            "src/grpc/health/v1/health_service_grpc_pb.js",
            "src/grpc/health/v1/health_service_grpc_pb.d.ts",
            "src/grpc/health/v1/health_service_pb.js",
            "src/grpc/health/v1/health_service_pb.d.ts",
            "src/da/daml_lf_1_pb.d.ts",
            "src/da/daml_lf_1_pb.js",
            "src/da/daml_lf_dev_pb.d.ts",
            "src/da/daml_lf_0_pb.d.ts",
            "src/da/daml_lf_pb.js",
            "src/da/daml_lf_pb.d.ts",
            "src/da/daml_lf_dev_pb.js",
            "src/da/daml_lf_0_pb.js",
            "src/google/rpc/error_details_pb.js",
            "src/google/rpc/status_pb.js",
            "src/google/rpc/code_pb.js",
            "src/google/rpc/status_pb.d.ts",
            "src/google/rpc/error_details_pb.d.ts",
            "src/google/rpc/code_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/command_service_grpc_pb.js",
            "src/com/digitalasset/ledger/api/v1/command_completion_service_grpc_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/ledger_offset_pb.js",
            "src/com/digitalasset/ledger/api/v1/completion_pb.js",
            "src/com/digitalasset/ledger/api/v1/transaction_filter_pb.js",
            "src/com/digitalasset/ledger/api/v1/ledger_identity_service_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/command_submission_service_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/ledger_configuration_service_grpc_pb.js",
            "src/com/digitalasset/ledger/api/v1/commands_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/ledger_offset_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/commands_pb.js",
            "src/com/digitalasset/ledger/api/v1/package_service_grpc_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/ledger_configuration_service_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/command_service_pb.js",
            "src/com/digitalasset/ledger/api/v1/command_service_grpc_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/command_completion_service_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/transaction_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/package_service_pb.js",
            "src/com/digitalasset/ledger/api/v1/package_service_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/transaction_service_pb.js",
            "src/com/digitalasset/ledger/api/v1/value_pb.js",
            "src/com/digitalasset/ledger/api/v1/transaction_service_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/command_submission_service_grpc_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/active_contracts_service_grpc_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/ledger_identity_service_pb.js",
            "src/com/digitalasset/ledger/api/v1/trace_context_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/trace_context_pb.js",
            "src/com/digitalasset/ledger/api/v1/transaction_pb.js",
            "src/com/digitalasset/ledger/api/v1/command_completion_service_grpc_pb.js",
            "src/com/digitalasset/ledger/api/v1/command_service_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/ledger_identity_service_grpc_pb.js",
            "src/com/digitalasset/ledger/api/v1/command_submission_service_grpc_pb.js",
            "src/com/digitalasset/ledger/api/v1/event_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/command_completion_service_pb.js",
            "src/com/digitalasset/ledger/api/v1/transaction_filter_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/transaction_service_grpc_pb.js",
            "src/com/digitalasset/ledger/api/v1/active_contracts_service_grpc_pb.js",
            "src/com/digitalasset/ledger/api/v1/active_contracts_service_pb.js",
            "src/com/digitalasset/ledger/api/v1/testing/reset_service_grpc_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/testing/time_service_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/testing/reset_service_grpc_pb.js",
            "src/com/digitalasset/ledger/api/v1/testing/reset_service_pb.js",
            "src/com/digitalasset/ledger/api/v1/testing/reset_service_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/testing/time_service_grpc_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/testing/time_service_pb.js",
            "src/com/digitalasset/ledger/api/v1/testing/time_service_grpc_pb.js",
            "src/com/digitalasset/ledger/api/v1/ledger_configuration_service_grpc_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/completion_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/package_service_grpc_pb.js",
            "src/com/digitalasset/ledger/api/v1/value_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/command_submission_service_pb.js",
            "src/com/digitalasset/ledger/api/v1/active_contracts_service_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/event_pb.js",
            "src/com/digitalasset/ledger/api/v1/transaction_service_grpc_pb.d.ts",
            "src/com/digitalasset/ledger/api/v1/ledger_configuration_service_pb.js",
            "src/com/digitalasset/ledger/api/v1/ledger_identity_service_grpc_pb.d.ts",
        ],
        cmd = """
            set -e
            export LEDGER_API="$(GENDIR)/ledger-api/grpc-definitions"
            export DAML_LF_ARCHIVE="daml-lf/archive"
            export IN=proto
            export OUT=src
            export PATH=$$PATH:external/nodejs/bin/
            mkdir -p "$$IN"
            mkdir -p "$$OUT"
            tar xzf $(location //ledger-api/grpc-definitions:ledger-api-protos) -C "$$IN"
            $(execpath //language-support/js:grpc_tools_node_protoc) \
              --plugin="protoc-gen-grpc=external/npm/node_modules/grpc-tools/bin/grpc_node_plugin" \
              --js_out="import_style=commonjs,binary:$$OUT" \
              --proto_path="$$IN" \
              --proto_path="$$LEDGER_API" \
              --grpc_out="$$OUT" \
              "$$IN"/com/digitalasset/ledger/api/v1/*.proto \
              "$$IN"/com/digitalasset/ledger/api/v1/testing/*.proto \
              "$$IN"/google/rpc/*.proto \
              "$$IN"/grpc/health/v1/*.proto
            $(execpath //language-support/js:grpc_tools_node_protoc) \
              --plugin="protoc-gen-ts=external/npm/node_modules/grpc_tools_node_protoc_ts/bin/protoc-gen-ts" \
              --proto_path="$$IN" \
              --proto_path="$$LEDGER_API" \
              --ts_out="$$OUT" \
              "$$IN"/com/digitalasset/ledger/api/v1/*.proto \
              "$$IN"/com/digitalasset/ledger/api/v1/testing/*.proto \
              "$$IN"/google/rpc/*.proto \
              "$$IN"/grpc/health/v1/*.proto
            $(execpath //language-support/js:grpc_tools_node_protoc) \
              --plugin="protoc-gen-grpc=external/npm/node_modules/grpc-tools/bin/grpc_node_plugin" \
              --js_out="import_style=commonjs,binary:$$OUT" \
              --proto_path="$$DAML_LF_ARCHIVE" \
              --grpc_out="$$OUT" \
              "$$DAML_LF_ARCHIVE"/da/*.proto
            $(execpath //language-support/js:grpc_tools_node_protoc) \
              --plugin="protoc-gen-ts=external/npm/node_modules/grpc_tools_node_protoc_ts/bin/protoc-gen-ts" \
              --proto_path="$$DAML_LF_ARCHIVE" \
              --ts_out="$$OUT" \
              "$$DAML_LF_ARCHIVE"/da/*.proto
            cp -R "$$OUT" $(@D)
        """,
        tools = [
            "//language-support/js:grpc_tools_node_protoc",
            "@nodejs//:node",
            "@npm//grpc-tools",
            "@npm//grpc_tools_node_protoc_ts",
        ],
    )