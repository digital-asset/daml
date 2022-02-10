# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def deps(lf_version):
    return [
        "//daml-lf/data",
        "//language-support/scala/bindings",
        "//ledger/error",
        "//ledger/ledger-api-common",
        "//ledger/ledger-api-tests/infrastructure:infrastructure-%s" % lf_version,
        "//ledger/ledger-resources",
        "//ledger/test-common:dar-files-%s-lib" % lf_version,
        "//ledger/test-common:model-tests-%s.scala" % lf_version,
        "//ledger/test-common:package_management-tests-%s.scala" % lf_version,
        "//ledger/test-common:semantic-tests-%s.scala" % lf_version,
        "//ledger/test-common:test-common-%s" % lf_version,
        "//libs-scala/contextualized-logging",
        "//libs-scala/grpc-utils",
        "//libs-scala/resources",
        "//libs-scala/resources-akka",
        "//libs-scala/resources-grpc",
        "//libs-scala/timer-utils",
        "@maven//:com_google_api_grpc_proto_google_common_protos",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_netty_netty_handler",
        "@maven//:org_slf4j_slf4j_api",
    ]
