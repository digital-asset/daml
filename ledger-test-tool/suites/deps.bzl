# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("//daml-lf/language:daml-lf.bzl", "version_in")

def _has_model_tests(lf_version):
    return version_in(
        lf_version,
        v1_minor_version_range = ("15", "dev"),
        v2_minor_version_range = ("0", "dev"),
    )

def deps(lf_version):
    carbon_tests = [
        "//test-common:carbonv1-tests-%s.java-codegen" % lf_version,
        "//test-common:carbonv2-tests-%s.java-codegen" % lf_version,
        "//test-common:carbonv3-tests-%s.java-codegen" % lf_version,
    ]
    additional_tests = carbon_tests if _has_model_tests(lf_version) else []
    return [
        "//canton:bindings-java",
        "//daml-lf/data",
        "//daml-lf/transaction",
        "//canton:ledger_api_proto_scala",
        "//ledger/error",
        "//ledger/ledger-api-errors",
        "//ledger/ledger-api-common",
        "//ledger-test-tool/infrastructure:infrastructure-%s" % lf_version,
        "//libs-scala/ledger-resources",
        "//libs-scala/test-evidence/tag:test-evidence-tag",
        "//test-common:dar-files-%s-lib" % lf_version,
        "//test-common:model-tests-%s.java-codegen" % lf_version,
        "//test-common:package_management-tests-%s.java-codegen" % lf_version,
        "//test-common:semantic-tests-%s.java-codegen" % lf_version,
        "//libs-scala/contextualized-logging",
        "//libs-scala/grpc-utils",
        "//libs-scala/resources",
        "//libs-scala/resources-pekko",
        "//libs-scala/resources-grpc",
        "//libs-scala/timer-utils",
        "@maven//:com_google_api_grpc_proto_google_common_protos",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_netty_netty_handler",
        "@maven//:org_slf4j_slf4j_api",
    ] + additional_tests
