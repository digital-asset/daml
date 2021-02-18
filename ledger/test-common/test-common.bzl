# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
load("//daml-lf/archive:archive.bzl", "mangle_for_java")
load(
    "//daml-lf/language:daml-lf.bzl",
    "lf_dev_version",
    "lf_latest_version",
    "lf_preview_version",
    "lf_stable_version",
)

test_common_configurations = {
    "stable": lf_stable_version,
    "latest": lf_latest_version,
    "dev": lf_dev_version,
}

test_common_lf_targets = {
    lf_target: mangle_for_java(lf_target)
    for lf_target in test_common_configurations.values()
}
