# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def migration_test(name, versions, **kwargs):
    native.sh_test(
        name = name,
        srcs = ["//sandbox-migration:test.sh"],
        deps = ["@bazel_tools//tools/bash/runfiles"],
        data = [
            "//sandbox-migration:sandbox-migration-runner",
            "//sandbox-migration:migration-model.dar",
            "//sandbox-migration:migration-step",
        ] + ["@daml-sdk-{}//:daml".format(ver) for ver in versions],
        args = versions,
        **kwargs
    )
