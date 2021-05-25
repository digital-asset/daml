# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def migration_test(name, versions, tags, **kwargs):
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
        tags = tags,
        **kwargs
    )

    # TODO (MK) Remove once the append-only schema is the default.
    native.sh_test(
        name = "{}-append-only".format(name),
        srcs = ["//sandbox-migration:test.sh"],
        deps = ["@bazel_tools//tools/bash/runfiles"],
        tags = tags,
        data = [
            "//sandbox-migration:sandbox-migration-runner",
            "//sandbox-migration:migration-model.dar",
            "//sandbox-migration:migration-step",
        ] + ["@daml-sdk-{}//:daml".format(ver) for ver in versions],
        args = ["--append-only"] + versions,
        **kwargs
    )
