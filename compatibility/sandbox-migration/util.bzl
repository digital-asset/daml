# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_linux")
load("//bazel_tools:versions.bzl", "versions")

def runfiles(ver):
    return ["@daml-sdk-{}//:daml".format(ver)] + (["@daml-sdk-{}//:sandbox-on-x".format(ver)] if versions.is_at_least("2.0.0", ver) else [])

def migration_test(name, versions, tags, quick_tags, **kwargs):
    native.sh_test(
        name = name,
        srcs = ["//sandbox-migration:test.sh"],
        deps = ["@bazel_tools//tools/bash/runfiles"],
        data = [
            "//sandbox-migration:sandbox-migration-runner",
            "//sandbox-migration:migration-model.dar",
            "//sandbox-migration:migration-step",
        ] + [dep for ver in versions for dep in runfiles(ver)],
        args = versions,
        tags = tags + quick_tags,
        **kwargs
    )

    # We keep both since some ledgers switched to the append only schema before it was the default.
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
