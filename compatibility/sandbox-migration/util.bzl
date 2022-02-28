# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_linux")
load("//bazel_tools:versions.bzl", "versions")

def runfiles(ver):
    return ["@daml-sdk-{}//:daml".format(ver)] + (["@daml-sdk-{}//:sandbox-on-x".format(ver)] if versions.is_at_least("2.0.0", ver) else [])

def oracle_versions(vers):
    return [v for v in vers if versions.is_at_least("1.18.0", v)]

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
        ] + [dep for ver in versions for dep in runfiles(ver)],
        args = ["--append-only"] + versions,
        **kwargs
    )

    # Oracle was introduced after the switch to the append-only schema, no need to run with --append-only.
    # Oracle tests only run on Linux by default, because we don't have docker on macOS or Windows CI nodes.
    native.sh_test(
        name = "{}-oracle".format(name),
        srcs = ["//sandbox-migration:test.sh"],
        deps = ["@bazel_tools//tools/bash/runfiles"],
        tags = tags + (["manual"] if not is_linux else []),
        data = [
            "//sandbox-migration:sandbox-migration-runner",
            "//sandbox-migration:migration-model.dar",
            "//sandbox-migration:migration-step",
        ] + [dep for ver in oracle_versions(versions) for dep in runfiles(ver)],
        args = ["--oracle"] + oracle_versions(versions),
        **kwargs
    ) if len(oracle_versions(versions)) > 1 else None
