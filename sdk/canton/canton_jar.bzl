# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("//canton:canton_version.bzl", "CANTON_OPEN_SOURCE_TAG", "CANTON_OPEN_SOURCE_SHA", "USE_LOCAL_CANTON_INSTEAD")

def canton_jar():
    write_file(
        name = "use_local_canton_instead_path",
        content = [USE_LOCAL_CANTON_INSTEAD] if USE_LOCAL_CANTON_INSTEAD != None else [],
        out = "use_local_canton_instead_path",
        visibility = ["//visibility:private"],
    )
    if USE_LOCAL_CANTON_INSTEAD != None:
        copy_file(
            name = "canton_open_source_jar",
            src = USE_LOCAL_CANTON_INSTEAD + "/community/app/target/release/canton-open-source-3.5.0-SNAPSHOT/lib/canton-open-source-3.5.0-SNAPSHOT.jar",
            out = "canton_jar.jar",
            visibility = ["//visibility:private"],
        )
    else:
        native.genrule(
            name = "canton_open_source_jar",
            srcs = [],
            outs = ["canton_jar.jar"],
            cmd = """
        TMPDIR=$$(mktemp -d)
        # oras requires some home directory, we provide a random one
        FAKE_HOME=$$(mktemp -d)
        export HOME=$$FAKE_HOME
        export USERPROFILE=$$FAKE_HOME
        REF=europe-docker.pkg.dev/da-images/public-unstable/components/canton-open-source:{tag}@{sha}
        $(execpath @dpm_binary//:oras) pull $$REF --output $$TMPDIR
        cp $$TMPDIR/lib/canton-open-source-{tag}.jar $@
        """.format(
                sha = CANTON_OPEN_SOURCE_SHA,
                tag = CANTON_OPEN_SOURCE_TAG,
            ),
            tools = ["@dpm_binary//:oras"],
            visibility = ["//visibility:private"],
        )

