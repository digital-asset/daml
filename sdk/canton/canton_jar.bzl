# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@bazel_skylib//rules:write_file.bzl", "write_file")
load("//canton:canton_version.bzl", "CANTON_OPEN_SOURCE_SHA", "CANTON_OPEN_SOURCE_TAG", "LOCAL_CANTON_OVERRIDE")

def local_canton_repo():
    if LOCAL_CANTON_OVERRIDE != None:
        native.new_local_repository(
            name = "canton_local",
            path = LOCAL_CANTON_OVERRIDE,
            build_file_content = """
filegroup(
  name = "bundle.jar",
  srcs = glob([
    "community/app/target/release/canton-open-source-3.5.0-SNAPSHOT/lib/canton-open-source-3.5.0-SNAPSHOT.jar"
  ]),
  visibility = ["//visibility:public"],
)
            """,
        )

def _canton_bucket(tag):
    # Stable release tags look like "3.5.1" or "3.5.1-rc2"; everything else
    # (e.g. snapshots) lives in the "public-unstable" bucket.
    parts = tag.split("-", 1)
    version = parts[0]
    suffix = parts[1] if len(parts) > 1 else ""
    version_parts = version.split(".")
    if len(version_parts) != 3:
        return "public-unstable"
    for p in version_parts:
        if not p.isdigit():
            return "public-unstable"
    if suffix and not (suffix.startswith("rc") and suffix[2:].isdigit()):
        return "public-unstable"
    return "public"

def canton_jar():
    if LOCAL_CANTON_OVERRIDE != None:
        copy_file(
            name = "canton_open_source_jar",
            src = "@canton_local//:bundle.jar",
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
        REF=europe-docker.pkg.dev/da-images/{bucket}/components/canton-open-source:{tag}@{sha}
        $(execpath @dpm_binary//:oras) pull $$REF --output $$TMPDIR
        cp $$TMPDIR/lib/canton-open-source-{tag}.jar $@
        """.format(
                sha = CANTON_OPEN_SOURCE_SHA,
                tag = CANTON_OPEN_SOURCE_TAG,
                bucket = _canton_bucket(CANTON_OPEN_SOURCE_TAG),
            ),
            tools = ["@dpm_binary//:oras"],
            visibility = ["//visibility:private"],
        )
