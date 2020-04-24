# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

runfiles_library = """
# Copy-pasted from the Bazel Bash runfiles library v2.
set -uo pipefail; f=bazel_tools/tools/bash/runfiles/runfiles.bash
source "${RUNFILES_DIR:-/dev/null}/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "${RUNFILES_MANIFEST_FILE:-/dev/null}" | cut -f2- -d' ')" 2>/dev/null || \
  source "$0.runfiles/$f" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  source "$(grep -sm1 "^$f " "$0.exe.runfiles_manifest" | cut -f2- -d' ')" 2>/dev/null || \
  { echo>&2 "ERROR: cannot find $f"; exit 1; }; f=; set -e
# --- end runfiles.bash initialization v2 ---
"""

def _daml_sdk_impl(ctx):
    ctx.download_and_extract(
        output = "extracted-sdk",
        # TODO (MK) Make this work on other platforms.
        url =
            "https://github.com/digital-asset/daml/releases/download/v{}/daml-sdk-{}-linux.tar.gz".format(ctx.attr.version, ctx.attr.version),
        sha256 = ctx.attr.sdk_sha256,
        stripPrefix = "sdk-{}".format(ctx.attr.version),
    )
    ctx.download(
        output = "ledger-api-test-tool.jar",
        url = "https://repo1.maven.org/maven2/com/daml/ledger-api-test-tool/{}/ledger-api-test-tool-{}.jar".format(ctx.attr.version, ctx.attr.version),
        sha256 = ctx.attr.test_tool_sha256,
    )
    ctx.extract(
        "ledger-api-test-tool.jar",
        output = "extracted-test-tool",
    )
    ps_result = ctx.execute(
        ["extracted-sdk/daml/daml", "install", "extracted-sdk", "--install-assistant=no"],
        environment = {
            "DAML_HOME": "sdk",
        },
    )
    if ps_result.return_code != 0:
        fail("Failed to install SDK.\nExit code %d.\n%s\n%s" %
             (ps_result.return_code, ps_result.stdout, ps_result.stderr))

    # At least on older SDKs, the symlinking in --install-assistant=yes does not work
    # properly so we symlink ourselves.
    ctx.symlink("sdk/sdk/{}/daml/daml".format(ctx.attr.version), "sdk/bin/daml")
    ctx.file(
        "sdk/daml-config.yaml",
        content =
            """
auto-install: false
update-check: never
""",
    )
    ctx.file(
        "ledger-api-test-tool.sh",
        content =
            """#!/usr/bin/env bash
{runfiles_library}
$JAVA_HOME/bin/java -jar $(rlocation daml-sdk-{version}/ledger-api-test-tool.jar) $@
""".format(version = ctx.attr.version, runfiles_library = runfiles_library),
    )
    ctx.file(
        "daml.sh",
        content =
            """#!/usr/bin/env bash
# The assistant assumes Java is in PATH.
# Here we just rely on Bazel always providing JAVA_HOME.
export PATH=$JAVA_HOME/bin:$PATH
{runfiles_library}
$(rlocation daml-sdk-{version}/sdk/bin/daml) $@
""".format(version = ctx.attr.version, runfiles_library = runfiles_library),
    )
    ctx.file(
        "BUILD",
        content =
            """
package(default_visibility = ["//visibility:public"])
sh_binary(
  name = "ledger-api-test-tool",
  srcs = [":ledger-api-test-tool.sh"],
  data = [":ledger-api-test-tool.jar"],
  deps = ["@bazel_tools//tools/bash/runfiles"],
)
sh_binary(
  name = "daml",
  srcs = [":daml.sh"],
  data = [":sdk/bin/daml"],
  deps = ["@bazel_tools//tools/bash/runfiles"],
)
# Needed to provide the same set of DARs to the ledger that
# are used by the ledger API test tool.
filegroup(
    name = "dar-files",
    srcs = glob(["extracted-test-tool/ledger/test-common/**"]),
)
""",
    )
    return None

_daml_sdk = repository_rule(
    implementation = _daml_sdk_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "sdk_sha256": attr.string(mandatory = True),
        "test_tool_sha256": attr.string(mandatory = True),
    },
)

def daml_sdk(version, **kwargs):
    _daml_sdk(
        name = "daml-sdk-{}".format(version),
        version = version,
        **kwargs
    )
