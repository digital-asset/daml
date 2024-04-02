# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_windows", "os_name")
load("@daml//bazel_tools/dev_env_tool:dev_env_tool.bzl", "dadew_tool_home", "dadew_where")
load("@io_bazel_rules_scala//scala:scala_cross_version.bzl", "default_maven_server_urls")
load("//bazel_tools:versions.bzl", "versions")

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
    # The Daml assistant will mark the installed SDK read-only.
    # This breaks Bazel horribly on Windows to the point where
    # even `bazel clean --expunge` fails because it cannot remove
    # the installed SDK. Therefore, we do not use the assistant to
    # install the SDK but instead simply extract the SDK to the right
    # location and set the symlink ourselves.
    out_dir = ctx.path("sdk").get_child("sdk").get_child(ctx.attr.version)

    if ctx.attr.sdk_tarball:
        ctx.extract(
            ctx.attr.sdk_tarball,
            output = out_dir,
            stripPrefix = "sdk-{}".format(ctx.attr.version),
        )
        sha256sum = "sha256sum"
        if is_windows:
            ps = ctx.which("powershell")
            dadew = dadew_where(ctx, ps)
            sha256sum = dadew_tool_home(dadew, "msys2") + "\\usr\\bin\\sha256sum.exe"

        exec_result = ctx.execute([sha256sum, ctx.path(ctx.attr.sdk_tarball)])
        if exec_result.return_code:
            fail("Error executing sha256sum: {stdout}\n{stderr}".format(stdout = exec_result.stdout, stderr = exec_result.stderr))
        sdk_checksum = exec_result.stdout.strip()
    elif ctx.attr.sdk_sha256:
        ctx.download_and_extract(
            output = out_dir,
            url =
                "https://github.com/digital-asset/daml/releases/download/v{}/daml-sdk-{}-{}.tar.gz".format(ctx.attr.version, ctx.attr.version, ctx.attr.os_name),
            sha256 = ctx.attr.sdk_sha256[ctx.attr.os_name],
            stripPrefix = "sdk-{}".format(ctx.attr.version),
        )
        sdk_checksum = ctx.attr.sdk_sha256[ctx.attr.os_name]
    else:
        fail("Must specify either sdk_tarball or sdk_sha256")

    if ctx.attr.test_tool:
        ctx.symlink(ctx.attr.test_tool, "ledger-api-test-tool.jar")
    elif ctx.attr.test_tool_sha256:
        ctx.download(
            output = "ledger-api-test-tool.jar",
            url = ["{mirror}/com/daml/ledger-api-test-tool/{version}/ledger-api-test-tool-{version}.jar".format(mirror = mirror, version = ctx.attr.version) for mirror in default_maven_server_urls()],
            sha256 = ctx.attr.test_tool_sha256,
        )
    else:
        fail("Must specify either test_tool or test_tool_sha256")

    if versions.is_at_least("2.6.0", ctx.attr.version):
        stripPrefix = "test-common"
    else:
        stripPrefix = "ledger/test-common"

    ctx.extract(
        "ledger-api-test-tool.jar",
        output = "extracted-test-tool",
        # We cannot fully extract the JAR because there are files
        # that clash on case insensitive file systems. Luckily, we only
        # need the DAR so this is not an issue.
        stripPrefix = stripPrefix,
    )

    if ctx.attr.create_daml_app_patch:
        ctx.symlink(ctx.attr.create_daml_app_patch, "create_daml_app.patch")
    elif ctx.attr.test_tool_sha256:
        # TODO: update once we have 2.7 and/or 2.8 on subdir
        if versions.is_at_most("2.4.0", ctx.attr.version):
            ctx.download(
                output = "create_daml_app.patch",
                url = "https://raw.githubusercontent.com/digital-asset/daml/v{}/sdk/templates/create-daml-app-test-resources/messaging.patch".format(ctx.attr.version),
                sha256 = ctx.attr.create_daml_app_patch_sha256,
            )
        else:
            ctx.download(
                output = "create_daml_app.patch",
                url = "https://raw.githubusercontent.com/digital-asset/daml/v{}/templates/create-daml-app-test-resources/messaging.patch".format(ctx.attr.version),
                sha256 = ctx.attr.create_daml_app_patch_sha256,
            )
    else:
        fail("Must specify either test_tool or test_tool_sha256")

    for lib in ["types", "ledger", "react"]:
        tarball_name = "daml_{}_tarball".format(lib)
        if getattr(ctx.attr, tarball_name):
            ctx.symlink(
                getattr(ctx.attr, tarball_name),
                "daml-{}.tgz".format(lib),
            )
        else:
            ctx.download(
                output = "daml-{}.tgz".format(lib),
                url = "https://registry.npmjs.org/@daml/{}/-/{}-{}.tgz".format(lib, lib, ctx.attr.version),
                sha256 = getattr(ctx.attr, "daml_{}_sha256".format(lib)),
            )

    ctx.symlink(out_dir.get_child("daml").get_child("daml" + (".exe" if is_windows else "")), "sdk/bin/daml")
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

    # Depending on all files as runfiles results in thousands of symlinks
    # which eventually results in us running out of inodes on CI.
    # By writing all checksums to a file and only depending on that we get
    # only one extra runfile while still making sure that things are properly
    # invalidated.
    ctx.file(
        "sdk/sdk/{version}/checksums".format(version = ctx.attr.version),
        # We don’t really care about the order here but find is non-deterministic
        # and having something fixed is clearly better for caching.
        content = sdk_checksum,
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
    ctx.template(
        "daml.cc",
        Label("@compatibility//bazel_tools:daml.cc.tpl"),
        substitutions = {"{SDK_VERSION}": ctx.attr.version},
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
cc_binary(
  name = "daml",
  srcs = ["daml.cc"],
  data = [":sdk/bin/daml", ":sdk/sdk/{version}/checksums"],
  deps = ["@bazel_tools//tools/cpp/runfiles:runfiles"],
)
# Needed to provide the same set of DARs to the ledger that
# are used by the ledger API test tool.
filegroup(
    name = "dar-files",
    srcs = glob(["extracted-test-tool/**/*.dar"], exclude = ["**/*-dev.dar"]),
)
exports_files(["daml-types.tgz", "daml-ledger.tgz", "daml-react.tgz", "create_daml_app.patch"])
""".format(version = ctx.attr.version),
    )
    return None

_daml_sdk = repository_rule(
    implementation = _daml_sdk_impl,
    attrs = {
        "version": attr.string(mandatory = True),
        "os_name": attr.string(mandatory = False, default = os_name),
        "sdk_sha256": attr.string_dict(mandatory = False),
        "sdk_tarball": attr.label(allow_single_file = True, mandatory = False),
        "test_tool_sha256": attr.string(mandatory = False),
        "test_tool": attr.label(allow_single_file = True, mandatory = False),
        "daml_types_tarball": attr.label(allow_single_file = True, mandatory = False),
        "daml_ledger_tarball": attr.label(allow_single_file = True, mandatory = False),
        "daml_react_tarball": attr.label(allow_single_file = True, mandatory = False),
        "daml_types_sha256": attr.string(mandatory = False),
        "daml_ledger_sha256": attr.string(mandatory = False),
        "daml_react_sha256": attr.string(mandatory = False),
        "create_daml_app_patch": attr.label(allow_single_file = True, mandatory = False),
        "create_daml_app_patch_sha256": attr.string(mandatory = False),
    },
)

def daml_sdk(version, **kwargs):
    _daml_sdk(
        name = "daml-sdk-{}".format(version),
        version = version,
        **kwargs
    )

def daml_sdk_head(sdk_tarball, ledger_api_test_tool, daml_types_tarball, daml_ledger_tarball, daml_react_tarball, create_daml_app_patch, **kwargs):
    version = "0.0.0"
    _daml_sdk(
        name = "daml-sdk-{}".format(version),
        version = version,
        sdk_tarball = sdk_tarball,
        test_tool = ledger_api_test_tool,
        daml_types_tarball = daml_types_tarball,
        daml_ledger_tarball = daml_ledger_tarball,
        daml_react_tarball = daml_react_tarball,
        create_daml_app_patch = create_daml_app_patch,
        **kwargs
    )
