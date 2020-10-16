# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def is_figure(c):
    return c == "0" or c == "1" or c == "2" or c == "3" or c == "4" or c == "5" or c == "6" or c == "7" or c == "8" or c == "9"

def is_number(s):
    for c in s.elems():
        if not is_figure(c):
            return False
    return True

def _impl(ctx):
    # Generates an empty BUILD file, because we do not need to build anything.
    ctx.file(
        "BUILD",
        content = "",
        executable = False,
    )

    # Generates a simple Bazel file that just sets a bunch of Bazel variables,
    # so they can be used in our main Bazel BUILD files.
    semver = ctx.os.environ.get("DAML_SDK_RELEASE_VERSION", default = "0.0.0")
    if semver.find("-") > 0:
        ghc_list = []
        for segment in semver.replace("-", ".").split("."):
            if is_number(segment):
                ghc_list.append(segment)
        ghc = ghc_list[0]
        for elem in ghc_list[1:]:
            ghc = ghc + "." + elem
    else:
        ghc = semver
    return ctx.file(
        "configuration.bzl",
        content =
            """
npm_version = "{NPM_VERSION}"
mvn_version = "{MVN_VERSION}"
ghc_version = "{GHC_VERSION}"
sdk_version = "{SDK_VERSION}"
""".format(
                SDK_VERSION = semver,
                NPM_VERSION = semver,
                MVN_VERSION = semver,
                GHC_VERSION = ghc,
            ),
        executable = False,
    )

build_environment = repository_rule(
    # Tell Bazel that this rule will produce different results if any of the
    # env vars in the list has changed.
    environ = ["DAML_SDK_RELEASE_VERSION"],
    implementation = _impl,
    attrs = {},
)
