# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

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
    if semver.find("-snapshot.") > 0:
        ghc = semver[:-9].replace("-snapshot.", ".")
    else:
        ghc = semver
    ctx.file(
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
