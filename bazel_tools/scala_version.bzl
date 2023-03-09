# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Keep the Scala versions in sync with /nix/nixpkgs.nix and /release/src/Options.hs.
# When upgrading download the libraries from maven and then use the sha256sum to generate the checksum.

default_scala_version = "2.13.10"

scala_artifacts = {
    "2.13.10": {
        "io_bazel_rules_scala_scala_compiler": {
            "artifact": "org.scala-lang:scala-compiler:2.13.10",
            "sha256": "2cd4a964ea48e78c91a2a7b19c4fc67a9868728ace5ee966b1d498270b3c3aa7",
        },
        "io_bazel_rules_scala_scala_library": {
            "artifact": "org.scala-lang:scala-library:2.13.10",
            "sha256": "e6ca607c3fce03e8fa38af3374ce1f8bb098e316e8bf6f6d27331360feddb1c1",
        },
        "io_bazel_rules_scala_scala_reflect": {
            "artifact": "org.scala-lang:scala-reflect:2.13.10",
            "sha256": "62bd7a7198193c5373a992c122990279e413af3307162472a5d3cbb8ecadb35e",
        },
    },
}

def _impl(ctx):
    # Generates an empty BUILD file, because we do not need to build anything.
    ctx.file(
        "BUILD",
        content = """exports_files(["index.bzl"])""",
        executable = False,
    )

    version = ctx.os.environ.get("DAML_SCALA_VERSION", default = default_scala_version)
    if version == "":
        version = default_scala_version
    suffix = version.replace(".", "_")

    major = version[:version.rfind(".")]
    major_suffix = major.replace(".", "_")
    artifacts = scala_artifacts.get(version) or fail("Unknown Scala version: %s" % version)
    ctx.file(
        "index.bzl",
        content = """
scala_version = "{version}"
scala_major_version = "{major}"
scala_version_suffix = "{suffix}"
scala_major_version_suffix = "{major_suffix}"
scala_artifacts = {artifacts}
""".format(
            version = version,
            major = major,
            suffix = suffix,
            major_suffix = major_suffix,
            artifacts = artifacts,
        ),
        executable = False,
    )

scala_version_configure = repository_rule(
    environ = ["DAML_SCALA_VERSION"],
    implementation = _impl,
    attrs = {},
)
