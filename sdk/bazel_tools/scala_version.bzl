# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Keep the Scala versions in sync with /nix/nixpkgs.nix and /release/src/Options.hs.
# When upgrading download the libraries from maven and then use the sha256sum to generate the checksum.

default_scala_version = "2.13.16"

scala_artifacts = {
    "2.13.16": {
        "io_bazel_rules_scala_scala_compiler": {
            "artifact": "org.scala-lang:scala-compiler:2.13.16",
            "sha256": "f59982714591e321ba9c087af2c8666e2f5fb92b11a0cef72c2c5e9b342152d3",
        },
        "io_bazel_rules_scala_scala_library": {
            "artifact": "org.scala-lang:scala-library:2.13.16",
            "sha256": "1ebb2b6f9e4eb4022497c19b1e1e825019c08514f962aaac197145f88ed730f1",
        },
        "io_bazel_rules_scala_scala_reflect": {
            "artifact": "org.scala-lang:scala-reflect:2.13.16",
            "sha256": "fb49ccd9cac7464486ab993cda20a3c1569d8ef26f052e897577ad2a4970fb1d",
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
