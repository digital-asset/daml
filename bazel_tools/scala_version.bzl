# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Keep in sync with /nix/nixpkgs.nix and /release/src/Main.hs
default_scala_version = "2.12.13"

scala_artifacts = {
    "2.12.13": {
        "io_bazel_rules_scala_scala_compiler": {
            "artifact": "org.scala-lang:scala-compiler:2.12.13",
            "sha256": "ea971e004e2f15d3b7569eee8b559f220e23b9993e688bbe986f97938d1dc9f9",
        },
        "io_bazel_rules_scala_scala_library": {
            "artifact": "org.scala-lang:scala-library:2.12.13",
            "sha256": "1bb415cff43f792636556a1137b213b192ab0246be003680a3b006d01235dd89",
        },
        "io_bazel_rules_scala_scala_reflect": {
            "artifact": "org.scala-lang:scala-reflect:2.12.13",
            "sha256": "2bd46318d87945e72eb186a7b5ea496c43cf8f0aabc6ff11b3e7962f8635e669",
        },
    },
    "2.13.3": {
        "io_bazel_rules_scala_scala_library": {
            "artifact": "org.scala-lang:scala-library:2.13.3",
            "sha256": "cb0eb1a33a6056b2e652f26923bfa361348ae72a2119da7b78dc1f673d1a93b1",
        },
        "io_bazel_rules_scala_scala_compiler": {
            "artifact": "org.scala-lang:scala-compiler:2.13.3",
            "sha256": "e86e4f70d30cb3fd78e82aa6e6d82b2b3d1b46bd209a31f833a03b370ae0f32a",
        },
        "io_bazel_rules_scala_scala_reflect": {
            "artifact": "org.scala-lang:scala-reflect:2.13.3",
            "sha256": "959dc9ab8aad84e2fc7adacfb84f2ed908caee22c4c2d291dd818f0c40c6ed5b",
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
