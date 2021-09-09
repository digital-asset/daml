# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Keep the Scala versions in sync with /nix/nixpkgs.nix and /release/src/Options.hs.

default_scala_version = "2.13.6"

scala_artifacts = {
    "2.12.14": {
        "io_bazel_rules_scala_scala_compiler": {
            "artifact": "org.scala-lang:scala-compiler:2.12.14",
            "sha256": "2a1b3fbf9c956073c8c5374098a6f987e3b8d76e34756ab985fc7d2ca37ee113",
        },
        "io_bazel_rules_scala_scala_library": {
            "artifact": "org.scala-lang:scala-library:2.12.14",
            "sha256": "0451dce8322903a6c2aa7d31232b54daa72a61ced8ade0b4c5022442a3f6cb57",
        },
        "io_bazel_rules_scala_scala_reflect": {
            "artifact": "org.scala-lang:scala-reflect:2.12.14",
            "sha256": "497f4603e9d19dc4fa591cd467de5e32238d240bbd955d3dac6390b270889522",
        },
    },
    "2.13.6": {
        "io_bazel_rules_scala_scala_compiler": {
            "artifact": "org.scala-lang:scala-compiler:2.13.6",
            "sha256": "310d263d622a3d016913e94ee00b119d270573a5ceaa6b21312d69637fd9eec1",
        },
        "io_bazel_rules_scala_scala_library": {
            "artifact": "org.scala-lang:scala-library:2.13.6",
            "sha256": "f19ed732e150d3537794fd3fe42ee18470a3f707efd499ecd05a99e727ff6c8a",
        },
        "io_bazel_rules_scala_scala_reflect": {
            "artifact": "org.scala-lang:scala-reflect:2.13.6",
            "sha256": "f713593809b387c60935bb9a940dfcea53bd0dbf8fdc8d10739a2896f8ac56fa",
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
