# Copyright (c) 2019 The DAML Authors. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# The dependencies of the daml workspace.
# This allows using the daml workspace externally
# from another bazel workspace.
#
# For example, another Bazel project can depend on
# targets in the daml repository by doing:
# ---
# local_repository(
#   name = "com_github_digital_asset_daml",
#   path = "/path/to/daml"
# )
# load("@com_github_digital_asset_daml//:deps.bzl", "daml_deps")
# daml_deps()
# ---
#
# A 3rd-party consumer would also need to register relevant
# toolchains and repositories in order to build targets.
# That is, copy some setup calls from WORKSPACE into the
# other WORKSPACE.
#
# Make sure to reference repository local files with the full
# prefix: @com_github_digital_asset_daml//..., as these won't
# be resolvable from external workspaces otherwise.

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

rules_scala_version = "8092d5f6165a8d9c4797d5f089c1ba4eee3326b1"
rules_haskell_version = "74d91c116c59f0a3ad41e376d3307d85ddcc3253"
rules_haskell_sha256 = "5f2423d4707c5601f465d7343c68ff4e8f271c1269e79af4dc2d156cb8a0c17d"
rules_nixpkgs_version = "2980a2f75a178bb3c521d46380a52fe72c656239"

def daml_deps():
    if "rules_haskell" not in native.existing_rules():
        http_archive(
            name = "rules_haskell",
            strip_prefix = "rules_haskell-%s" % rules_haskell_version,
            urls = ["https://github.com/tweag/rules_haskell/archive/%s.tar.gz" % rules_haskell_version],
            patches = [
                # Remove once https://github.com/tweag/rules_haskell/pull/1039 is merged.
                "@com_github_digital_asset_daml//bazel_tools:haskell-cc-wrapper.patch",
                # Upstream once https://github.com/tweag/rules_haskell/pull/1039 is merged.
                # Used to work around https://github.com/tweag/rules_haskell/issues/1062.
                "@com_github_digital_asset_daml//bazel_tools:haskell-cc-wrapper-include-dirs.patch",
                "@com_github_digital_asset_daml//bazel_tools:haskell-cc-wrapper-globbing.patch",
                "@com_github_digital_asset_daml//bazel_tools:haskell-windows-extra-libraries.patch",
                "@com_github_digital_asset_daml//bazel_tools:haskell-ghci-grpc.patch",
                "@com_github_digital_asset_daml//bazel_tools:haskell_public_ghci_repl_wrapper.patch",
                "@com_github_digital_asset_daml//bazel_tools:haskell-no-isystem.patch",
                "@com_github_digital_asset_daml//bazel_tools:haskell-opt.patch",
            ],
            patch_args = ["-p1"],
            sha256 = rules_haskell_sha256,
        )

    if "io_tweag_rules_nixpkgs" not in native.existing_rules():
        http_archive(
            name = "io_tweag_rules_nixpkgs",
            strip_prefix = "rules_nixpkgs-%s" % rules_nixpkgs_version,
            urls = ["https://github.com/tweag/rules_nixpkgs/archive/%s.tar.gz" % rules_nixpkgs_version],
            sha256 = "a37917d9cb535466d94cbedab5a3d7365fc333843b2265cfb0a5211647769b88",
        )

    if "ai_formation_hazel" not in native.existing_rules():
        http_archive(
            name = "ai_formation_hazel",
            strip_prefix = "rules_haskell-{}/hazel".format(rules_haskell_version),
            urls = ["https://github.com/tweag/rules_haskell/archive/%s.tar.gz" % rules_haskell_version],
            sha256 = rules_haskell_sha256,
        )

    if "com_github_madler_zlib" not in native.existing_rules():
        http_archive(
            name = "com_github_madler_zlib",
            build_file = "@com_github_digital_asset_daml//3rdparty/c:zlib.BUILD",
            strip_prefix = "zlib-cacf7f1d4e3d44d871b605da3b647f07d718623f",
            urls = ["https://github.com/madler/zlib/archive/cacf7f1d4e3d44d871b605da3b647f07d718623f.tar.gz"],
            sha256 = "6d4d6640ca3121620995ee255945161821218752b551a1a180f4215f7d124d45",
        )

    if "bzip2" not in native.existing_rules():
        http_archive(
            name = "bzip2",
            build_file = "@com_github_digital_asset_daml//3rdparty/c:bzip2.BUILD",
            strip_prefix = "bzip2-1.0.8",
            urls = ["https://sourceware.org/pub/bzip2/bzip2-1.0.8.tar.gz"],
            sha256 = "ab5a03176ee106d3f0fa90e381da478ddae405918153cca248e682cd0c4a2269",
        )

    if "io_bazel_rules_go" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_go",
            urls = ["https://github.com/bazelbuild/rules_go/releases/download/0.18.6/rules_go-0.18.6.tar.gz"],
            sha256 = "f04d2373bcaf8aa09bccb08a98a57e721306c8f6043a2a0ee610fd6853dcde3d",
        )

    if "rules_jvm_external" not in native.existing_rules():
        http_archive(
            name = "rules_jvm_external",
            strip_prefix = "rules_jvm_external-2.8",
            sha256 = "79c9850690d7614ecdb72d68394f994fef7534b292c4867ce5e7dec0aa7bdfad",
            url = "https://github.com/bazelbuild/rules_jvm_external/archive/2.8.zip",
        )

    if "io_bazel_rules_scala" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_scala",
            url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip" % rules_scala_version,
            type = "zip",
            strip_prefix = "rules_scala-%s" % rules_scala_version,
            sha256 = "db536b9db36b5aa737db9d08fa05d1fa5531c9cf213b04bed4e9b9fc34cc2390",
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:scala-escape-jvmflags.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_google_protobuf" not in native.existing_rules():
        http_archive(
            name = "com_google_protobuf",
            sha256 = "1e622ce4b84b88b6d2cdf1db38d1a634fe2392d74f0b7b74ff98f3a51838ee53",
            strip_prefix = "protobuf-3.8.0",
            urls = ["https://github.com/google/protobuf/archive/v3.8.0.zip"],
        )

    if "io_bazel_skydoc" not in native.existing_rules():
        http_archive(
            name = "io_bazel_skydoc",
            sha256 = "c2d66a0cc7e25d857e480409a8004fdf09072a1bd564d6824441ab2f96448eea",
            strip_prefix = "skydoc-0.3.0",
            urls = ["https://github.com/bazelbuild/skydoc/archive/0.3.0.tar.gz"],
        )

    if "bazel_gazelle" not in native.existing_rules():
        http_archive(
            name = "bazel_gazelle",
            urls = ["https://github.com/bazelbuild/bazel-gazelle/releases/download/0.17.0/bazel-gazelle-0.17.0.tar.gz"],
            sha256 = "3c681998538231a2d24d0c07ed5a7658cb72bfb5fd4bf9911157c0e9ac6a2687",
        )

    if "io_bazel_rules_sass" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_sass",
            sha256 = "7f0d64061e5bac749275349a7a7918b6f5759365f289192ff791f3c1495afcf1",
            strip_prefix = "rules_sass-1.22.3",
            urls = ["https://github.com/bazelbuild/rules_sass/archive/1.22.3.tar.gz"],
        )

    # Fetch rules_nodejs so we can install our npm dependencies
    if "build_bazel_rules_nodejs" not in native.existing_rules():
        http_archive(
            name = "build_bazel_rules_nodejs",
            urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/0.32.2/rules_nodejs-0.32.2.tar.gz"],
            sha256 = "6d4edbf28ff6720aedf5f97f9b9a7679401bf7fca9d14a0fff80f644a99992b4",
            patches = ["@com_github_digital_asset_daml//bazel_tools:rules_nodejs_default_shell_env.patch"],
            patch_args = ["-p1"],
        )

    if "com_github_grpc_grpc" not in native.existing_rules():
        http_archive(
            name = "com_github_grpc_grpc",
            strip_prefix = "grpc-1.23.0",
            urls = ["https://github.com/grpc/grpc/archive/v1.23.0.tar.gz"],
            sha256 = "f56ced18740895b943418fa29575a65cc2396ccfa3159fa40d318ef5f59471f9",
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:grpc-bazel-mingw.patch",
            ],
            patch_args = ["-p1"],
        )

    if "io_grpc_grpc_java" not in native.existing_rules():
        http_archive(
            name = "io_grpc_grpc_java",
            strip_prefix = "grpc-java-1.21.0",
            urls = ["https://github.com/grpc/grpc-java/archive/v1.21.0.tar.gz"],
            sha256 = "9bc289e861c6118623fcb931044d843183c31d0e4d53fc43c4a32b56d6bb87fa",
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:grpc-java-plugin-visibility.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_github_johnynek_bazel_jar_jar" not in native.existing_rules():
        http_archive(
            name = "com_github_johnynek_bazel_jar_jar",
            sha256 = "841ae424eec3f322d411eb49d949622cc84787cb4189a30698fa9adadb98deac",
            strip_prefix = "bazel_jar_jar-20dbf71f09b1c1c2a8575a42005a968b38805519",
            urls = ["https://github.com/johnynek/bazel_jar_jar/archive/20dbf71f09b1c1c2a8575a42005a968b38805519.zip"],  # Latest commit SHA as at 2019/02/13
        )

    if "com_github_scalapb_scalapb" not in native.existing_rules():
        http_archive(
            name = "com_github_scalapb_scalapb",
            url = "https://github.com/scalapb/ScalaPB/releases/download/v0.8.0/scalapbc-0.8.0.zip",
            sha256 = "bda0b44b50f0a816342a52c34e6a341b1a792f2a6d26f4f060852f8f10f5d854",
            strip_prefix = "scalapbc-0.8.0/lib",
            build_file_content = """
java_import(
    name = "compilerplugin",
    jars = ["com.thesamet.scalapb.compilerplugin-0.8.0.jar"],
    visibility = ["//visibility:public"],
)
java_import(
    name = "scala-library",
    jars = ["org.scala-lang.scala-library-2.11.12.jar"],
    visibility = ["//visibility:public"],
)
            """,
        )

        if "com_github_googleapis_googleapis" not in native.existing_rules():
            http_archive(
                name = "com_github_googleapis_googleapis",
                strip_prefix = "googleapis-6c48ab5aef47dc14e02e2dc718d232a28067129d",
                urls = ["https://github.com/googleapis/googleapis/archive/6c48ab5aef47dc14e02e2dc718d232a28067129d.tar.gz"],
                sha256 = "70d7be6ad49b4424313aad118c8622aab1c5fdd5a529d4215d3884ff89264a71",
            )

    # Buildifier.
    # It is written in Go and hence needs rules_go to be available.
    if "com_github_bazelbuild_buildtools" not in native.existing_rules():
        http_archive(
            name = "com_github_bazelbuild_buildtools",
            sha256 = "86592d703ecbe0c5cbb5139333a63268cf58d7efd2c459c8be8e69e77d135e29",
            strip_prefix = "buildtools-0.26.0",
            url = "https://github.com/bazelbuild/buildtools/archive/0.26.0.tar.gz",
        )

    native.bind(
        name = "guava",
        actual = "@com_google_guava_guava//jar",
    )
    native.bind(
        name = "gson",
        actual = "@com_google_code_gson_gson//jar",
    )

    if "com_github_google_bazel_common" not in native.existing_rules():
        http_archive(
            name = "com_github_google_bazel_common",
            sha256 = "48a209fed9575c9d108eaf11fb77f7fe6178a90135e4d60cac6f70c2603aa53a",
            strip_prefix = "bazel-common-9e3880428c1837db9fb13335ed390b7e33e346a7",
            urls = ["https://github.com/google/bazel-common/archive/9e3880428c1837db9fb13335ed390b7e33e346a7.zip"],
        )
