# Copyright (c) 2020 The DAML Authors. All rights reserved.
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

rules_scala_version = "6c16cff213b76a4126bdc850956046da5db1daaa"

rules_haskell_version = "107ab5ccf0cdf884e19c1b3a37b9b8064c4e4e03"
rules_haskell_sha256 = "758f8190a9dd6e5e6fd7c9fb38a1bb4c5743a6e314d6678761e2cc070d8e465b"
rules_nixpkgs_version = "33c50ba64c11dddb95823d12f6b1324083cc5c43"
rules_nixpkgs_sha256 = "91fedd5151bbd9ef89efc39e2172921bd7036c68cff54712a5df8ddf62bd6922"
davl_version = "625a5791458c54051adb6d1e41e720c673951b72"
davl_sha256 = "fa300aacb00096d61f527422bbab2f98a38d7438795f80d2e6cbc365fc5256f3"

def daml_deps():
    if "rules_haskell" not in native.existing_rules():
        http_archive(
            name = "rules_haskell",
            strip_prefix = "rules_haskell-%s" % rules_haskell_version,
            urls = ["https://github.com/tweag/rules_haskell/archive/%s.tar.gz" % rules_haskell_version],
            patches = [
                # The fake libs issue should be fixed in upstream rules_haskell
                # or GHC. Remove this patch once that's available.
                "@com_github_digital_asset_daml//bazel_tools:haskell-windows-remove-fake-libs.patch",
                # This is a daml specific patch and not upstreamable.
                "@com_github_digital_asset_daml//bazel_tools:haskell-windows-extra-libraries.patch",
                # This is a daml specific patch and not upstreamable.
                "@com_github_digital_asset_daml//bazel_tools:haskell-ghci-grpc.patch",
                # rules_haskell should have builtin support for hie-bios.
                # Remove this patch once that's available.
                "@com_github_digital_asset_daml//bazel_tools:haskell_public_ghci_repl_wrapper.patch",
                # This fixes a ghc-lib specific build issue and is not upstreamable.
                # This might also be fixed by using `stack_snapshot` in the future.
                "@com_github_digital_asset_daml//bazel_tools:haskell-no-isystem.patch",
                # This should be made configurable in rules_haskell.
                # Remove this patch once that's available.
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
            sha256 = rules_nixpkgs_sha256,
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
            urls = [
                "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/rules_go/releases/download/v0.20.2/rules_go-v0.20.2.tar.gz",
                "https://github.com/bazelbuild/rules_go/releases/download/v0.20.2/rules_go-v0.20.2.tar.gz",
            ],
            sha256 = "b9aa86ec08a292b97ec4591cf578e020b35f98e12173bbd4a921f84f583aebd9",
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
            sha256 = "132cf8eeaab67f3142cec17152b8415901e7fa8396dd585d6334eec21bf7419d",
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:scala-escape-jvmflags.patch",
                "@com_github_digital_asset_daml//bazel_tools:scala-fail-jmh-build-on-error.patch",
            ],
            patch_args = ["-p1"],
        )

    if "io_bazel_rules_docker" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_docker",
            url = "https://github.com/bazelbuild/rules_docker/releases/download/v0.12.1/rules_docker-v0.12.1.tar.gz",
            strip_prefix = "rules_docker-0.12.1",
            sha256 = "14ac30773fdb393ddec90e158c9ec7ebb3f8a4fd533ec2abbfd8789ad81a284b",
        )

    if "com_google_protobuf" not in native.existing_rules():
        http_archive(
            name = "com_google_protobuf",
            sha256 = "1e622ce4b84b88b6d2cdf1db38d1a634fe2392d74f0b7b74ff98f3a51838ee53",
            strip_prefix = "protobuf-3.8.0",
            urls = ["https://github.com/google/protobuf/archive/v3.8.0.zip"],
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:proto-zlib-url.patch",
            ],
            patch_args = ["-p1"],
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
            urls = [
                "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/v0.19.1/bazel-gazelle-v0.19.1.tar.gz",
                "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.19.1/bazel-gazelle-v0.19.1.tar.gz",
            ],
            sha256 = "86c6d481b3f7aedc1d60c1c211c6f76da282ae197c3b3160f54bd3a8f847896f",
        )

    if "io_bazel_rules_sass" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_sass",
            sha256 = "7b9c9a88099d00dbb16be359c3b1946309d99673220c6b39c7e8bda8ecc692f8",
            strip_prefix = "rules_sass-1.24.4",
            urls = [
                "https://github.com/bazelbuild/rules_sass/archive/1.24.4.zip",
                "https://mirror.bazel.build/github.com/bazelbuild/rules_sass/archive/1.24.4.zip",
            ],
        )

    # Fetch rules_nodejs so we can install our npm dependencies
    if "build_bazel_rules_nodejs" not in native.existing_rules():
        http_archive(
            name = "build_bazel_rules_nodejs",
            urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/1.1.0/rules_nodejs-1.1.0.tar.gz"],
            sha256 = "c97bf38546c220fa250ff2cc052c1a9eac977c662c1fc23eda797b0ce8e70a43",
            patches = [
                # Work around for https://github.com/bazelbuild/rules_nodejs/issues/1565
                "@com_github_digital_asset_daml//bazel_tools:rules_nodejs_npm_cli_path.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_github_grpc_grpc" not in native.existing_rules():
        # This should be kept in sync with the grpc version we get from Nix.
        http_archive(
            name = "com_github_grpc_grpc",
            strip_prefix = "grpc-1.23.1",
            urls = ["https://github.com/grpc/grpc/archive/v1.23.1.tar.gz"],
            sha256 = "dd7da002b15641e4841f20a1f3eb1e359edb69d5ccf8ac64c362823b05f523d9",
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

    if "com_github_grpc_ecosystem_grpc_health_probe_binary" not in native.existing_rules():
        http_file(
            name = "com_github_grpc_ecosystem_grpc_health_probe_binary",
            sha256 = "bfbe82e34645e91cdf3bacbb0d2dc7786f3c3cc4da6b64a446e5fdfb7bb0429f",
            downloaded_file_path = "grpc-health-probe",
            urls = [
                "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.1/grpc_health_probe-linux-amd64",
            ],
            executable = True,
        )

    if "davl" not in native.existing_rules():
        http_archive(
            name = "davl",
            strip_prefix = "davl-{}".format(davl_version),
            urls = ["https://github.com/digital-asset/davl/archive/{}.tar.gz".format(davl_version)],
            sha256 = davl_sha256,
            build_file_content = """
package(default_visibility = ["//visibility:public"])
exports_files(["released/davl-v3.dar"])
            """,
        )
