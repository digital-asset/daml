# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

rules_haskell_version = "ac87721a4dbc0f7dbe731df928d322f02ed93330"
rules_haskell_sha256 = "684f91defad36e9d6ce3ac4213864b89e8f6fe813508ae93bfe80996447a1516"
rules_nixpkgs_version = "d3c7bc94fed4001d5375632a936d743dc085c9a1"
rules_nixpkgs_sha256 = "903c6b98aa6a298bf45a6b931e77a3313c40a0cb1b44fa00d9792f9e8aedbb35"
buildifier_version = "0.26.0"
buildifier_sha256 = "86592d703ecbe0c5cbb5139333a63268cf58d7efd2c459c8be8e69e77d135e29"
zlib_version = "cacf7f1d4e3d44d871b605da3b647f07d718623f"
zlib_sha256 = "6d4d6640ca3121620995ee255945161821218752b551a1a180f4215f7d124d45"
rules_nodejs_version = "1.6.0"
rules_nodejs_sha256 = "f9e7b9f42ae202cc2d2ce6d698ccb49a9f7f7ea572a78fd451696d03ef2ee116"

# Recent davl.
davl_version = "f2d7480d118f32626533d6a150a8ee7552cc0222"  # 2020-03-23, "Deploy upgrade to DAML SDK 0.13.56-snapshot.20200318",https://github.com/digital-asset/davl/pull/233/commits.
davl_sha256 = "3e8ae2a05724093e33b7f0363381e81a7e8e9655ccb3aa47ad540ea87e814321"

# Pinned davl relied on by damlc packaging tests.
davl_v3_version = "51d3977be2ab22f7f4434fd4692ca2e17a7cce23"
davl_v3_sha256 = "e8e76e21b50fb3adab36df26045b1e8c3ee12814abc60f137d39b864d2eae166"

# daml cheat sheet
daml_cheat_sheet_version = "32bc69d42c49be5844650ddf81d3ac37e5f7fc8b"  # 2020-05-19
daml_cheat_sheet_sha256 = "f21626f0eb258ad578d7a73afa2256d976fcf0680be2d5eeefbac392a9b01496"

def daml_deps():
    if "rules_haskell" not in native.existing_rules():
        http_archive(
            name = "rules_haskell",
            strip_prefix = "rules_haskell-%s" % rules_haskell_version,
            urls = ["https://github.com/tweag/rules_haskell/archive/%s.tar.gz" % rules_haskell_version],
            patches = [
                # Update and remove this patch once this is upstreamed.
                # See https://github.com/tweag/rules_haskell/pull/1281
                "@com_github_digital_asset_daml//bazel_tools:haskell-strict-source-names.patch",
                # The fake libs issue should be fixed in upstream rules_haskell
                # or GHC. Remove this patch once that's available.
                "@com_github_digital_asset_daml//bazel_tools:haskell-windows-remove-fake-libs.patch",
                # This is a daml specific patch and not upstreamable.
                "@com_github_digital_asset_daml//bazel_tools:haskell-windows-extra-libraries.patch",
                # This fixes a ghc-lib specific build issue and is not upstreamable.
                # This might also be fixed by using `stack_snapshot` in the future.
                "@com_github_digital_asset_daml//bazel_tools:haskell-no-isystem.patch",
                # This should be made configurable in rules_haskell.
                # Remove this patch once that's available.
                "@com_github_digital_asset_daml//bazel_tools:haskell-opt.patch",
                # This can be upstreamed.
                "@com_github_digital_asset_daml//bazel_tools:haskell-pgmc.patch",
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
            patches = [
                # Remove once https://github.com/tweag/rules_nixpkgs/pull/128
                # has been merged
                "@com_github_digital_asset_daml//bazel_tools:nixpkgs-hermetic-cc-toolchain.patch",
                # On CI and locally we observe occasional segmantation faults
                # of nix. A known issue since Nix 2.2.2 is that HTTP2 support
                # can cause such segmentation faults. Since Nix 2.3.2 it is
                # possible to disable HTTP2 via a command-line flag, which
                # reportedly solves the issue. See
                # https://github.com/NixOS/nix/issues/2733#issuecomment-518324335
                "@com_github_digital_asset_daml//bazel_tools:nixpkgs-disable-http2.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_github_madler_zlib" not in native.existing_rules():
        http_archive(
            name = "com_github_madler_zlib",
            build_file = "@com_github_digital_asset_daml//3rdparty/c:zlib.BUILD",
            strip_prefix = "zlib-{}".format(zlib_version),
            urls = ["https://github.com/madler/zlib/archive/{}.tar.gz".format(zlib_version)],
            sha256 = zlib_sha256,
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
            urls = ["https://github.com/bazelbuild/rules_nodejs/releases/download/{}/rules_nodejs-{}.tar.gz".format(rules_nodejs_version, rules_nodejs_version)],
            sha256 = rules_nodejs_sha256,
            patches = [
                # Work around for https://github.com/bazelbuild/rules_nodejs/issues/1565
                "@com_github_digital_asset_daml//bazel_tools:rules_nodejs_npm_cli_path.patch",
                "@com_github_digital_asset_daml//bazel_tools:rules_nodejs_node_dependency.patch",
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
            sha256 = buildifier_sha256,
            strip_prefix = "buildtools-{}".format(buildifier_version),
            url = "https://github.com/bazelbuild/buildtools/archive/{}.tar.gz".format(buildifier_version),
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

    if "davl-v3" not in native.existing_rules():
        http_archive(
            name = "davl-v3",
            strip_prefix = "davl-{}".format(davl_v3_version),
            urls = ["https://github.com/digital-asset/davl/archive/{}.tar.gz".format(davl_v3_version)],
            sha256 = davl_v3_sha256,
            build_file_content = """
package(default_visibility = ["//visibility:public"])
exports_files(["released/davl-v3.dar"])
            """,
        )

    if "davl" not in native.existing_rules():
        http_archive(
            name = "davl",
            strip_prefix = "davl-{}".format(davl_version),
            urls = ["https://github.com/digital-asset/davl/archive/{}.tar.gz".format(davl_version)],
            sha256 = davl_sha256,
            build_file_content = """
package(default_visibility = ["//visibility:public"])
exports_files(["released/davl-v4.dar", "released/davl-v5.dar", "released/davl-upgrade-v3-v4.dar", "released/davl-upgrade-v4-v5.dar"])
            """,
        )

    if "daml-cheat-sheet" not in native.existing_rules():
        http_archive(
            name = "daml-cheat-sheet",
            strip_prefix = "daml-cheat-sheet-{}".format(daml_cheat_sheet_version),
            urls = ["https://github.com/digital-asset/daml-cheat-sheet/archive/{}.tar.gz".format(daml_cheat_sheet_version)],
            sha256 = daml_cheat_sheet_sha256,
            build_file_content = """
package(default_visibility = ["//visibility:public"])
genrule(
  name = "site",
  srcs = ["_config.yml"] + glob(["**/*"],
          exclude = ["_config.yml", "LICENSE", "WORKSPACE", "BUILD.bazel", "README.md"]),
  outs = ["cheat-sheet.tar.gz"],
  tools = ["@jekyll_nix//:bin/jekyll"],
  cmd = '''
    DIR=$$(dirname $(execpath _config.yml))
    $(execpath @jekyll_nix//:bin/jekyll) build -s $$DIR
    tar hc _site \
        --owner=1000 \
        --group=1000 \
        --mtime=2000-01-01\ 00:00Z \
        --no-acls \
        --no-xattrs \
        --no-selinux \
        --sort=name \
        | gzip -n > $(OUTS)
  ''',
)
            """,
        )
