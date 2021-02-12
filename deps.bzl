# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

rules_scala_version = "67a7ac178a73d1d5ff4c2b0663a8eda6dfcbbc56"
rules_scala_sha256 = "95054009fd938ac7ef53a20619f94a5408d8ae74eb5b318cd150a3ecb1a6086f"

rules_haskell_version = "3987c494f7a3b72805855246a844a28880dd0b15"
rules_haskell_sha256 = "7f6e7005cefbce61930e2feca71d96abba9d7a23059ca1bcdc1d13355f5d448f"
rules_nixpkgs_version = "0dd4c8a085b108592b0193ad1e237e2e07f715ac"
rules_nixpkgs_sha256 = "f2073135db911ee94b70da1e2288dd2445976a1b20a1edfe67773b29751f50a9"
buildifier_version = "4.0.0"
buildifier_sha256 = "0d3ca4ed434958dda241fb129f77bd5ef0ce246250feed2d5a5470c6f29a77fa"
zlib_version = "1.2.11"
zlib_sha256 = "629380c90a77b964d896ed37163f5c3a34f6e6d897311f1df2a7016355c45eff"
rules_nodejs_version = "2.3.1"
rules_nodejs_sha256 = "121f17d8b421ce72f3376431c3461cd66bfe14de49059edc7bb008d5aebd16be"
rules_jvm_external_version = "3.3"
rules_jvm_external_sha256 = "d85951a92c0908c80bd8551002d66cb23c3434409c814179c0ff026b53544dab"
rules_go_version = "0.23.6"
rules_go_sha256 = "8663604808d2738dc615a2c3eb70eba54a9a982089dd09f6ffe5d0e75771bc4f"
rules_bazel_common_version = "9e3880428c1837db9fb13335ed390b7e33e346a7"
rules_bazel_common_sha256 = "48a209fed9575c9d108eaf11fb77f7fe6178a90135e4d60cac6f70c2603aa53a"

# Recent davl.
davl_version = "f2d7480d118f32626533d6a150a8ee7552cc0222"  # 2020-03-23, "Deploy upgrade to SDK 0.13.56-snapshot.20200318",https://github.com/digital-asset/davl/pull/233/commits.
davl_sha256 = "3e8ae2a05724093e33b7f0363381e81a7e8e9655ccb3aa47ad540ea87e814321"

# Pinned davl relied on by damlc packaging tests.
davl_v3_version = "51d3977be2ab22f7f4434fd4692ca2e17a7cce23"
davl_v3_sha256 = "e8e76e21b50fb3adab36df26045b1e8c3ee12814abc60f137d39b864d2eae166"

# daml cheat sheet
daml_cheat_sheet_version = "9d44b550de6c5d23096116da37ccb2fb4a90f7af"  # 2020-06-30
daml_cheat_sheet_sha256 = "6429b73e33a7a937048c3d1316182bff0cb34b6aed30e2915875da698ee4d5c9"

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
                # This should be made configurable in rules_haskell.
                # Remove this patch once that's available.
                "@com_github_digital_asset_daml//bazel_tools:haskell-opt.patch",
                # Can be removed once https://github.com/tweag/rules_haskell/pull/1464 is merged.
                "@com_github_digital_asset_daml//bazel_tools:haskell-cc-wrapper-windows.patch",
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
                # On CI and locally we observe occasional segmantation faults
                # of nix. A known issue since Nix 2.2.2 is that HTTP2 support
                # can cause such segmentation faults. Since Nix 2.3.2 it is
                # possible to disable HTTP2 via a command-line flag, which
                # reportedly solves the issue. See
                # https://github.com/NixOS/nix/issues/2733#issuecomment-518324335
                "@com_github_digital_asset_daml//bazel_tools:nixpkgs-disable-http2.patch",
                # Already upstreamed to rules-nixpkgs. Remove on next upgrade.
                "@com_github_digital_asset_daml//bazel_tools:rules-nixpkgs-llvm-cov.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_github_madler_zlib" not in native.existing_rules():
        http_archive(
            name = "com_github_madler_zlib",
            build_file = "@com_github_digital_asset_daml//3rdparty/c:zlib.BUILD",
            strip_prefix = "zlib-{}".format(zlib_version),
            urls = ["https://github.com/madler/zlib/archive/v{}.tar.gz".format(zlib_version)],
            sha256 = zlib_sha256,
        )

    if "io_bazel_rules_go" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_go",
            urls = [
                "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v{version}/rules_go-v{version}.tar.gz".format(version = rules_go_version),
                "https://github.com/bazelbuild/rules_go/releases/download/v{version}/rules_go-v{version}.tar.gz".format(version = rules_go_version),
            ],
            sha256 = rules_go_sha256,
        )

    if "rules_jvm_external" not in native.existing_rules():
        http_archive(
            name = "rules_jvm_external",
            strip_prefix = "rules_jvm_external-{}".format(rules_jvm_external_version),
            sha256 = rules_jvm_external_sha256,
            url = "https://github.com/bazelbuild/rules_jvm_external/archive/{}.zip".format(rules_jvm_external_version),
        )

    if "io_bazel_rules_scala" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_scala",
            url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip" % rules_scala_version,
            type = "zip",
            strip_prefix = "rules_scala-%s" % rules_scala_version,
            sha256 = rules_scala_sha256,
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:scala-escape-jvmflags.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_google_protobuf" not in native.existing_rules():
        http_archive(
            name = "com_google_protobuf",
            sha256 = "bf0e5070b4b99240183b29df78155eee335885e53a8af8683964579c214ad301",
            # changing this version needs to be in sync with protobuf-java and grpc dependencies in bazel-java-bdeps.bzl
            strip_prefix = "protobuf-3.14.0",
            urls = ["https://github.com/google/protobuf/archive/v3.14.0.zip"],
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:protobuf-win32.patch",
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

    if "upb" not in native.existing_rules():
        # upb is a dependency of com_github_grpc_grpc.
        # It is usually pulled in automatically by grpc_deps(), but depend on it explicitly to patch it.
        # This http_archive can be removed when we no longer need to patch upb.
        http_archive(
            name = "upb",
            sha256 = "c0b97bf91dfea7e8d7579c24e2ecdd02d10b00f3c5defc3dce23d95100d0e664",
            strip_prefix = "upb-60607da72e89ba0c84c84054d2e562d8b6b61177",
            urls = [
                "https://storage.googleapis.com/grpc-bazel-mirror/github.com/protocolbuffers/upb/archive/60607da72e89ba0c84c84054d2e562d8b6b61177.tar.gz",
                "https://github.com/protocolbuffers/upb/archive/60607da72e89ba0c84c84054d2e562d8b6b61177.tar.gz",
            ],
        )

    if "com_github_grpc_grpc" not in native.existing_rules():
        http_archive(
            name = "com_github_grpc_grpc",
            strip_prefix = "grpc-ea22dd6f1b610a37a44be3e9050addbe8a1cb59d",
            urls = ["https://github.com/grpc/grpc/archive/ea22dd6f1b610a37a44be3e9050addbe8a1cb59d.tar.gz"],
            sha256 = "c89c54c5fb9b4b4b0b7ecfa7cd6bbbdecd060bac81ba35b9ee997c1342934b16",
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:grpc-bazel-mingw.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_google_absl" not in native.existing_rules():
        http_archive(
            name = "com_google_absl",
            sha256 = "3d74cdc98b42fd4257d91f652575206de195e2c824fcd8d6e6d227f85cb143ef",
            strip_prefix = "abseil-cpp-0f3bb466b868b523cf1dc9b2aaaed65c77b28862",
            urls = [
                "https://storage.googleapis.com/grpc-bazel-mirror/github.com/abseil/abseil-cpp/archive/0f3bb466b868b523cf1dc9b2aaaed65c77b28862.tar.gz",
                "https://github.com/abseil/abseil-cpp/archive/0f3bb466b868b523cf1dc9b2aaaed65c77b28862.tar.gz",
            ],
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:absl-mingw.patch",
            ],
            patch_args = ["-p1"],
        )

    if "io_grpc_grpc_java" not in native.existing_rules():
        http_archive(
            name = "io_grpc_grpc_java",
            strip_prefix = "grpc-java-1.35.0",
            urls = ["https://github.com/grpc/grpc-java/archive/v1.35.0.tar.gz"],
            sha256 = "537d01bdc5ae2bdb267853a75578d671db3075b33e3a00a93f5a572191d3a7b3",
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

    if "com_github_bazelbuild_remote_apis" not in native.existing_rules():
        http_archive(
            name = "com_github_bazelbuild_remote_apis",
            strip_prefix = "remote-apis-2.0.0",
            urls = ["https://github.com/bazelbuild/remote-apis/archive/v2.0.0.tar.gz"],
            sha256 = "79204ed1fa385c03b5235f65b25ced6ac51cf4b00e45e1157beca6a28bdb8043",
            patches = ["@com_github_digital_asset_daml//:bazel_tools/remote_apis_no_services.patch"],
            patch_args = ["-p1"],
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
            sha256 = rules_bazel_common_sha256,
            strip_prefix = "bazel-common-{}".format(rules_bazel_common_version),
            urls = ["https://github.com/google/bazel-common/archive/{}.zip".format(rules_bazel_common_version)],
        )

    maybe(
        http_archive,
        name = "rules_pkg",
        urls = [
            "https://github.com/bazelbuild/rules_pkg/releases/download/0.2.6-1/rules_pkg-0.2.6.tar.gz",
            "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.2.6/rules_pkg-0.2.6.tar.gz",
        ],
        sha256 = "aeca78988341a2ee1ba097641056d168320ecc51372ef7ff8e64b139516a4937",
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
    tar hc _site \\\\
        --owner=1000 \\\\
        --group=1000 \\\\
        --mtime=2000-01-01\\\\ 00:00Z \\\\
        --no-acls \\\\
        --no-xattrs \\\\
        --no-selinux \\\\
        --sort=name \\\\
        | gzip -n > $(OUTS)
  ''',
)
            """,
        )
