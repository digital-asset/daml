# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

rules_scala_version = "17791a18aa966cdf2babb004822e6c70a7decc76"
rules_scala_sha256 = "a8faef92f59a4f1428ed9a93c7c313a996466a66ad64c119fc49b5c7dea98c59"

rules_haskell_version = "156b091fc1b8d0736ad9f072cdc9abdadacbf57f"
rules_haskell_sha256 = "e3fb9e6da187d0ef79f1e6240325092c011dd1c7db207cd3f3793806642bd2c4"
rules_haskell_patches = [
    # This is a daml specific patch and not upstreamable.
    "@com_github_digital_asset_daml//bazel_tools:haskell-windows-extra-libraries.patch",
    # This should be made configurable in rules_haskell.
    # Remove this patch once that's available.
    "@com_github_digital_asset_daml//bazel_tools:haskell-opt.patch",
]
rules_nixpkgs_version = "81f61c4b5afcf50665b7073f7fce4c1755b4b9a3"
rules_nixpkgs_sha256 = "33fd540d0283cf9956d0a5a640acb1430c81539a84069114beaf9640c96d221a"
rules_nixpkgs_patches = [
    # On CI and locally we observe occasional segmantation faults
    # of nix. A known issue since Nix 2.2.2 is that HTTP2 support
    # can cause such segmentation faults. Since Nix 2.3.2 it is
    # possible to disable HTTP2 via a command-line flag, which
    # reportedly solves the issue. See
    # https://github.com/NixOS/nix/issues/2733#issuecomment-518324335
    "@com_github_digital_asset_daml//bazel_tools:nixpkgs-disable-http2.patch",
]

buildifier_version = "4.0.0"
buildifier_sha256 = "0d3ca4ed434958dda241fb129f77bd5ef0ce246250feed2d5a5470c6f29a77fa"
zlib_version = "1.2.11"
zlib_sha256 = "629380c90a77b964d896ed37163f5c3a34f6e6d897311f1df2a7016355c45eff"
rules_nodejs_version = "4.6.1"
rules_nodejs_sha256 = "d63ecec7192394f5cc4ad95a115f8a6c9de55c60d56c1f08da79c306355e4654"
rules_jvm_external_version = "3.3"
rules_jvm_external_sha256 = "d85951a92c0908c80bd8551002d66cb23c3434409c814179c0ff026b53544dab"
rules_go_version = "0.29.0"
rules_go_sha256 = "2b1641428dff9018f9e85c0384f03ec6c10660d935b750e3fa1492a281a53b0f"
bazel_gazelle_version = "67a3e22af6547f43bb9b8e4dd0bad5f354ad4e60"
bazel_gazelle_sha256 = "c71b12d890d1e299e012bfa6f08dc3d9e57281a0955dc28a1e9c16769d556203"
rules_bazel_common_version = "9e3880428c1837db9fb13335ed390b7e33e346a7"
rules_bazel_common_sha256 = "48a209fed9575c9d108eaf11fb77f7fe6178a90135e4d60cac6f70c2603aa53a"

# Recent davl.
davl_version = "f2d7480d118f32626533d6a150a8ee7552cc0222"  # 2020-03-23, "Deploy upgrade to SDK 0.13.56-snapshot.20200318",https://github.com/digital-asset/davl/pull/233/commits.
davl_sha256 = "3e8ae2a05724093e33b7f0363381e81a7e8e9655ccb3aa47ad540ea87e814321"

# Pinned davl relied on by damlc packaging tests.
davl_v3_version = "51d3977be2ab22f7f4434fd4692ca2e17a7cce23"
davl_v3_sha256 = "e8e76e21b50fb3adab36df26045b1e8c3ee12814abc60f137d39b864d2eae166"

# daml cheat sheet
daml_cheat_sheet_version = "2710b8df28d97253b5487a68feb2d1452d29fc54"  # 2021-09-17
daml_cheat_sheet_sha256 = "eb022565a929a69d869f0ab0497f02d1a3eacb4dafdafa076a82ecbe7c401315"

platforms_version = "0.0.3"
platforms_sha256 = "15b66b5219c03f9e8db34c1ac89c458bb94bfe055186e5505d5c6f09cb38307f"

def daml_deps():
    if "platforms" not in native.existing_rules():
        http_archive(
            name = "platforms",
            sha256 = platforms_sha256,
            strip_prefix = "platforms-{}".format(platforms_version),
            urls = ["https://github.com/bazelbuild/platforms/archive/{version}.tar.gz".format(version = platforms_version)],
        )

    if "rules_haskell" not in native.existing_rules():
        http_archive(
            name = "rules_haskell",
            strip_prefix = "rules_haskell-%s" % rules_haskell_version,
            urls = ["https://github.com/tweag/rules_haskell/archive/%s.tar.gz" % rules_haskell_version],
            patches = rules_haskell_patches,
            patch_args = ["-p1"],
            sha256 = rules_haskell_sha256,
        )

    if "io_tweag_rules_nixpkgs" not in native.existing_rules():
        http_archive(
            name = "io_tweag_rules_nixpkgs",
            strip_prefix = "rules_nixpkgs-%s" % rules_nixpkgs_version,
            urls = ["https://github.com/tweag/rules_nixpkgs/archive/%s.tar.gz" % rules_nixpkgs_version],
            sha256 = rules_nixpkgs_sha256,
            patches = rules_nixpkgs_patches,
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

    if "go_googleapis" not in native.existing_rules():
        # The Haskell gRPC bindings require access to the status.proto source file.
        # This import of go_googleapis is taken from rules_go and extended with the status.proto patch.
        http_archive(
            name = "go_googleapis",
            # master, as of 2021-10-06
            urls = [
                "https://mirror.bazel.build/github.com/googleapis/googleapis/archive/409e134ffaacc243052b08e6fb8e2d458014ed37.zip",
                "https://github.com/googleapis/googleapis/archive/409e134ffaacc243052b08e6fb8e2d458014ed37.zip",
            ],
            sha256 = "a85c6a00e9cf0f004992ebea1d10688e3beea9f8e1a5a04ee53f367e72ee85af",
            strip_prefix = "googleapis-409e134ffaacc243052b08e6fb8e2d458014ed37",
            patches = [
                # releaser:patch-cmd find . -name BUILD.bazel -delete
                "@io_bazel_rules_go//third_party:go_googleapis-deletebuild.patch",
                # set gazelle directives; change workspace name
                "@io_bazel_rules_go//third_party:go_googleapis-directives.patch",
                # releaser:patch-cmd gazelle -repo_root .
                "@io_bazel_rules_go//third_party:go_googleapis-gazelle.patch",
                # The Haskell gRPC bindings require access to the status.proto source file.
                "//bazel_tools:googleapis-status-proto.patch",
            ],
            patch_args = ["-E", "-p1"],
        )

    if "io_bazel_rules_go" not in native.existing_rules():
        http_archive(
            name = "io_bazel_rules_go",
            urls = [
                "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v{version}/rules_go-v{version}.zip".format(version = rules_go_version),
                "https://github.com/bazelbuild/rules_go/releases/download/v{version}/rules_go-v{version}.zip".format(version = rules_go_version),
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

    if "bazel_gazelle" not in native.existing_rules():
        http_archive(
            name = "bazel_gazelle",
            urls = [
                "https://github.com/bazelbuild/bazel-gazelle/archive/{version}/bazel-gazelle-{version}.tar.gz".format(version = bazel_gazelle_version),
            ],
            strip_prefix = "bazel-gazelle-{version}".format(version = bazel_gazelle_version),
            sha256 = bazel_gazelle_sha256,
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
            strip_prefix = "grpc-1.43.0",
            urls = ["https://github.com/grpc/grpc/archive/v1.43.0.tar.gz"],
            sha256 = "9647220c699cea4dafa92ec0917c25c7812be51a18143af047e20f3fb05adddc",
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:grpc-bazel-mingw.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_google_protobuf" not in native.existing_rules():
        http_archive(
            name = "com_google_protobuf",
            sha256 = "c6003e1d2e7fefa78a3039f19f383b4f3a61e81be8c19356f85b6461998ad3db",
            strip_prefix = "protobuf-3.17.3",
            urls = [
                "https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.17.3.tar.gz",
            ],
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

    if "canton" not in native.existing_rules():
        http_archive(
            name = "canton",
            build_file_content = """
package(default_visibility = ["//visibility:public"])

java_import(
    name = "lib",
    jars = glob(["lib/**/*.jar"]),
)
        """,
            sha256 = "b21a198820586e5a02afdd9814d374e2d7e1d57689e9f7859d0d3ddb6804f62f",
            strip_prefix = "canton-community-1.0.0-SNAPSHOT",
            urls = ["https://www.canton.io/releases/canton-community-20220127.tar.gz"],
        )
