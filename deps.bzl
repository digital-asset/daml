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
load("//:canton_dep.bzl", "canton")

rules_scala_version = "17791a18aa966cdf2babb004822e6c70a7decc76"
rules_scala_sha256 = "6899cddf7407d09266dddcf6faf9f2a8b414de5e2b35ef8b294418f559172f28"

rules_haskell_version = "d3caf0cc94a8dc6af682da42d3b89ef7e85cb987"
rules_haskell_sha256 = "16f6ba4997fa4847d7dc26db2c08f71087b591e53ac8c334b1e7f62f6cf3a5da"
rules_haskell_patches = [
    # This is a daml specific patch and not upstreamable.
    "@com_github_digital_asset_daml//bazel_tools:haskell-windows-extra-libraries.patch",
    # This should be made configurable in rules_haskell.
    # Remove this patch once that's available.
    "@com_github_digital_asset_daml//bazel_tools:haskell-opt.patch",
]
rules_nixpkgs_version = "210d30a81cedde04b4281fd163428722278fddfb"
rules_nixpkgs_sha256 = "61b24e273821a15146f9ae7577e64b53f6aa332d5a7056abe8221ae2c346fdbd"
rules_nixpkgs_patches = [
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
rules_bazel_common_sha256 = "5290e0c8e0b7639f20b70f8d0046b50ad340cb55a4733545f6ec8f43af8727fe"

# Recent davl.
davl_version = "f2d7480d118f32626533d6a150a8ee7552cc0222"  # 2020-03-23, "Deploy upgrade to SDK 0.13.56-snapshot.20200318",https://github.com/digital-asset/davl/pull/233/commits.
davl_sha256 = "3e8ae2a05724093e33b7f0363381e81a7e8e9655ccb3aa47ad540ea87e814321"

# Pinned davl relied on by damlc packaging tests.
davl_v3_version = "51d3977be2ab22f7f4434fd4692ca2e17a7cce23"
davl_v3_sha256 = "e8e76e21b50fb3adab36df26045b1e8c3ee12814abc60f137d39b864d2eae166"

# daml cheat sheet
daml_cheat_sheet_version = "baa4d23a7f4e6365244c204b4e934737c32832be"  # 2022-11-09
daml_cheat_sheet_sha256 = "d0ded22423e336d17cd3e788f5f9b6a652fbf720fd729a000c8a31eb2009537b"

platforms_version = "0.0.4"
platforms_sha256 = "2697e95e085c6e1f970637d178e9dfa1231dca3a099d584ff85a7cb9c0af3826"

rules_sh_version = "f02af9ac549d2a7246a9ee12eb17d113aa218d90"
rules_sh_sha256 = "9bf2a139af12e290a02411b993007ea5f8dd7cad5d0fe26741df6ef3aaa984bc"

def daml_deps():
    if "platforms" not in native.existing_rules():
        http_archive(
            name = "platforms",
            sha256 = platforms_sha256,
            strip_prefix = "platforms-{}".format(platforms_version),
            urls = ["https://github.com/bazelbuild/platforms/archive/{version}.tar.gz".format(version = platforms_version)],
        )

    if "rules_sh" not in native.existing_rules():
        http_archive(
            name = "rules_sh",
            strip_prefix = "rules_sh-%s" % rules_sh_version,
            urls = ["https://github.com/tweag/rules_sh/archive/%s.tar.gz" % rules_sh_version],
            sha256 = rules_sh_sha256,
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
        # N.B. rules_nixpkgs was split into separate components, which need to be loaded separately
        #
        # See https://github.com/tweag/rules_nixpkgs/issues/182 for the rational

        strip_prefix = "rules_nixpkgs-%s" % rules_nixpkgs_version

        http_archive(
            name = "io_tweag_rules_nixpkgs",
            strip_prefix = strip_prefix,
            urls = ["https://github.com/tweag/rules_nixpkgs/archive/%s.tar.gz" % rules_nixpkgs_version],
            sha256 = rules_nixpkgs_sha256,
            patches = rules_nixpkgs_patches,
            patch_args = ["-p1"],
        )

        http_archive(
            name = "rules_nixpkgs_core",
            strip_prefix = strip_prefix + "/core",
            urls = ["https://github.com/tweag/rules_nixpkgs/archive/%s.tar.gz" % rules_nixpkgs_version],
            sha256 = rules_nixpkgs_sha256,
            patches = rules_nixpkgs_patches,
            patch_args = ["-p2"],
        )

        for toolchain in ["cc", "java", "python", "go", "rust", "posix"]:
            http_archive(
                name = "rules_nixpkgs_" + toolchain,
                strip_prefix = strip_prefix + "/toolchains/" + toolchain,
                urls = ["https://github.com/tweag/rules_nixpkgs/archive/%s.tar.gz" % rules_nixpkgs_version],
                sha256 = rules_nixpkgs_sha256,
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
            url = "https://github.com/bazelbuild/rules_scala/archive/%s.tar.gz" % rules_scala_version,
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
        http_archive(
            name = "com_github_grpc_grpc",
            strip_prefix = "grpc-1.44.0",
            urls = ["https://github.com/grpc/grpc/archive/v1.44.0.tar.gz"],
            sha256 = "8c05641b9f91cbc92f51cc4a5b3a226788d7a63f20af4ca7aaca50d92cc94a0d",
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:grpc-bazel-mingw.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_google_protobuf" not in native.existing_rules():
        http_archive(
            name = "com_google_protobuf",
            sha256 = "4dd35e788944b7686aac898f77df4e9a54da0ca694b8801bd6b2a9ffc1b3085e",
            strip_prefix = "protobuf-3.19.2",
            urls = [
                "https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.19.2.tar.gz",
            ],
        )

    if "io_grpc_grpc_java" not in native.existing_rules():
        http_archive(
            name = "io_grpc_grpc_java",
            strip_prefix = "grpc-java-1.44.0",
            urls = ["https://github.com/grpc/grpc-java/archive/v1.44.0.tar.gz"],
            sha256 = "e3781bcab2a410a7cd138f13b2e6a643e111575f6811b154c570f4d020e87507",
            patch_args = ["-p1"],
        )

    if "com_github_johnynek_bazel_jar_jar" not in native.existing_rules():
        http_archive(
            name = "com_github_johnynek_bazel_jar_jar",
            sha256 = "64748da73bc82ecbbb2a872722690a3be52c06bb92a1c939136e2852470f308d",
            strip_prefix = "bazel_jar_jar-20dbf71f09b1c1c2a8575a42005a968b38805519",
            urls = ["https://github.com/johnynek/bazel_jar_jar/archive/20dbf71f09b1c1c2a8575a42005a968b38805519.tar.gz"],  # Latest commit SHA as at 2019/02/13
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
            urls = ["https://github.com/google/bazel-common/archive/{}.tar.gz".format(rules_bazel_common_version)],
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
            sha256 = canton["sha"],
            strip_prefix = canton["prefix"],
            urls = [canton["url"]],
        )

    if "freefont" not in native.existing_rules():
        http_archive(
            name = "freefont",
            build_file_content = """
filegroup(
  name = "fonts",
  srcs = glob(["**/*.otf"]),
  visibility = ["//visibility:public"],
)""",
            sha256 = "3a6c51868c71b006c33c4bcde63d90927e6fcca8f51c965b8ad62d021614a860",
            strip_prefix = "freefont-20120503",
            urls = ["http://ftp.gnu.org/gnu/freefont/freefont-otf-20120503.tar.gz"],
        )
