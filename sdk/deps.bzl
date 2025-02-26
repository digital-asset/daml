# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
load("//:daml_finance_dep.bzl", "quickstart")

rules_scala_version = "17791a18aa966cdf2babb004822e6c70a7decc76"
rules_scala_sha256 = "6899cddf7407d09266dddcf6faf9f2a8b414de5e2b35ef8b294418f559172f28"

rules_haskell_version = "a361943682c2f312de4afff0e4438259bfd8119c"  # 1.0
rules_haskell_sha256 = "f2b04c7dd03f8adacc44f44e6232cd086c02395a03236228d8f09335a931ab9c"
rules_haskell_patches = [
    # This is a daml specific patch and not upstreamable.
    "@com_github_digital_asset_daml//bazel_tools:haskell-windows-extra-libraries.patch",
    # TODO This must have been upstream
    # This should be made configurable in rules_haskell.
    # Remove this patch once that's available.
    # "@com_github_digital_asset_daml//bazel_tools:haskell-opt.patch",
]
rules_nixpkgs_version = "ff70910af286a19dbcc109fe36f2e3cb59da78ff"  # 0.10.0
rules_nixpkgs_sha256 = "c269fa7f70069180e31aa5982313f5a26838db2a6e5f2e9ecbd5941c8c6ceed0"
rules_nixpkgs_patches = [
]

rules_nixpkgs_toolchain_patches = {
    "java": [],
    "cc": [],
    "python": [],
    "go": [],
    "rust": [],
    "posix": [],
}

buildifier_version = "b163fcf72b7def638f364ed129c9b28032c1d39b"
buildifier_sha256 = "c2399161fa569f7c815f8e27634035557a2e07a557996df579412ac73bf52c23"

zlib_version = "1.2.11"
zlib_sha256 = "629380c90a77b964d896ed37163f5c3a34f6e6d897311f1df2a7016355c45eff"
rules_nodejs_version = "5.8.5"
rules_nodejs_sha256 = "a1295b168f183218bc88117cf00674bcd102498f294086ff58318f830dd9d9d1"
rules_jvm_external_version = "4.4.2"
rules_jvm_external_sha256 = "735602f50813eb2ea93ca3f5e43b1959bd80b213b836a07a62a29d757670b77b"
rules_go_version = "0.40.0"
rules_go_sha256 = "bfc5ce70b9d1634ae54f4e7b495657a18a04e0d596785f672d35d5f505ab491a"
bazel_gazelle_version = "0.42.0"
bazel_gazelle_sha256 = "b9bd60e869ac69b083e818a5e4c6803fea17442576d111a2d05f7a314f34b466"
rules_bazel_common_version = "9e3880428c1837db9fb13335ed390b7e33e346a7"
rules_bazel_common_sha256 = "5290e0c8e0b7639f20b70f8d0046b50ad340cb55a4733545f6ec8f43af8727fe"
go_googleapis_version = "83c3605afb5a39952bf0a0809875d41cf2a558ca"
go_googleapis_sha256 = "ba694861340e792fd31cb77274eacaf6e4ca8bda97707898f41d8bebfd8a4984"
rules_pkg_version = "1.0.1"
rules_pkg_sha256 = "d20c951960ed77cb7b341c2a59488534e494d5ad1d30c4818c736d57772a9fef"

# Recent davl.
davl_version = "f2d7480d118f32626533d6a150a8ee7552cc0222"  # 2020-03-23, "Deploy upgrade to SDK 0.13.56-snapshot.20200318",https://github.com/digital-asset/davl/pull/233/commits.
davl_sha256 = "3e8ae2a05724093e33b7f0363381e81a7e8e9655ccb3aa47ad540ea87e814321"

# Pinned davl relied on by damlc packaging tests.
davl_v3_version = "51d3977be2ab22f7f4434fd4692ca2e17a7cce23"
davl_v3_sha256 = "e8e76e21b50fb3adab36df26045b1e8c3ee12814abc60f137d39b864d2eae166"

# daml cheat sheet
daml_cheat_sheet_version = "e65f725ef3b19c9ffdee0baa3eee623cbb115024"  # 2022-11-28
daml_cheat_sheet_sha256 = "e7ef4def3b7c6bada4235603b314ab0b1874bb949cd3c8d974d5443337e89a8b"

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

        http_archive(
            name = "rules_nixpkgs_nodejs",
            strip_prefix = strip_prefix + "/toolchains/nodejs",
            urls = ["https://github.com/tweag/rules_nixpkgs/archive/%s.tar.gz" % rules_nixpkgs_version],
            sha256 = rules_nixpkgs_sha256,
        )

        for toolchain in ["cc", "java", "python", "go", "rust", "posix"]:
            http_archive(
                name = "rules_nixpkgs_" + toolchain,
                strip_prefix = strip_prefix + "/toolchains/" + toolchain,
                urls = ["https://github.com/tweag/rules_nixpkgs/archive/%s.tar.gz" % rules_nixpkgs_version],
                sha256 = rules_nixpkgs_sha256,
                patches = rules_nixpkgs_toolchain_patches[toolchain],
                patch_args = ["-p3"],
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
            # We must use the same version as rules_go
            # master, as of 2022-12-05
            urls = [
                "https://mirror.bazel.build/github.com/googleapis/googleapis/archive/{}.zip".format(go_googleapis_version),
                "https://github.com/googleapis/googleapis/archive/{}.zip".format(go_googleapis_version),
            ],
            sha256 = go_googleapis_sha256,
            strip_prefix = "googleapis-{}".format(go_googleapis_version),
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
                "https://github.com/bazelbuild/bazel-gazelle/archive/{version}/bazel-gazelle-v{version}.tar.gz".format(version = bazel_gazelle_version),
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
              "@com_github_digital_asset_daml//bazel_tools:rules_nodejs_hotfix.patch",
            ],
            # # TODO Those must be backported? or can we jump directly to rules_js?
            #  # Work around for https://github.com/bazelbuild/rules_nodejs/issues/1565
            #  "@com_github_digital_asset_daml//bazel_tools:rules_nodejs_npm_cli_path.patch",
            # #     "@com_github_digital_asset_daml//bazel_tools:rules_nodejs_node_dependency.patch",
            patch_args = ["-p1"],
        )

    if "com_google_absl" not in native.existing_rules():
        http_archive(
            name = "com_google_absl",
            sha256 = "59d2976af9d6ecf001a81a35749a6e551a335b949d34918cfade07737b9d93c5",
            strip_prefix = "abseil-cpp-20230802.0",
            urls = [
                "https://storage.googleapis.com/grpc-bazel-mirror/github.com/abseil/abseil-cpp/archive/20230802.0.tar.gz",
                "https://github.com/abseil/abseil-cpp/archive/20230802.0.tar.gz",
            ],
            patches = [
                "@com_github_digital_asset_daml//bazel_tools:absl-mingw-win-version.patch",
                "@com_github_digital_asset_daml//bazel_tools:absl-mingw-compiler-name.patch",
            ],
            patch_args = ["-p1"],
        )

    if "com_github_grpc_grpc" not in native.existing_rules():
        http_archive(
            name = "com_github_grpc_grpc",
            strip_prefix = "grpc-1.59.5",
            urls = ["https://github.com/grpc/grpc/archive/v1.59.5.tar.gz"],
            sha256 = "ad295f118a84d87096fe3eb416ef446d75d44c988eadccebc650656eb9383b3d",
        )

    if "com_google_protobuf" not in native.existing_rules():
        http_archive(
            name = "com_google_protobuf",
            sha256 = "a1fa6ffa97c09d1efe0344e4352a6dbc51cebaafbdf20bcb6405147a0158c406",
            strip_prefix = "protobuf-3.24.4",
            urls = [
                "https://github.com/protocolbuffers/protobuf/archive/refs/tags/v3.24.4.tar.gz",
            ],
        )

    if "io_grpc_grpc_java" not in native.existing_rules():
        http_archive(
            name = "io_grpc_grpc_java",
            strip_prefix = "grpc-java-1.60.0",
            urls = ["https://github.com/grpc/grpc-java/archive/v1.60.0.tar.gz"],
            sha256 = "02c9a7f9400d4e29c7e55667851083a9f695935081787079a834da312129bf97",
        )

    if "com_github_johnynek_bazel_jar_jar" not in native.existing_rules():
        http_archive(
            name = "com_github_johnynek_bazel_jar_jar",
            sha256 = "a9d2ca9a2e9014f8d63dcbe9091bcb9f2d2929b3b7d16836c6225e98f9ca54df",
            strip_prefix = "bazel_jar_jar-0.1.5",
            url = "https://github.com/bazeltools/bazel_jar_jar/releases/download/v0.1.5/bazel_jar_jar-v0.1.5.tar.gz",
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

    if not native.existing_rule("rules_pkg"):
        http_archive(
            name = "rules_pkg",
            urls = [
                "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/{}/rules_pkg-{}.tar.gz".format(rules_pkg_version, rules_pkg_version),
                "https://github.com/bazelbuild/rules_pkg/releases/download/{}/rules_pkg-{}.tar.gz".format(rules_pkg_version, rules_pkg_version),
            ],
            sha256 = rules_pkg_sha256,
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
            urls = ["https://storage.googleapis.com/daml-binaries/build-inputs/freefont-otf-20120503.tar.gz"],
        )

    if "daml-finance" not in native.existing_rules():
        http_archive(
            name = "daml-finance",
            strip_prefix = "daml-finance-{}".format(quickstart["version"]),
            urls = ["https://github.com/digital-asset/daml-finance/archive/{}.tar.gz".format(quickstart["version"])],
            sha256 = quickstart["sha256"],
            build_file_content = """
package(default_visibility = ["//visibility:public"])
genrule(
    name = "quickstart",
    srcs = glob(["docs/code-samples/getting-started/**/*"]
            , exclude = ["docs/code-samples/getting-started/daml.yaml", "docs/code-samples/getting-started/NO_AUTO_COPYRIGHT"]),
    outs = ["daml-finance-quickstart.tar.gz"],
    cmd = '''
        tar czhf $(OUTS) \\\\
            --transform 's|^.*docs/code-samples/getting-started/||' \\\\
            --owner=1000 \\\\
            --group=1000 \\\\
            --mtime=2000-01-01\\\\ 00:00Z \\\\
            --no-acls \\\\
            --no-xattrs \\\\
            --no-selinux \\\\
            --sort=name \\\\
            $(SRCS)
    ''',
)
genrule(
    name = "lifecycling",
    srcs = glob(["docs/code-samples/lifecycling/**/*"]
            , exclude = ["docs/code-samples/lifecycling/daml.yaml", "docs/code-samples/lifecycling/NO_AUTO_COPYRIGHT"]),
    outs = ["daml-finance-lifecycling.tar.gz"],
    cmd = '''
        tar czhf $(OUTS) \\\\
            --transform 's|^.*docs/code-samples/lifecycling/||' \\\\
            --owner=1000 \\\\
            --group=1000 \\\\
            --mtime=2000-01-01\\\\ 00:00Z \\\\
            --no-acls \\\\
            --no-xattrs \\\\
            --no-selinux \\\\
            --sort=name \\\\
            $(SRCS)
    ''',
)
genrule(
    name = "settlement",
    srcs = glob(["docs/code-samples/settlement/**/*"]
            , exclude = ["docs/code-samples/settlement/daml.yaml", "docs/code-samples/settlement/NO_AUTO_COPYRIGHT"]),
    outs = ["daml-finance-settlement.tar.gz"],
    cmd = '''
        tar czhf $(OUTS) \\\\
            --transform 's|^.*docs/code-samples/settlement/||' \\\\
            --owner=1000 \\\\
            --group=1000 \\\\
            --mtime=2000-01-01\\\\ 00:00Z \\\\
            --no-acls \\\\
            --no-xattrs \\\\
            --no-selinux \\\\
            --sort=name \\\\
            $(SRCS)
    ''',
)
genrule(
    name = "upgrades",
    srcs = glob(["docs/code-samples/upgrades/**/*"]
            , exclude = ["docs/code-samples/upgrades/daml.yaml", "docs/code-samples/upgrades/NO_AUTO_COPYRIGHT"]),
    outs = ["daml-finance-upgrades.tar.gz"],
    cmd = '''
        tar czhf $(OUTS) \\\\
            --transform 's|^.*docs/code-samples/upgrades/||' \\\\
            --owner=1000 \\\\
            --group=1000 \\\\
            --mtime=2000-01-01\\\\ 00:00Z \\\\
            --no-acls \\\\
            --no-xattrs \\\\
            --no-selinux \\\\
            --sort=name \\\\
            $(SRCS)
    ''',
)
genrule(
    name = "payoff-modeling",
    srcs = glob(["docs/code-samples/payoff-modeling/**/*"]
            , exclude = ["docs/code-samples/payoff-modeling/daml.yaml", "docs/code-samples/payoff-modeling/NO_AUTO_COPYRIGHT"]),
    outs = ["daml-finance-payoff-modeling.tar.gz"],
    cmd = '''
        tar czhf $(OUTS) \\\\
            --transform 's|^.*docs/code-samples/payoff-modeling/||' \\\\
            --owner=1000 \\\\
            --group=1000 \\\\
            --mtime=2000-01-01\\\\ 00:00Z \\\\
            --no-acls \\\\
            --no-xattrs \\\\
            --no-selinux \\\\
            --sort=name \\\\
            $(SRCS)
    ''',
)
            """,
        )
