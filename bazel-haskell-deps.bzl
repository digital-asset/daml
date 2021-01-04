# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Defines external Haskell dependencies.
#
# Add Stackage dependencies to the `packages` attribute of the `@stackage`
# `stack_snapshot` in the very bottom of this file. If a package or version is
# not available on Stackage, add it to the custom stack snapshot in
# `stack-snapshot.yaml`. If a library requires patching, then add it as an
# `http_archive` and add it to the `vendored_packages` attribute of
# `stack_snapshot`. Executables are defined in an `http_archive` using
# `haskell_cabal_binary`.

load("@bazel_skylib//lib:dicts.bzl", "dicts")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@os_info//:os_info.bzl", "is_windows")
load("@dadew//:dadew.bzl", "dadew_tool_home")
load("@rules_haskell//haskell:cabal.bzl", "stack_snapshot")

GHCIDE_REV = "8e4f52892d88d23259547b03bb096b46f28c0afc"
GHCIDE_SHA256 = "3fa3e23d760f1bdfb2a707d2593ea3eaedba748abfd649e8917c585780fa91db"
GHCIDE_VERSION = "0.1.0"
JS_JQUERY_VERSION = "3.3.1"
JS_DGTABLE_VERSION = "0.5.2"
JS_FLOT_VERSION = "0.8.3"
SHAKE_VERSION = "0.18.5"
ZIP_VERSION = "1.5.0"

def daml_haskell_deps():
    """Load all Haskell dependencies of the DAML repository."""

    # XXX: We do not have access to an integer-simple version of GHC on Windows.
    # For the time being we build with GMP. See https://github.com/digital-asset/daml/issues/106
    use_integer_simple = not is_windows

    #
    # Executables
    #

    http_archive(
        name = "proto3_suite",
        build_file_content = """
# XXX: haskell_cabal_binary inexplicably fails with
#   realgcc.exe: error: CreateProcess: No such file or directory
# So we use haskell_binary instead.
load("@rules_haskell//haskell:defs.bzl", "haskell_binary")
haskell_binary(
    name = "compile-proto-file",
    srcs = ["tools/compile-proto-file/Main.hs"],
    compiler_flags = ["-w", "-optF=-w"],
    deps = [
        "@stackage//:base",
        "@stackage//:optparse-applicative",
        "@stackage//:proto3-suite",
        "@stackage//:system-filepath",
        "@stackage//:text",
        "@stackage//:turtle",
    ],
    visibility = ["//visibility:public"],
)
""",
        sha256 = "216fb8b5d92afc9df70512da2331e098e926239efd55e770802079c2a13bad5e",
        strip_prefix = "proto3-suite-0.4.0.0",
        urls = ["http://hackage.haskell.org/package/proto3-suite-0.4.0.0/proto3-suite-0.4.0.0.tar.gz"],
    )

    #
    # Vendored Libraries
    #

    # ghc-lib based ghcide - injected into `@stackage` and used for DAML IDE.
    http_archive(
        name = "ghcide_ghc_lib",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@rules_haskell//haskell:defs.bzl", "haskell_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "ghcide",
    version = "{version}",
    srcs = glob(["**"]),
    haddock = False,
    flags = packages["ghcide"].flags,
    deps = packages["ghcide"].deps,
    visibility = ["//visibility:public"],
)
haskell_library(
    name = "testing",
    srcs = glob(["test/src/**/*.hs"]),
    src_strip_prefix = "test/src",
    deps = [
        "@stackage//:base",
        "@stackage//:extra",
        "@stackage//:containers",
        "@stackage//:haskell-lsp-types",
        "@stackage//:lens",
        "@stackage//:lsp-test",
        "@stackage//:parser-combinators",
        "@stackage//:tasty-hunit",
        "@stackage//:text",
    ],
    compiler_flags = [
       "-XBangPatterns",
       "-XDeriveFunctor",
       "-XDeriveGeneric",
       "-XGeneralizedNewtypeDeriving",
       "-XLambdaCase",
       "-XNamedFieldPuns",
       "-XOverloadedStrings",
       "-XRecordWildCards",
       "-XScopedTypeVariables",
       "-XStandaloneDeriving",
       "-XTupleSections",
       "-XTypeApplications",
       "-XViewPatterns",
    ],
    visibility = ["//visibility:public"],
)
""".format(version = GHCIDE_VERSION),
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-ghcide-binary-q.patch",
            "@com_github_digital_asset_daml//bazel_tools:haskell-ghcide-expose-compat.patch",
        ],
        sha256 = GHCIDE_SHA256,
        strip_prefix = "daml-ghcide-%s" % GHCIDE_REV,
        urls = ["https://github.com/digital-asset/daml-ghcide/archive/%s.tar.gz" % GHCIDE_REV],
    )

    # The Bazel-provided grpc libs cause issues in GHCi so we get them from Nix on Linux and MacOS.
    deps = '[":grpc", ":libgpr"]' if is_windows else '["@grpc_nix//:grpc_lib"]'
    extra_targets = """
fat_cc_library(
  name = "grpc",
  input_lib = "@com_github_grpc_grpc//:grpc",
)
# Cabal requires libgpr next to libgrpc. However, fat_cc_library of grpc
# already contains gpr and providing a second copy would cause duplicate symbol
# errors. Instead, we define an empty dummy libgpr.
genrule(name = "gpr-source", outs = ["gpr.c"], cmd = "touch $(OUTS)")
cc_library(name = "gpr", srcs = [":gpr-source"])
cc_library(name = "libgpr", linkstatic = 1, srcs = [":gpr"])
""" if is_windows else ""

    http_archive(
        name = "grpc_haskell_core",
        build_file_content = """
load("@com_github_digital_asset_daml//bazel_tools:fat_cc_library.bzl", "fat_cc_library")
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "grpc-haskell-core",
    version = "0.0.0.0",
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["grpc-haskell-core"].deps + {deps},
    tools = ["@stackage-exe//c2hs"],
    verbose = False,
    visibility = ["//visibility:public"],
)
{extra_targets}
""".format(deps = deps, extra_targets = extra_targets),
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:grpc-haskell-core-cpp-options.patch",
        ],
        sha256 = "087527ec3841330b5328d123ca410901905d111529956821b724d92c436e6cdf",
        strip_prefix = "grpc-haskell-core-0.0.0.0",
        urls = ["http://hackage.haskell.org/package/grpc-haskell-core-0.0.0.0/grpc-haskell-core-0.0.0.0.tar.gz"],
    )

    # Note (MK)
    # We vendor Shake and its JS dependencies
    # so that we can replace the data-files with file-embed.
    # This is both to workaround bugs in rules_haskell where data-files
    # are not propagated correctly to non-cabal targets and to
    # make sure that they are shipped in the SDK.

    http_archive(
        name = "js_jquery",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "js-jquery",
    version = "{version}",
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["js-jquery"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
)
""".format(version = JS_JQUERY_VERSION),
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-js-jquery.patch",
        ],
        sha256 = "e0e0681f0da1130ede4e03a051630ea439c458cb97216cdb01771ebdbe44069b",
        strip_prefix = "js-jquery-{}".format(JS_JQUERY_VERSION),
        urls = ["http://hackage.haskell.org/package/js-jquery-{version}/js-jquery-{version}.tar.gz".format(version = JS_JQUERY_VERSION)],
    )

    http_archive(
        name = "js_dgtable",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "js-dgtable",
    version = "{version}",
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["js-dgtable"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
)
""".format(version = JS_DGTABLE_VERSION),
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-js-dgtable.patch",
        ],
        sha256 = "e28dd65bee8083b17210134e22e01c6349dc33c3b7bd17705973cd014e9f20ac",
        strip_prefix = "js-dgtable-{}".format(JS_DGTABLE_VERSION),
        urls = ["http://hackage.haskell.org/package/js-dgtable-{version}/js-dgtable-{version}.tar.gz".format(version = JS_DGTABLE_VERSION)],
    )

    http_archive(
        name = "js_flot",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "js-flot",
    version = "{version}",
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["js-flot"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
)
""".format(version = JS_FLOT_VERSION),
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-js-flot.patch",
        ],
        sha256 = "1ba2f2a6b8d85da76c41f526c98903cbb107f8642e506c072c1e7e3c20fe5e7a",
        strip_prefix = "js-flot-{}".format(JS_FLOT_VERSION),
        urls = ["http://hackage.haskell.org/package/js-flot-{version}/js-flot-{version}.tar.gz".format(version = JS_FLOT_VERSION)],
    )

    http_archive(
        name = "shake",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "shake",
    version = "{version}",
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["shake"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
    flags = ["embed-files"],
)
""".format(version = SHAKE_VERSION),
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-shake.patch",
        ],
        sha256 = "576ab57f53b8051f67ceeb97bd9abf2e0926f592334a7a1c27c07b36afca240f",
        strip_prefix = "shake-{}".format(SHAKE_VERSION),
        urls = ["http://hackage.haskell.org/package/shake-{version}/shake-{version}.tar.gz".format(version = SHAKE_VERSION)],
    )

    http_archive(
        name = "zip",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "zip",
    version = "{version}",
    srcs = glob(["**"]),
    haddock = False,
    deps = [
        "@stackage//:case-insensitive",
        "@stackage//:cereal",
        "@stackage//:conduit",
        "@stackage//:conduit-extra",
        "@stackage//:digest",
        "@stackage//:dlist",
        "@stackage//:exceptions",
        "@stackage//:monad-control",
        "@stackage//:resourcet",
        "@stackage//:transformers-base",
    ],
    verbose = False,
    visibility = ["//visibility:public"],
    flags = ["disable-bzip2"],
)
""".format(version = ZIP_VERSION),
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-zip.patch",
        ],
        sha256 = "051e891d6a13774f1d06b0251e9a0bf92f05175da8189d936c7d29c317709802",
        strip_prefix = "zip-{}".format(ZIP_VERSION),
        urls = ["http://hackage.haskell.org/package/zip-{version}/zip-{version}.tar.gz".format(version = ZIP_VERSION)],
    )

    #
    # Stack binary
    #

    # On Windows the stack binary is provisioned using dadew.
    if is_windows:
        native.new_local_repository(
            name = "stack_windows",
            build_file_content = """
exports_files(["stack.exe"], visibility = ["//visibility:public"])
""",
            path = dadew_tool_home("stack"),
        )

    #
    # Stack Snapshots
    #

    stack_snapshot(
        name = "stackage",
        extra_deps = {
            "digest": ["@com_github_madler_zlib//:libz"],
            "zlib": ["@com_github_madler_zlib//:libz"],
        },
        flags = dicts.add(
            {
                "ghcide": ["ghc-lib"],
                "hlint": ["ghc-lib"],
                "ghc-lib-parser-ex": ["ghc-lib"],
                "zip": ["disable-bzip2"],
            },
            {
                "blaze-textual": ["integer-simple"],
                "cryptonite": ["-integer-gmp"],
                "hashable": ["-integer-gmp"],
                "integer-logarithms": ["-integer-gmp"],
                "text": ["integer-simple"],
                "scientific": ["integer-simple"],
            } if use_integer_simple else {},
        ),
        haddock = False,
        local_snapshot = "//:stack-snapshot.yaml",
        stack_snapshot_json =
            "//:stackage_snapshot_windows.json" if is_windows else "//:stackage_snapshot.json",
        packages = [
            "aeson",
            "aeson-extra",
            "aeson-pretty",
            "ansi-terminal",
            "ansi-wl-pprint",
            "array",
            "async",
            "attoparsec",
            "base",
            "base16-bytestring",
            "base64-bytestring",
            "binary",
            "blaze-html",
            "bytestring",
            "c2hs",
            "Cabal",
            "case-insensitive",
            "cereal",
            "clock",
            "cmark-gfm",
            "conduit",
            "conduit-extra",
            "connection",
            "containers",
            "contravariant",
            "cryptohash",
            "cryptonite",
            "data-default",
            "Decimal",
            "deepseq",
            "digest",
            "directory",
            "dlist",
            "either",
            "exceptions",
            "extra",
            "fast-logger",
            "file-embed",
            "filepath",
            "filepattern",
            "foldl",
            "fuzzy",
            "ghc",
            "ghc-boot",
            "ghc-boot-th",
            "ghc-lib",
            "ghc-lib-parser",
            "ghc-lib-parser-ex",
            "ghc-paths",
            "ghc-prim",
            "gitrev",
            "grpc-haskell",
            "haddock-library",
            "hashable",
            "haskeline",
            "haskell-lsp",
            "haskell-lsp-types",
            "haskell-src",
            "haskell-src-exts",
            "heaps",
            "hie-bios",
            "hlint",
            "hpc",
            "hpp",
            "hslogger",
            "hspec",
            "http-client",
            "http-client-tls",
            "http-conduit",
            "http-types",
            "insert-ordered-containers",
            "jwt",
            "lens",
            "lens-aeson",
            "lifted-async",
            "lifted-base",
            "lsp-test",
            "main-tester",
            "managed",
            "megaparsec",
            "memory",
            "monad-control",
            "monad-logger",
            "monad-loops",
            "mtl",
            "neat-interpolation",
            "network",
            "network-uri",
            "nsis",
            "open-browser",
            "optparse-applicative",
            "optparse-generic",
            "parsec",
            "parser-combinators",
            "parsers",
            "path",
            "path-io",
            "pipes",
            "pretty",
            "prettyprinter",
            "prettyprinter-ansi-terminal",
            "pretty-show",
            "primitive",
            "process",
            "proto3-suite",
            "proto3-wire",
            "QuickCheck",
            "quickcheck-instances",
            "random",
            "range-set-list",
            "recursion-schemes",
            "regex-tdfa",
            "repline",
            "resourcet",
            "retry",
            "rope-utf16-splay",
            "safe",
            "safe-exceptions",
            "scientific",
            "semigroupoids",
            "semigroups",
            "semver",
            "silently",
            "simple-smt",
            "sorted-list",
            "split",
            "stache",
            "stm",
            "swagger2",
            "syb",
            "system-filepath",
            "tagged",
            "tar",
            "tar-conduit",
            "tasty",
            "tasty-ant-xml",
            "tasty-expected-failure",
            "tasty-golden",
            "tasty-hunit",
            "tasty-quickcheck",
            "template-haskell",
            "temporary",
            "terminal-progress-bar",
            "text",
            "time",
            "tls",
            "transformers",
            "transformers-base",
            "turtle",
            "typed-process",
            "uniplate",
            "unix-compat",
            "unliftio",
            "unliftio-core",
            "unordered-containers",
            "uri-encode",
            "utf8-string",
            "uuid",
            "vector",
            "xml",
            "xml-conduit",
            "yaml",
            "zip-archive",
            "zlib",
            "zlib-bindings",
        ] + (["unix"] if not is_windows else ["Win32"]),
        components = {
            "hpp": ["lib", "exe"],
        },
        stack = "@stack_windows//:stack.exe" if is_windows else None,
        vendored_packages = {
            "ghcide": "@ghcide_ghc_lib//:ghcide",
            "grpc-haskell-core": "@grpc_haskell_core//:grpc-haskell-core",
            "js-jquery": "@js_jquery//:js-jquery",
            "js-dgtable": "@js_dgtable//:js-dgtable",
            "js-flot": "@js_flot//:js-flot",
            "shake": "@shake//:shake",
            "zip": "@zip//:zip",
        },
    )

    stack_snapshot(
        name = "ghcide",
        extra_deps = {
            "zlib": ["@com_github_madler_zlib//:libz"],
        },
        flags = {
            "hashable": ["-integer-gmp"],
            "integer-logarithms": ["-integer-gmp"],
            "text": ["integer-simple"],
            "scientific": ["integer-simple"],
        } if use_integer_simple else {},
        haddock = False,
        local_snapshot = "//:ghcide-snapshot.yaml",
        stack_snapshot_json =
            "//:ghcide_snapshot_windows.json" if is_windows else "//:ghcide_snapshot.json",
        packages = [
            "ghcide",
        ],
        components = {"ghcide": ["lib", "exe"]},
        stack = "@stack_windows//:stack.exe" if is_windows else None,
        vendored_packages = {
            "zip": "@zip//:zip",
        },
    )
