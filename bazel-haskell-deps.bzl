# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
load("@os_info//:os_info.bzl", "is_linux", "is_windows")
load("@dadew//:dadew.bzl", "dadew_tool_home")
load("@rules_haskell//haskell:cabal.bzl", "stack_snapshot")
load("//bazel_tools/ghc-lib:repositories.bzl", "ghc_lib_and_dependencies")

GHCIDE_REV = "d6c242d5753ba963a8e6390afbcb7fa555b64b26"
GHCIDE_SHA256 = "583f85b6268ce6d1abdc0889ace8fbf13c6842c27fe4d8fbbcac9ad01dc6e705"
JS_JQUERY_VERSION = "3.3.1"
JS_DGTABLE_VERSION = "0.5.2"
JS_FLOT_VERSION = "0.8.3"
SHAKE_VERSION = "0.19.6"
ZIP_VERSION = "1.7.1"
GRPC_HASKELL_REV = "641f0bab046f2f03e5350a7c5f2044af1e19a5b1"
GRPC_HASKELL_SHA256 = "d850d804d7af779bb8717ebe4ea2ac74903a30adeb5262477a2e7a1536f4ca81"
GRPC_HASKELL_PATCHES = [
    "@com_github_digital_asset_daml//bazel_tools:grpc-haskell-core-cpp-options.patch",
    "@com_github_digital_asset_daml//bazel_tools:grpc-haskell-core-upgrade.patch",
]
XML_CONDUIT_VERSION = "1.9.1.1"
LSP_TYPES_VERSION = "1.4.0.0"
LSP_TYPES_SHA256 = "7ae8a3bad0e91d4a2af9b93e3ad207e3f4c3dace40d420e0592f6323ac93fb67"

def daml_haskell_deps():
    """Load all Haskell dependencies of the Daml repository."""

    #
    # Vendored Packages
    #

    http_archive(
        name = "lsp-types",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "lsp-types",
    version = packages["lsp-types"].version,
    srcs = glob(["**"]),
    deps = packages["lsp-types"].deps,
    haddock = False,
    visibility = ["//visibility:public"],
)""",
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:lsp-types-normalisation.patch",
        ],
        sha256 = LSP_TYPES_SHA256,
        strip_prefix = "lsp-types-{}".format(LSP_TYPES_VERSION),
        urls = ["http://hackage.haskell.org/package/lsp-types-{version}/lsp-types-{version}.tar.gz".format(version = LSP_TYPES_VERSION)],
    )

    # ghc-lib based ghcide - injected into `@stackage` and used for Daml IDE.
    http_archive(
        name = "ghcide_ghc_lib",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@rules_haskell//haskell:defs.bzl", "haskell_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "ghcide",
    version = packages["ghcide"].version,
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
        "@stackage//:aeson",
        "@stackage//:base",
        "@stackage//:extra",
        "@stackage//:containers",
        "@stackage//:lsp-types",
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
""",
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-ghcide-binary-q.patch",
        ],
        sha256 = GHCIDE_SHA256,
        strip_prefix = "daml-ghcide-%s" % GHCIDE_REV,
        urls = ["https://github.com/digital-asset/daml-ghcide/archive/%s.tar.gz" % GHCIDE_REV],
    )

    ghc_lib_and_dependencies()

    http_archive(
        name = "grpc_haskell_core",
        build_file_content = """
load("@com_github_digital_asset_daml//bazel_tools:haskell.bzl", "c2hs_suite")
load("@rules_haskell//haskell:defs.bzl", "haskell_library")
c2hs_suite(
    name = "grpc-haskell-core",
    srcs = [
        "src/Network/GRPC/Unsafe/Constants.hsc",
    ] + glob(["src/**/*.hs"]),
    c2hs_src_strip_prefix = "src",
    hackage_deps = ["clock", "managed", "base", "sorted-list", "bytestring", "containers", "stm", "transformers"],
    c2hs_srcs = [
        "src/Network/GRPC/Unsafe/Time.chs",
        "src/Network/GRPC/Unsafe/ChannelArgs.chs",
        "src/Network/GRPC/Unsafe/Slice.chs",
        "src/Network/GRPC/Unsafe/ByteBuffer.chs",
        "src/Network/GRPC/Unsafe/Metadata.chs",
        "src/Network/GRPC/Unsafe/Op.chs",
        "src/Network/GRPC/Unsafe.chs",
        "src/Network/GRPC/Unsafe/Security.chs",
    ],
    compiler_flags = ["-XCPP", "-Wno-unused-imports", "-Wno-unused-record-wildcards"],
    visibility = ["//visibility:public"],
    deps = [
        "@grpc_haskell_core_cbits//:merged_cbits",
    ],
)
""",
        patch_args = ["-p1"],
        patches = GRPC_HASKELL_PATCHES,
        sha256 = GRPC_HASKELL_SHA256,
        strip_prefix = "gRPC-haskell-{}/core".format(GRPC_HASKELL_REV),
        urls = ["https://github.com/awakesecurity/gRPC-haskell/archive/{}.tar.gz".format(GRPC_HASKELL_REV)],
    )

    # We need to make sure that the cbits are in a different directory
    # to get GHCi on MacOS to pick the dynamic library. Otherwise it first looks in the directory
    # and it sees libfatcbits.so which has the wrong ending and libfatcbits.a and ends up loading
    # the static library which breaks. The GHCi wrapper from rules_haskell actually sets up
    # libfatcbits.dylib properly so moving it outside ensures that this is the only option
    # for GHCi and things work properly.
    http_archive(
        name = "grpc_haskell_core_cbits",
        build_file_content = """
load("@com_github_digital_asset_daml//bazel_tools:fat_cc_library.bzl", "fat_cc_library")

fat_cc_library(
  name = "merged_cbits",
  input_lib = "cbits",
  visibility = ["//visibility:public"],
)
cc_library(
  name = "cbits",
  srcs = glob(["cbits/*.c"]),
  hdrs = glob(["include/*.h"]),
  includes = ["include/"],
  deps = [
    "@com_github_grpc_grpc//:grpc",
  ]
)
""",
        patch_args = ["-p1"],
        patches = GRPC_HASKELL_PATCHES,
        sha256 = GRPC_HASKELL_SHA256,
        strip_prefix = "gRPC-haskell-{}/core".format(GRPC_HASKELL_REV),
        urls = ["https://github.com/awakesecurity/gRPC-haskell/archive/{}.tar.gz".format(GRPC_HASKELL_REV)],
    )

    http_archive(
        name = "grpc_haskell",
        build_file_content = """
load("@rules_haskell//haskell:defs.bzl", "haskell_library")
load("@stackage//:packages.bzl", "packages")
haskell_library(
    name = "grpc-haskell",
    srcs = glob(["src/**/*.hs"]),
    deps = packages["grpc-haskell"].deps,
    visibility = ["//visibility:public"],
)
""",
        sha256 = GRPC_HASKELL_SHA256,
        strip_prefix = "gRPC-haskell-{}".format(GRPC_HASKELL_REV),
        urls = ["https://github.com/awakesecurity/gRPC-haskell/archive/{}.tar.gz".format(GRPC_HASKELL_REV)],
    )

    http_archive(
        name = "proto3-suite",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
deps = [p for p in packages["proto3-suite"].deps if p.name != "swagger2"]
haskell_cabal_library(
    name = "proto3-suite",
    version = packages["proto3-suite"].version,
    srcs = glob(["src/**", "test-files/*.bin", "tests/*", "proto3-suite.cabal"]),
    haddock = False,
    deps = deps,
    verbose = False,
    visibility = ["//visibility:public"],
    flags = ["-swagger"],
)
# XXX: haskell_cabal_binary inexplicably fails with
#   realgcc.exe: error: CreateProcess: No such file or directory
# So we use haskell_binary instead.
load("@rules_haskell//haskell:defs.bzl", "haskell_binary")
haskell_binary(
    name = "compile-proto-file",
    srcs = ["tools/compile-proto-file/Main.hs"],
    compiler_flags = ["-w", "-optF=-w"],
    deps = [":proto3-suite"] + deps,
    visibility = ["//visibility:public"],
)
""",
        sha256 = "1649ebbe49ee34901ea920c860ad6f21188340a981c4c8d7521df101e75aa8ab",
        strip_prefix = "proto3-suite-d4a288068587f8738c84465a9ca113a3fe845ffc",
        urls = ["https://github.com/cocreature/proto3-suite/archive/d4a288068587f8738c84465a9ca113a3fe845ffc.tar.gz"],
        patches = ["@com_github_digital_asset_daml//bazel_tools:haskell_proto3_suite_deriving_defaults.patch"],
        patch_args = ["-p1"],
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
    version = packages["js-jquery"].version,
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["js-jquery"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
)
""",
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
    version = packages["js-dgtable"].version,
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["js-dgtable"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
)
""",
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
    version = packages["js-flot"].version,
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["js-flot"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
)
""",
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-js-flot.patch",
        ],
        sha256 = "1ba2f2a6b8d85da76c41f526c98903cbb107f8642e506c072c1e7e3c20fe5e7a",
        strip_prefix = "js-flot-{}".format(JS_FLOT_VERSION),
        urls = ["http://hackage.haskell.org/package/js-flot-{version}/js-flot-{version}.tar.gz".format(version = JS_FLOT_VERSION)],
    )

    http_archive(
        name = "turtle",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "turtle",
    version = packages["turtle"].version,
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["turtle"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
)
""",
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-turtle.patch",
        ],
        sha256 = "ac5c352a2e2a4dec853623f24677f41cdd8cff1140741bf38c8e06f09551e009",
        strip_prefix = "turtle-1.5.23",
        urls = ["http://hackage.haskell.org/package/turtle-1.5.23/turtle-1.5.23.tar.gz"],
    )

    http_archive(
        name = "xml-conduit",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "xml-conduit",
    version = packages["xml-conduit"].version,
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["xml-conduit"].deps,
    # For some reason we need to manually add the setup dep here.
    setup_deps = ["@stackage//:cabal-doctest"],
    verbose = False,
    visibility = ["//visibility:public"],
)
""",
        sha256 = "bdb117606c0b56ca735564465b14b50f77f84c9e52e31d966ac8d4556d3ff0ff",
        strip_prefix = "xml-conduit-{}".format(XML_CONDUIT_VERSION),
        urls = ["http://hackage.haskell.org/package/xml-conduit-{version}/xml-conduit-{version}.tar.gz".format(version = XML_CONDUIT_VERSION)],
    )

    http_archive(
        name = "shake",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "shake",
    version = packages["shake"].version,
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["shake"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
    flags = packages["shake"].flags,
)
""",
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-shake.patch",
        ],
        sha256 = "7d9db837bfd67acaaabdb3cea29acc15559ede82dd9f75d438589268031cd542",
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
    version = packages["zip"].version,
    srcs = glob(["**"]),
    haddock = False,
    deps = packages["zip"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
    flags = packages["zip"].flags,
)
""",
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:haskell-zip.patch",
        ],
        sha256 = "0d7f02bbdf6c49e9a33d2eca4b3d7644216a213590866dafdd2b47ddd38eb746",
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
                "ghc-lib-parser-ex": ["ghc-lib"],
                "hlint": ["ghc-lib"],
                "shake": ["embed-files"],
                "zip": ["disable-bzip2", "disable-zstd"],
                "proto3-suite": ["-swagger"],
            },
        ),
        haddock = False,
        local_snapshot = "//:stack-snapshot.yaml",
        stack_snapshot_json =
            "//:stackage_snapshot_windows.json" if is_windows else "//:stackage_snapshot.json",
        packages = [
            "aeson",
            "aeson-extra",
            "aeson-pretty",
            "alex",
            "ansi-terminal",
            "ansi-wl-pprint",
            "async",
            "base",
            "base16-bytestring",
            "base64",
            "base64-bytestring",
            "binary",
            "blaze-html",
            "bytestring",
            "c2hs",
            "Cabal",
            "case-insensitive",
            "clock",
            "cmark-gfm",
            "conduit",
            "conduit-extra",
            "connection",
            "containers",
            "cryptohash",
            "cryptonite",
            "data-default",
            "Decimal",
            "deepseq",
            "directory",
            "dlist",
            "either",
            "exceptions",
            "extra",
            "fast-logger",
            "file-embed",
            "filelock",
            "filepath",
            "filepattern",
            "ghc-lib-parser-ex",
            "gitrev",
            "happy",
            "hashable",
            "haskeline",
            "haskell-src-exts",
            "hlint",
            "hpp",
            "hspec",
            "http-client",
            "http-client-tls",
            "http-conduit",
            "http-types",
            "jwt",
            "lens",
            "lens-aeson",
            "lifted-async",
            "lifted-base",
            "lsp",
            "lsp-test",
            "main-tester",
            "megaparsec",
            "memory",
            "monad-control",
            "monad-logger",
            "monad-loops",
            "mtl",
            "network",
            "network-uri",
            "nsis",
            "open-browser",
            "optparse-applicative",
            "parsec",
            "parser-combinators",
            "path",
            "path-io",
            "pretty",
            "prettyprinter",
            "pretty-show",
            "process",
            "proto3-wire",
            "QuickCheck",
            "random",
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
            "semver",
            "silently",
            "simple-smt",
            "sorted-list",
            "split",
            "stache",
            "stm",
            "stm-chans",
            "stm-conduit",
            "syb",
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
            "yaml",
            "zip-archive",
        ] + (["unix"] if not is_windows else ["Win32"]),
        components = {
            "hpp": ["lib", "exe"],
            "attoparsec": [
                "lib:attoparsec",
                "lib:attoparsec-internal",
            ],
        },
        components_dependencies = {
            "attoparsec": """{"lib:attoparsec": ["lib:attoparsec-internal"]}""",
        },
        stack = "@stack_windows//:stack.exe" if is_windows else None,
        vendored_packages = {
            "ghcide": "@ghcide_ghc_lib//:ghcide",
            "ghc-lib": "@com_github_digital_asset_daml//bazel_tools/ghc-lib/ghc-lib",
            "ghc-lib-parser": "@com_github_digital_asset_daml//bazel_tools/ghc-lib/ghc-lib-parser",
            "grpc-haskell-core": "@grpc_haskell_core//:grpc-haskell-core",
            "grpc-haskell": "@grpc_haskell//:grpc-haskell",
            "js-dgtable": "@js_dgtable//:js-dgtable",
            "js-flot": "@js_flot//:js-flot",
            "js-jquery": "@js_jquery//:js-jquery",
            "lsp-types": "@lsp-types//:lsp-types",
            "proto3-suite": "@proto3-suite//:proto3-suite",
            "shake": "@shake//:shake",
            "turtle": "@turtle//:turtle",
            "xml-conduit": "@xml-conduit//:xml-conduit",
            "zip": "@zip//:zip",
        },
    )

    stack_snapshot(
        name = "hls",
        extra_deps = {
            "zlib": ["@com_github_madler_zlib//:libz"],
        },
        flags = dicts.add(
            {
                "haskell-language-server": ["-fourmolu", "-ormolu"],
            },
        ),
        haddock = False,
        local_snapshot = "//:hls-snapshot.yaml",
        stack_snapshot_json = "//:hls_snapshot.json",
        packages = [
            "haskell-language-server",
        ],
        components = {
            "haskell-language-server": ["lib", "exe"],
            "attoparsec": [
                "lib:attoparsec",
                "lib:attoparsec-internal",
            ],
        },
        components_dependencies = {
            "attoparsec": """{"lib:attoparsec": ["lib:attoparsec-internal"]}""",
        },
        stack = "@stack_windows//:stack.exe" if is_windows else None,
        vendored_packages = {
            "zip": "@zip//:zip",
        },
    ) if not is_windows else None
