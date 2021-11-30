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
load("@os_info//:os_info.bzl", "is_linux", "is_windows")
load("@dadew//:dadew.bzl", "dadew_tool_home")
load("@rules_haskell//haskell:cabal.bzl", "stack_snapshot")

GHC_LIB_REV = "ded0a9e82e5d4bdf47e568ee83f26959"
GHC_LIB_SHA256 = "cee47f49c8f26feba53d3d90bc6d306f9a832b199096e06c3cadc1b021792269"
GHC_LIB_VERSION = "8.8.1"
GHC_LIB_PARSER_REV = "ded0a9e82e5d4bdf47e568ee83f26959"
GHC_LIB_PARSER_SHA256 = "869452c0158186928eb20540818eec69d6d76c1f7cf8a96de98be4150bebdd66"
GHC_LIB_PARSER_VERSION = "8.8.1"
GHCIDE_REV = "e04b5386b3741b839eb5c3d2a2586fd2aa97229c"
GHCIDE_SHA256 = "1d27926e0ad3c2a9536f23b454875a385ecc766ae68ce48a0ec88d0867884b46"
JS_JQUERY_VERSION = "3.3.1"
JS_DGTABLE_VERSION = "0.5.2"
JS_FLOT_VERSION = "0.8.3"
SHAKE_VERSION = "0.18.5"
ZIP_VERSION = "1.7.1"
GRPC_HASKELL_REV = "641f0bab046f2f03e5350a7c5f2044af1e19a5b1"
GRPC_HASKELL_SHA256 = "d850d804d7af779bb8717ebe4ea2ac74903a30adeb5262477a2e7a1536f4ca81"
XML_CONDUIT_VERSION = "1.9.1.1"
LSP_TYPES_VERSION = "1.2.0.0"

def daml_haskell_deps():
    """Load all Haskell dependencies of the DAML repository."""

    # XXX: We do not have access to an integer-simple version of GHC on Windows.
    # For the time being we build with GMP. See https://github.com/digital-asset/daml/issues/106
    use_integer_simple = not is_windows

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
    visibility = ["//visibility:public"],
)""",
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:lsp-types-normalisation.patch",
        ],
        sha256 = "637a85878d7b8c895311eb6878f19c43038ef93db1e4de4820b04fa7bc30b4ab",
        strip_prefix = "lsp-types-{}".format(LSP_TYPES_VERSION),
        urls = ["http://hackage.haskell.org/package/lsp-types-{version}/lsp-types-{version}.tar.gz".format(version = LSP_TYPES_VERSION)],
    )

    # ghc-lib based ghcide - injected into `@stackage` and used for DAML IDE.
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

    http_archive(
        name = "ghc_lib",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "ghc-lib",
    version = packages["ghc-lib"].version,
    srcs = glob(["**"]),
    haddock = False,
    flags = packages["ghc-lib"].flags,
    deps = packages["ghc-lib"].deps,
    visibility = ["//visibility:public"],
    tools = packages["ghc-lib"].tools,
)
""",
        sha256 = GHC_LIB_SHA256,
        strip_prefix = "ghc-lib-%s" % GHC_LIB_VERSION,
        urls = ["https://daml-binaries.da-ext.net/da-ghc-lib/ghc-lib-%s.tar.gz" % GHC_LIB_REV],
    )

    http_archive(
        name = "ghc_lib_parser",
        build_file_content = """
load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_library")
load("@stackage//:packages.bzl", "packages")
haskell_cabal_library(
    name = "ghc-lib-parser",
    version = packages["ghc-lib-parser"].version,
    srcs = glob(["**"]),
    haddock = False,
    flags = packages["ghc-lib-parser"].flags,
    deps = packages["ghc-lib-parser"].deps,
    visibility = ["//visibility:public"],
    tools = packages["ghc-lib-parser"].tools,
)
""",
        sha256 = GHC_LIB_PARSER_SHA256,
        strip_prefix = "ghc-lib-parser-%s" % GHC_LIB_PARSER_VERSION,
        urls = ["https://daml-binaries.da-ext.net/da-ghc-lib/ghc-lib-parser-%s.tar.gz" % GHC_LIB_PARSER_REV],
    )

    cbit_dep = ":fat_cbits" if is_windows else ":needed-cbits-clib" if is_linux else ":cbits"
    grpc_dep = "@com_github_grpc_grpc//:grpc" if is_windows else "@grpc_nix//:grpc_lib"

    http_archive(
        name = "grpc_haskell_core",
        build_file_content = """
load("@com_github_digital_asset_daml//bazel_tools:fat_cc_library.bzl", "fat_cc_library")
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
        "{cbit_dep}",
    ],
)

cc_library(
  name = "needed-cbits-clib",
  srcs = [":libneeded-cbits.so"],
  deps = ["{grpc_dep}"],
  hdrs = glob(["include/*.h"]),
  includes = ["include/"],
)

# Bazel produces cbits without NEEDED entries which makes
# ghci unhappy. We cannot use fat_cbits on the nix-built grpc
# since it lacks static libs but we can patchelf the cbits to add
# the NEEDED entry.
# Apparently this is not needed on macos for whatever reason and patchelf
# doesn’t work there anyway so we only use it on Linux.
genrule(
  name = "needed-cbits",
  srcs = [":cbits", "@grpc_nix//:grpc_file"],
  outs = ["libneeded-cbits.so"],
  tools = ["@patchelf_nix//:bin/patchelf"],
  cmd = '''
  set -eou pipefail
  # We get 3 libs. We want the shared lib which comes last.
  CBITS=$$(echo $(locations :cbits) | cut -f 3 -d ' ')
  OLD_RPATH=$$($(location @patchelf_nix//:bin/patchelf) --print-rpath $$CBITS)
  GRPC_RPATH=$$(dirname $$(readlink -f $$(echo $(locations @grpc_nix//:grpc_file) | cut -f 1 -d ' ')))
  $(location @patchelf_nix//:bin/patchelf) $$CBITS --add-needed libgrpc.so.19 --output $(location libneeded-cbits.so)
  $(location @patchelf_nix//:bin/patchelf) $(location libneeded-cbits.so) --set-rpath "$$OLD_RPATH:$$GRPC_RPATH"
  '''
)

fat_cc_library(
  name = "fat_cbits",
  input_lib = "cbits",
)
cc_library(
  name = "cbits",
  srcs = glob(["cbits/*.c"]),
  hdrs = glob(["include/*.h"]),
  includes = ["include/"],
  deps = [
    "{grpc_dep}",
  ]
)
""".format(cbit_dep = cbit_dep, grpc_dep = grpc_dep),
        patch_args = ["-p1"],
        patches = [
            "@com_github_digital_asset_daml//bazel_tools:grpc-haskell-core-cpp-options.patch",
            "@com_github_digital_asset_daml//bazel_tools:grpc-haskell-core-upgrade.patch",
        ],
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
haskell_cabal_library(
    name = "proto3-suite",
    version = packages["proto3-suite"].version,
    srcs = glob(["src/**", "test-files/*.bin", "tests/*", "proto3-suite.cabal"]),
    haddock = False,
    deps = packages["proto3-suite"].deps,
    verbose = False,
    visibility = ["//visibility:public"],
)
# XXX: haskell_cabal_binary inexplicably fails with
#   realgcc.exe: error: CreateProcess: No such file or directory
# So we use haskell_binary instead.
load("@rules_haskell//haskell:defs.bzl", "haskell_binary")
haskell_binary(
    name = "compile-proto-file",
    srcs = ["tools/compile-proto-file/Main.hs"],
    compiler_flags = ["-w", "-optF=-w"],
    deps = [":proto3-suite"] + packages["proto3-suite"].deps,
    visibility = ["//visibility:public"],
)
""",
        sha256 = "b294ff0fe24c6c256dc8eca1d44c2a9a928b9a1bc70ddce6a1d059499edea119",
        strip_prefix = "proto3-suite-0af901f9ef3b9719e08eae4fab8fd700d6c8047a",
        urls = ["https://github.com/awakesecurity/proto3-suite/archive/0af901f9ef3b9719e08eae4fab8fd700d6c8047a.tar.gz"],
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
            },
            {
                "blaze-textual": ["integer-simple"],
                "cryptonite": ["-integer-gmp"],
                "hashable": ["-integer-gmp"],
                "integer-logarithms": ["-integer-gmp"],
                "scientific": ["integer-simple"],
                "text": ["integer-simple"],
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
            "async",
            "base",
            "base16-bytestring",
            "base64",
            "base64-bytestring",
            "binary",
            "blaze-html",
            "bytestring",
            "c2hs",
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
            "parser-combinators",
            "path",
            "path-io",
            "pretty",
            "prettyprinter",
            "pretty-show",
            "process",
            "proto3-wire",
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
        },
        stack = "@stack_windows//:stack.exe" if is_windows else None,
        vendored_packages = {
            "ghcide": "@ghcide_ghc_lib//:ghcide",
            "ghc-lib": "@ghc_lib//:ghc-lib",
            "ghc-lib-parser": "@ghc_lib_parser//:ghc-lib-parser",
            "grpc-haskell-core": "@grpc_haskell_core//:grpc-haskell-core",
            "grpc-haskell": "@grpc_haskell//:grpc-haskell",
            "js-dgtable": "@js_dgtable//:js-dgtable",
            "js-flot": "@js_flot//:js-flot",
            "js-jquery": "@js_jquery//:js-jquery",
            "lsp-types": "@lsp-types//:lsp-types",
            "proto3-suite": "@proto3-suite//:proto3-suite",
            "shake": "@shake//:shake",
            "xml-conduit": "@xml-conduit//:xml-conduit",
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
            "scientific": ["integer-simple"],
            "text": ["integer-simple"],
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
