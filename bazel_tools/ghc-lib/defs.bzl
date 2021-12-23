# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@rules_haskell//haskell:cabal.bzl", "haskell_cabal_binary", "haskell_cabal_library")

def ghc_lib_gen():
    native.filegroup(
        name = "srcs",
        srcs = native.glob(["**"]),
        visibility = ["//visibility:public"],
    )
    haskell_cabal_library(
        name = "ghc-lib-gen-lib",
        package_name = "ghc-lib-gen",
        version = "0.1.0.0",
        haddock = False,
        srcs = [":srcs"],
        deps = [
            "@stackage//:base",
            "@stackage//:process",
            "@stackage//:filepath",
            "@stackage//:containers",
            "@stackage//:directory",
            "@stackage//:optparse-applicative",
            "@stackage//:bytestring",
            "@stackage//:yaml",
            "@stackage//:aeson",
            "@stackage//:text",
            "@stackage//:unordered-containers",
            "@stackage//:extra",
        ],
    )
    haskell_cabal_binary(
        name = "ghc-lib-gen",
        srcs = [":srcs"],
        deps = [
            ":ghc-lib-gen-lib",
            "@stackage//:base",
            "@stackage//:containers",
            "@stackage//:directory",
            "@stackage//:extra",
            "@stackage//:filepath",
            "@stackage//:optparse-applicative",
            "@stackage//:process",
        ],
        visibility = ["//visibility:public"],
    )

def ghc():
    pass
