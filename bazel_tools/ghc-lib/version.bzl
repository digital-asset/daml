# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

GHC_LIB_REPO_URL = "https://github.com/digital-asset/ghc-lib"
GHC_LIB_REV = "905f51296d979d79da511bed9ab2da7cb9429c9f"
GHC_LIB_SHA256 = "9b688f19f1a5e0b243bc490517e40ea8c840f2ff4515b55cc2733de7bdd436b2"

GHC_REPO_URL = "https://github.com/digital-asset/ghc"
GHC_REV = "3b52915616984a41dfba3c89fad0b8f9c3cb4ed3"
GHC_SHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
GHC_PATCHES = [
    "@//bazel_tools/ghc-lib:ghc-daml-prim.patch",
]

GHC_FLAVOR = "da-ghc-8.8.1"
GHC_LIB_VERSION = "8.8.1"
