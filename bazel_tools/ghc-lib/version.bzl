# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

GHC_LIB_REPO_URL = "https://github.com/digital-asset/ghc-lib"
GHC_LIB_REV = "362d4f38a7ac10521393de9b7ad942a77a2605be"
GHC_LIB_SHA256 = "ce6ddf6b4706f811455cbe591f442d7b6519937d9ec1318f8544068609981986"

GHC_REPO_URL = "https://github.com/digital-asset/ghc"
GHC_REV = "3d554575dc40375a1e4995def35cca17a3e9aa95"
GHC_SHA256 = "26891fb947ed928d3b515a827060ead1a677a4eb8313d29ab57cdf9d481af04b"
GHC_PATCHES = [
    "@//bazel_tools/ghc-lib:ghc-daml-prim.patch",
]

GHC_FLAVOR = "da-ghc-8.8.1"
GHC_LIB_VERSION = "8.8.1"
