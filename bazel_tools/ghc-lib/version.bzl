# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

GHC_LIB_REPO_URL = "https://github.com/digital-asset/ghc-lib"
GHC_LIB_REV = "b503248db52d6049d18a9dbfa31e0f11aef71df7"
GHC_LIB_SHA256 = "2d677bd4bfe6c91fd989551b0821b87b48ab49473a747f548cb58766f9636c11"
GHC_LIB_PATCHES = [
    "@//bazel_tools/ghc-lib:ghc-lib-no-stack.patch",
]

GHC_REPO_URL = "https://github.com/digital-asset/ghc"
GHC_REV = "fc5176a3ded3ce7c9f4a317ec619e9e451bd55d2"
GHC_PATCHES = [
]

GHC_FLAVOR = "da-ghc-8.8.1"
GHC_LIB_VERSION = "8.8.1"
GHC_CPP_OPTIONS = [
    "-DDAML_PRIM",
]
