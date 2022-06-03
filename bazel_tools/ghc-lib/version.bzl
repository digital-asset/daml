# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

GHC_LIB_REPO_URL = "https://github.com/digital-asset/ghc-lib"
GHC_LIB_REV = "c6c184cdbc3493100f787ba87ce9971b2fbba22a"
GHC_LIB_SHA256 = "2da9ac94fbe8b5af465cf0a25316d392a0023ca0469fb83310c5fe1ace9499ea"
GHC_LIB_PATCHES = [
    "@//bazel_tools/ghc-lib:ghc-lib-no-stack.patch",
]

GHC_REPO_URL = "https://github.com/digital-asset/ghc"
GHC_REV = "45d5588a3341f8a6d43686fc0c9d0addc238a99a"
GHC_PATCHES = [
]

GHC_FLAVOR = "da-ghc-8.8.1"
GHC_LIB_VERSION = "8.8.1"
GHC_CPP_OPTIONS = [
    "-DDAML_PRIM",
]
