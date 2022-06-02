# Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

GHC_LIB_REPO_URL = "https://github.com/digital-asset/ghc-lib"
GHC_LIB_REV = "905f51296d979d79da511bed9ab2da7cb9429c9f"
GHC_LIB_SHA256 = "9b688f19f1a5e0b243bc490517e40ea8c840f2ff4515b55cc2733de7bdd436b2"
GHC_LIB_PATCHES = [
    "@//bazel_tools/ghc-lib:ghc-lib-no-stack.patch",
]

GHC_REPO_URL = "https://github.com/digital-asset/ghc"
GHC_REV = "463da30a622cc1b675892fe68e763ec8f614d291"
GHC_PATCHES = [
]

GHC_FLAVOR = "da-ghc-8.8.1"
GHC_LIB_VERSION = "8.8.1"
