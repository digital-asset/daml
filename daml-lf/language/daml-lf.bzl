# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Stable and latest refers to the versions used by the compiler.
# If we make a new LF release, we bump latest and once we make it the default
# we bump stable.
lf_stable_version = "1.8"
lf_latest_version = "1.11"

# lf_preview_version is non empty if a preview version is available
# contains at most one version
lf_preview_version = ["1.12"]
lf_dev_version = "1.dev"

# All LF versions for which we have protobufs.
LF_VERSIONS = [
    "1.6",
    "1.7",
    "1.8",
    "1.11",
    "1.12",
    "dev",
]

# The subset of LF versions accepted by the compiler in the syntax
# expected by the --target option.
COMPILER_LF_VERSIONS = [
    "1.6",
    "1.7",
    "1.8",
    "1.11",
    "1.dev",
]

# We need Any in DAML Script so we require DAML-LF >= 1.7
SCRIPT_LF_VERSIONS = [ver for ver in COMPILER_LF_VERSIONS if ver != "1.6"]

LF_VERSION_PACKAGE_DIGITALASSET = {
    "1.6": "digitalasset",
    "1.7": "digitalasset",
    "1.8": "digitalasset",
    "1.11": "daml",
}

def lf_version_package(version):
    return LF_VERSION_PACKAGE_DIGITALASSET.get(version, "daml")

LF_MAJOR_VERSIONS = ["1"]
