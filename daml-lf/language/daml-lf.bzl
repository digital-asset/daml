# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Stable and latest refers to the versions used by the compiler.
# If we make a new LF release, we bump latest and once we make it the default
# we bump stable.
lf_stable_version = "1.8"
lf_latest_version = "1.8"
lf_dev_version = "1.dev"

# All LF versions for which we have protobufs.
LF_VERSIONS = [
    "1.6",
    "1.7",
    "1.8",
    "dev",
]

LF_VERSION_PACKAGE_DIGITALASSET = {"1.6": "digitalasset", "1.7": "digitalasset", "1.8": "digitalasset"}

def lf_version_package(version):
    return LF_VERSION_PACKAGE_DIGITALASSET.get(version, "daml")

LF_MAJOR_VERSIONS = ["0", "1"]
