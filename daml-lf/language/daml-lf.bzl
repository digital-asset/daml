# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Stable and latest refers to the versions used by the compiler.
# If we make a new LF release, we bump latest and once we make it the default
# we bump stable.
# The following dictionary is always defined for "legacy", "stable", "latest",
# and "dev". It contains "preview" iff a preview version is available.
# If exists "preview"'s value is guarantee to be different for all other values.
lf_version_configuration = {
    "legacy": "1.8",
    "stable": "1.11",
    "latest": "1.12",
    "dev": "1.dev",
}

lf_version_configuration_versions = depset(lf_version_configuration.values()).to_list()

# aggregates a list of version keywords and versions:
# 1. converts keyword in version
# 2. removes "preview" if no preview version is available.
# 3. removes duplicates
def lf_versions_aggregate(versions):
    lf_versions = [lf_version_configuration.get(version, version) for version in versions]
    return depset([lf_version for lf_version in lf_versions if lf_version != "preview"]).to_list()

# We generate docs for the latest preview version since releasing
# preview versions without docs for them is a bit annoying.
# Once we start removing modules in newer LF versions, we might
# have to come up with something more clever here to make
# sure that we don’t remove docs for a module that is still supported
# in a stable LF version.
lf_docs_version = lf_version_configuration.get("preview", lf_version_configuration.get("latest"))

# All LF versions for which we have protobufs.
LF_VERSIONS = [
    "1.6",
    "1.7",
    "1.8",
    "1.11",
    "1.12",
    "dev",
]

# The subset of LF versions accepted by //daml-lf/encoder
ENCODER_LF_VERSIONS = ["1.dev" if ver == "dev" else ver for ver in LF_VERSIONS]

# The subset of LF versions accepted by the compiler in the syntax
# expected by the --target option.
COMPILER_LF_VERSIONS = ENCODER_LF_VERSIONS

# We need Any in DAML Script so we require DAML-LF >= 1.7
SCRIPT_LF_VERSIONS = [ver for ver in COMPILER_LF_VERSIONS if ver != "1.6"]

LF_VERSION_PACKAGE_DIGITALASSET = {
    "1.6": "digitalasset",
    "1.7": "digitalasset",
    "1.8": "digitalasset",
    "1.11": "daml",
    "1.12": "daml",
}

def lf_version_package(version):
    return LF_VERSION_PACKAGE_DIGITALASSET.get(version, "daml")

LF_MAJOR_VERSIONS = ["1"]

def mangle_for_java(name):
    return name.replace(".", "_")
