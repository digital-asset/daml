# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def mangle_for_java(name):
    return name.replace(".", "_")

def mangle_for_damlc(name):
    return "v{}".format(name.replace(".", ""))

# The following dictionary alias LF versions to keywords:
# - "legacy" is the keyword for last LF version that supports legacy
#    contract ID scheme,
# - "default" is the keyword for the default compiler output,
# - "latest" is the keyword for the latest stable LF version,
# - "preview" is the keyword fort he next LF version, *not stable*,
#    usable for beta testing,
# The following dictionary is always defined for "legacy", "stable",
# and "latest". It contains "preview" iff a preview version is
# available.  If it exists, "preview"'s value is guaranteed to be different
# from all other values. If we make a new LF release, we bump latest
# and once we make it the compiler default we bump stable.

lf_version_configuration = {
    "legacy": "1.8",
    "default": "1.15",
    "latest": "1.15",
    #    "preview": "",

    # "dev" is now ambiguous, use either 1.dev or 2.dev explicitly
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

# All LF dev versions
LF_DEV_VERSIONS = [
    "1.dev",
    "2.dev",
]

# All LF versions
LF_VERSIONS = [
    "1.6",
    "1.7",
    "1.8",
    "1.11",
    "1.12",
    "1.13",
    "1.14",
    "1.15",
] + LF_DEV_VERSIONS

def lf_version_is_dev(versionStr):
    return versionStr in LF_DEV_VERSIONS

# The stable versions for which we have an LF proto definition under daml-lf/archive/src/stable
# TODO(#17366): add 2.0 once created
SUPPORTED_PROTO_STABLE_LF_VERSIONS = ["1.14", "1.15"]

# The subset of LF versions accepted by the compiler in the syntax, expected by the --target option.
# Must be kept in sync with supportedOutputVersions in Version.hs.
# TODO(#17366): add 2.0 once created
COMPILER_LF_VERSIONS = ["1.14", "1.15"] + LF_DEV_VERSIONS

LF_MAJOR_VERSIONS = ["1", "2"]
