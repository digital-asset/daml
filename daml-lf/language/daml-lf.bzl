# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def mangle_for_java(name):
    return name.replace(".", "_")

def mangle_for_damlc(name):
    return "v{}".format(name.replace(".", ""))

def _to_major_minor_str(v):
    (major_str, _, minor_str) = v.partition(".")
    return (major_str, minor_str)

def _cmp_int(a, b):
    if a == b:
        return 0
    elif a > b:
        return 1
    else:
        return -1

def _cmp_minor_version_str(a_str, b_str):
    if a_str == b_str:
        return 0
    elif a_str == "dev":
        return 1
    elif b_str == "dev":
        return -1
    else:
        return _cmp_int(int(a_str), int(b_str))

def _minor_version_in(mv_str, minor_version_range):
    if minor_version_range == None:
        return False
    else:
        return (
            _cmp_minor_version_str(mv_str, minor_version_range[0]) >= 0 and
            _cmp_minor_version_str(mv_str, minor_version_range[1]) <= 0
        )

# Returns true if v is in the union of [1.x for x in v1_minor_version_range] and
# [2.y for y in v2_minor_version_range]. None means the empty range.
def version_in(
        v,
        v1_minor_version_range = None,
        v2_minor_version_range = None):
    (major_str, minor_str) = _to_major_minor_str(v)
    if major_str == "1":
        return _minor_version_in(minor_str, v1_minor_version_range)
    elif major_str == "2":
        return _minor_version_in(minor_str, v2_minor_version_range)
    else:
        return False

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

# TODO(#17366): rework lf_version_configuration to be indexed by major version
#  and delete this dictionary
lf_version_latest = {
    "1": lf_version_configuration.get("latest"),
    "2": "2.dev",
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

# The subset of LF versions accepted by the compiler's --target option.
# Must be kept in sync with supportedOutputVersions in Version.hs.
# TODO(#17366): add 2.0 once created
COMPILER_LF_VERSIONS = ["1.14", "1.15"] + LF_DEV_VERSIONS

# The subset of COMPILER_LF_VERSIONS with major version 2.
COMPILER_LF2_VERSIONS = [
    v
    for v in COMPILER_LF_VERSIONS
    if version_in(v, v2_minor_version_range = ("0", "dev"))
]

LF_MAJOR_VERSIONS = ["1", "2"]
