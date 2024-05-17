# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_intel")

def mangle_for_java(name):
    return name.replace(".", "_")

def mangle_for_damlc(name):
    return "v{}".format(name.replace(".", ""))

def _to_major_minor_str(v):
    (major_str, _, minor_str) = v.partition(".")
    return (major_str, minor_str)

def _major_str(v):
    return _to_major_minor_str(v)[0]

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

# The lastest stable version for each major LF version.
lf_version_latest = {
    "1": "1.16",
}

# The following dictionary aliases LF versions to keywords:
# - "default" is the keyword for the default compiler output,
# - "latest" is the keyword for the latest stable LF version,
# - "preview" is the keyword fort he next LF version, *not stable*,
#    usable for beta testing,
# The following dictionary is always defined for "default" and "latest". It
# contains "preview" iff a preview version is available. If it exists,
# "preview"'s value is guaranteed to be different from all other values. If we
# make a new LF release, we bump latest and once we make it the compiler default
# we bump default.
lf_version_configuration = {
    "default": "1.15",
    "latest": lf_version_latest.get("1"),
    #    "preview": "1.16",
    "dev": "1.dev",
}

# The Daml-LF version used by default by the compiler if it matches the
# provided major version, the latest non-dev version with that major version
# otherwise. Can be used as a future-proof approximation of "default" version
# across major versions.
def lf_version_default_or_latest(major):
    default_version = lf_version_configuration.get("default")
    (default_major, _) = _to_major_minor_str(default_version)
    return default_version if default_major == major else lf_version_latest.get(major)

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
]

# All LF versions
LF_VERSIONS = ([
    "1.8",
    "1.11",
    "1.12",
    "1.13",
    "1.14",
    "1.15",
    "1.16",
] if is_intel else ["1.14", "1.15", "1.16"]) + LF_DEV_VERSIONS

def lf_version_is_dev(versionStr):
    return versionStr in LF_DEV_VERSIONS

# The stable versions for which we have an LF proto definition under daml-lf/archive/src/stable
SUPPORTED_PROTO_STABLE_LF_VERSIONS = ["1.14", "1.15", "1.16"]

# The subset of LF versions accepted by the compiler's --target option.
# Must be kept in sync with supportedOutputVersions in Version.hs.
COMPILER_LF_VERSIONS = ["1.14", "1.15", "1.16"] + LF_DEV_VERSIONS

# All LF major versions
LF_MAJOR_VERSIONS = ["1"]

# The major version of the default LF version
LF_DEFAULT_MAJOR_VERSION = _major_str(lf_version_configuration.get("default"))

# The dev LF version with the same major version number as the default LF version.
LF_DEFAULT_DEV_VERSION = [
    v
    for v in LF_DEV_VERSIONS
    if _major_str(v) == LF_DEFAULT_MAJOR_VERSION
][0]
