# Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

def mangle_for_java(name):
    return name.replace(".", "_")

def _to_major_minor_str(v):
    (majorStr, _, minorStr) = v.partition(".")
    return (majorStr, minorStr)

def _cmp_major_version_str(aStr, bStr):
    return _cmp_int(int(aStr), int(bStr))

def _cmp_minor_version_str(aStr, bStr):
    if aStr == bStr:
        return 0
    elif aStr == "dev":
        return 1
    elif bStr == "dev":
        return -1
    else:
        return _cmp_int(int(aStr), int(bStr))

def _cmp_int(a, b):
    if a == b:
        return 0
    elif a > b:
        return 1
    else:
        return -1

def _cmp_lf_version(a, b):
    (aMajStr, aMinStr) = _to_major_minor_str(a)
    (bMajStr, bMinStr) = _to_major_minor_str(b)
    majComp = _cmp_major_version_str(aMajStr, bMajStr)
    return majComp if majComp != 0 else _cmp_minor_version_str(aMinStr, bMinStr)

def _gte(a, b):
    return _cmp_lf_version(a, b) >= 0

def _lte(a, b):
    return _cmp_lf_version(a, b) <= 0

versions = struct(
    lte = _lte,
    gte = _gte,
)

# The following dictionary alias LF versions to keywords:
# - "legacy" is the keyword for last LF version that supports legacy
#    contract ID scheme,
# - "default" is the keyword for the default compiler output,
# - "latest" is the keyword for the latest stable LF version,
# - "preview" is the keyword fort he next LF version, *not stable*,
#    usable for beta testing,
# - "dev" is the keyword for the development version, *not stable*,
#    usable for alpha testing.
# The following dictionary is always defined for "legacy", "stable",
# "latest", and "dev". It contains "preview" iff a preview version is
# available.  If exists "preview"'s value is guarantee to be different
# for all other values.  If we make a new LF release, we bump latest
# and once we make it the compiler default we bump stable.

_lf_version_configuration = {
    "legacy": "1.8",
    "default": "1.15",
    "latest": "1.15",
    #    "preview": "",
    "dev": "1.dev",
}

def _safe_get(x, default = None):
    if x == "get":
        return 1 / 0
    else:
        return _lf_version_configuration.get(x, default)

lf_version_configuration = struct(
    get = _safe_get,
    values = _lf_version_configuration.values,
    items = _lf_version_configuration.items,
)

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
    "1.dev",
    "2.dev",
]

PROTO_LF_VERSIONS = [ver for ver in LF_VERSIONS if versions.gte(ver, "1.14")]

# The subset of LF versions accepted by the compiler in the syntax
# expected by the --target option.
COMPILER_LF_VERSIONS = [ver for ver in LF_VERSIONS if versions.gte(ver, "1.14")]

LF_MAJOR_VERSIONS = ["1", "2"]
