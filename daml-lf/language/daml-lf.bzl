# Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# The following dictionary alias LF versions to keywords:
# - "legacy" is the keyword for last LF version that supports legacy
#    contract ID scheme,
# - "stable" is the keyword for the default compiler output,
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

lf_version_configuration = {
    "legacy": "1.8",
    "stable": "1.12",
    "latest": "1.13",
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

# All LF versions
LF_VERSIONS = [
    "1.6",
    "1.7",
    "1.8",
    "1.11",
    "1.12",
    "1.13",
    "dev",
]

# All LF versions for which we have protobufs.
PROTO_LF_VERSIONS = LF_VERSIONS

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
    "1.13": "daml",
}

def lf_version_package(version):
    return LF_VERSION_PACKAGE_DIGITALASSET.get(version, "daml")

LF_MAJOR_VERSIONS = ["1"]

def mangle_for_java(name):
    return name.replace(".", "_")

def _to_major_minor(v):
    (majorStr, minorStr) = v.partition(".")
    (int(majorStr), int(minorStr))

def _cmp(a, b):
    if a == b:
        return 0
    elif a > b:
        return 1
    else:
        return -1

def _cmp_lf_version(a, b):
    if a == "1.dev" and b == "1.dev":
        return 0
    elif a == "1.dev":
        return 1
    elif b == "1.dev":
        return -1
    else:
        aVer = _to_major_minor(a)
        bVer = _to_major_minor(b)
        return _cmp(aVer, bVer)

def _gte(a, b):
    return _cmp_lf_version(a, b) >= 0

def _lte(a, b):
    return _cmp_lf_version(a, b) <= 0

versions = struct(
    lte = _lte,
    gte = _gte,
)
