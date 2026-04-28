# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_intel")
load("@daml_versions_data//:data.bzl", "DATA")

# Helper to convert "defaultLfVersion" -> "DEFAULT_LF_VERSION"
def _camel_to_upper_snake(text):
    result = ""
    for i in range(len(text)):
        char = text[i]
        if char.isupper() and i > 0:
            result += "_"
        result += char
    return result.upper()

def _parse_version(v):
    """
    Parses a structured version dict from the JSON:
    - {"major": 2, "minor": {"Dev": {}}}                            -> status="dev"
    - {"major": 2, "minor": {"Staging": {"version": 3, "revision": 1}}} -> status="staging"
    - {"major": 2, "minor": {"Stable": {"version": 1}}}             -> status="stable"
    """
    major = str(v["major"])
    minor_map = v["minor"]

    if "Dev" in minor_map:
        return struct(
            dotted = "{}.dev".format(major),
            mangled_java = "{}_dev".format(major),
            mangled_damlc = "{}dev".format(major),
            major = major,
            minor = "dev",
            status = "dev",
        )

    elif "Staging" in minor_map:
        info = minor_map["Staging"]
        minor = str(info["version"])
        revision = str(info["revision"])
        return struct(
            dotted = "{}.{}-staging".format(major, minor),
            mangled_java = "{}_{}_staging".format(major, minor),
            mangled_damlc = "{}{}staging".format(major, minor),
            major = major,
            minor = minor,
            revision = revision,
            status = "staging",
        )

    elif "Stable" in minor_map:
        minor = str(minor_map["Stable"]["version"])
        return struct(
            dotted = "{}.{}".format(major, minor),
            mangled_java = "{}_{}".format(major, minor),
            mangled_damlc = "{}{}".format(major, minor),
            major = major,
            minor = minor,
            status = "stable",
        )

    fail("Unknown minor version format: {}".format(minor_map))

def _build_versions_struct():
    """
    Iterates over the JSON 'explicitVersions' map and converts them
    into a single struct of version objects.
    """
    fields = {}

    # explicitVersions is now a list of dicts; deduplicate by mangled_java key
    for val in DATA["explicitVersions"]:
        version_obj = _parse_version(val)
        field_name = version_obj.mangled_java
        fields[field_name] = version_obj

    for key, val in DATA["namedVersions"].items():
        field_name = _camel_to_upper_snake(key)
        version_obj = _parse_version(val)
        fields[field_name] = version_obj

    for key, val_list in DATA["versionLists"].items():
        field_name = _camel_to_upper_snake(key)

        # Parse every dict in the list into a struct
        fields[field_name] = [_parse_version(v) for v in val_list]

    return struct(**fields)

VERSIONS = _build_versions_struct()

def _to_dotted_version(v):
    """Converts a single version struct to a string like "2.2"."""
    return "{}.{}".format(v.major, v.minor)

def _to_dotted_versions(versions):
    """Converts a list of version structs to a list of strings."""
    return [_to_dotted_version(v) for v in versions]

# public API from the internal struct.
ALL_LF_VERSIONS = [v.dotted for v in VERSIONS.ALL_LF_VERSIONS]
STABLE_LF_VERSIONS = [v.dotted for v in VERSIONS.STABLE_LF_VERSIONS]

LATEST_STABLE_LF_VERSION = VERSIONS.LATEST_STABLE_LF_VERSION.dotted
DEFAULT_LF_VERSION = VERSIONS.DEFAULT_LF_VERSION.dotted
STAGING_LF_VERSION = VERSIONS.STAGING_LF_VERSION.dotted
DEV_LF_VERSION = VERSIONS.DEV_LF_VERSION.dotted

# Ideally, COMPILER_VERSIONS does not exist. Currently, all of these piont to
# ALL_LF_VERSIONS. Haskell source files point to input/output versions, but
# bazel files point to the nonspecific COMPILER_VERSIONS. As they are equal now,
# there is no difference. As soon as COMPILER_INPUT_VERSIONS <>
# COMPILER_OUTPUT_VERSIONS we must eliminate COMPILER_VERSIONS
COMPILER_LF_VERSIONS = [v.dotted for v in VERSIONS.COMPILER_LF_VERSIONS]  #deprecated

# configuration to maintain old-style names
lf_version_configuration = {
    "default": DEFAULT_LF_VERSION,
    "latest": LATEST_STABLE_LF_VERSION,
    "dev": DEV_LF_VERSION,
}
### End of definitions that rest of file points to

def mangle_for_java(name):
    return name.replace(".", "_")

def mangle_for_damlc(name):
    return "v{}".format(name.replace(".", ""))

def _to_major_minor_str(v):
    (major_str, _, minor_str) = v.partition(".")
    return (major_str, minor_str)

def _major_str(v):
    return _to_major_minor_str(v)[0]

def _minor_str(v):
    return _to_major_minor_str(v)[1]

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

# We generate docs for the latest preview version since releasing
# preview versions without docs for them is a bit annoying.
# Once we start removing modules in newer LF versions, we might
# have to come up with something more clever here to make
# sure that we don’t remove docs for a module that is still supported
# in a stable LF version.
#TODO[#22851]: revert back to LATEST_STABLE_LF_VERSION
# LF_DOCS_VERSION = LATEST_STABLE_LF_VERSION
LF_DOCS_VERSION = STAGING_LF_VERSION

# LF Versions supported by the dar reader
READABLE_LF_VERSIONS = (["1.14", "1.15", "1.dev"] if is_intel else []) + ALL_LF_VERSIONS

def lf_version_is_dev(versionStr):
    return versionStr == DEV_LF_VERSION

# The stable versions for which we have an LF proto definition under daml-lf/archive/src/stable
SUPPORTED_PROTO_STABLE_LF_VERSIONS = ["2.1"]

# All LF major versions supported by the compiler
COMPILER_LF_MAJOR_VERSIONS = depset([_major_str(v) for v in ALL_LF_VERSIONS]).to_list()
