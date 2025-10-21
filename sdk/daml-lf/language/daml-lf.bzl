# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@os_info//:os_info.bzl", "is_intel")

def _to_dotted_version(v):
    """Converts a single version struct to a string like "2.2"."""
    return "{}.{}".format(v.major, v.minor)

def _to_dotted_versions(versions):
    """Converts a list of version structs to a list of strings."""
    return [_to_dotted_version(v) for v in versions]

# --- Main Processing Function ---

def _init_data():
    """
    Initializes all version data, fully encapsulating the definitions and logic.
    """

    # Definition of versions
    # Encapsulated in _init_data, NOT TO BE USED OUTSIDE (use variables instead)
    V2_1 = struct(major = "2", minor = "1", status = "stable")
    V2_2 = struct(major = "2", minor = "2", status = "stable", default = True)

    # V2_3 = struct(major = "2", minor = "3", status = "staging")
    V2_DEV = struct(major = "2", minor = "dev", status = "dev")

    all_versions_structs = [
        V2_1,
        V2_2,
        # V2_3,
        V2_DEV,
    ]

    # Data for (list of) version variables
    def _get_by_status(status):
        """Filters the master list by status."""
        return [v for v in all_versions_structs if v.status == status]

    stable_versions = _get_by_status("stable")
    dev_definitions = _get_by_status("dev")
    staging_versions_list = _get_by_status("staging")

    # The 'default' check is a unique filter, so a list comprehension is still
    # clearest here.
    default_definitions = [v for v in all_versions_structs if hasattr(v, "default") and v.default]

    if not stable_versions:
        fail("No stable versions were found. Cannot determine the latest stable version.")
    latest_stable = stable_versions[-1]

    if len(default_definitions) != 1:
        fail("Expected exactly one version to be marked with 'default: True', but found {}.".format(len(default_definitions)))
    default_version = default_definitions[0]

    if len(dev_definitions) != 1:
        fail("Expected exactly one version to be marked with 'status: dev', but found {}.".format(len(dev_definitions)))
    dev_version = dev_definitions[0]

    staging_version = staging_versions_list[-1] if staging_versions_list else latest_stable

    return struct(
        all = all_versions_structs,
        stable = stable_versions,
        latest_stable = latest_stable,
        default = default_version,
        dev = dev_version,
        staging = staging_version,
    )

# --- Public interface of this .bzl file ---

# Call the private function once to compute all version variables.
_data = _init_data()

# Export the public API from the returned struct.
RAW_ALL_LF_VERSIONS = _data.all
ALL_LF_VERSIONS = _to_dotted_versions(_data.all)
STABLE_LF_VERSIONS = _to_dotted_versions(_data.stable)

LATEST_STABLE_VERSION = _to_dotted_version(_data.latest_stable)
RAW_DEFAULT_VERSION = _data.default
DEFAULT_VERSION = _to_dotted_version(RAW_DEFAULT_VERSION)
DEV_VERSION = _to_dotted_version(_data.dev)
STAGING_VERSION = _to_dotted_version(_data.staging)

# configuration to maintain old-style names
lf_version_configuration = {
    "default": DEFAULT_VERSION,
    "latest": LATEST_STABLE_VERSION,
    "dev": DEV_VERSION,
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
lf_docs_version = LATEST_STABLE_VERSION

# LF Versions supported by the dar reader
READABLE_LF_VERSIONS = (["1.14", "1.15", "1.dev"] if is_intel else []) + ALL_LF_VERSIONS

def lf_version_is_dev(versionStr):
    return versionStr == DEV_VERSION

# The stable versions for which we have an LF proto definition under daml-lf/archive/src/stable
SUPPORTED_PROTO_STABLE_LF_VERSIONS = ["2.1"]

# All LF major versions supported by the compiler
COMPILER_LF_MAJOR_VERSIONS = depset([_major_str(v) for v in ALL_LF_VERSIONS]).to_list()
