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

def _parse_version(v_str):
    """
    Parses a version string according to specific rules:
    - "2.dev"       -> status="dev",    minor="dev"
    - "2.n-rcm"     -> status="staging", minor="n", revision="m"
    - "2.n"         -> status="stable",  minor="n"
    """
    parts = v_str.split(".")
    major = parts[0]
    remainder = parts[1]  # "dev", "1", "3-rc1"

    if "dev" in remainder:
        return struct(
            dotted = v_str,
            mangled_java = "{}_{}".format(major, "dev"),
            mangled_damlc = "{}{}".format(major, "dev"),
            major = major,
            minor = "dev",
            status = "dev",
        )

    if "-rc" in remainder:
        # split "3-rc1" into "3" and "1"
        rc_parts = remainder.split("-rc")
        minor = rc_parts[0]
        revision = rc_parts[1]

        return struct(
            dotted = v_str,
            mangled_java = "{}_{}_rc{}".format(major, minor, revision),
            mangled_damlc = "{}{}rc{}".format(major, minor, revision),
            major = major,
            minor = minor,
            revision = revision,
            status = "staging",
        )

    return struct(
        dotted = v_str,
        mangled_java = "{}_{}".format(major, remainder),
        mangled_damlc = "{}{}".format(major, remainder),
        major = major,
        minor = remainder,
        status = "stable",
    )

def _build_versions_struct():
    """
    Iterates over the JSON 'explicitVersions' map and converts them
    into a single struct of version objects.
    """
    fields = {}

    for key, val in sorted(DATA["explicitVersions"].items()):
        field_name = key.upper()
        version_obj = _parse_version(val)
        fields[field_name] = version_obj

    for key, val in sorted(DATA["namedVersions"].items()):
        field_name = _camel_to_upper_snake(key)
        version_obj = _parse_version(val)
        fields[field_name] = version_obj

    for key, val_list in sorted(DATA["versionLists"].items()):
        field_name = _camel_to_upper_snake(key)

        # Parse every string in the list into a struct
        fields[field_name] = [_parse_version(v) for v in val_list]

    return struct(**fields)

VERSIONS = _build_versions_struct()

print("DEBUG: Parsed explicit versions:", VERSIONS.COMPILER_LF_VERSIONS)

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

    # Encapsulated in _init_data, NOT TO BE USED OUTSIDE (use VARIABLES like
    # DEFAULT_LF_VERSION instead)
    V2_1 = struct(major = "2", minor = "1", status = "stable")
    V2_2 = struct(major = "2", minor = "2", status = "stable", default = True)

    V2_DEV = struct(major = "2", minor = "dev", status = "dev")

    all_versions = [
        V2_1,
        V2_2,
        V2_DEV,
    ]

    def _get_by_status(status):
        """Filters the master list by status."""
        return [v for v in all_versions if v.status == status]

    stable_versions = _get_by_status("stable")

    if not stable_versions:
        fail("No stable versions were found. Cannot determine the latest stable version.")
    latest_stable_version = stable_versions[-1]

    _default_versions = [v for v in all_versions if hasattr(v, "default") and v.default]
    if len(_default_versions) != 1:
        fail("Expected exactly one version to be marked with 'default: True', but found {}.".format(len(_default_versions)))
    default_version = _default_versions[0]

    _dev_versions = _get_by_status("dev")
    if len(_dev_versions) != 1:
        fail("Expected exactly one version to be marked with 'status: dev', but found {}.".format(len(_dev_versions)))
    dev_version = _dev_versions[0]

    _staging_versions = _get_by_status("staging")
    staging_version = _staging_versions[-1] if _staging_versions else latest_stable_version

    ### Features

    # Helpers
    dev_only = {
        "low": V2_DEV,
        "high": V2_DEV,
    }

    # version_req: just leave off a bound if its not needed, {"low"=x, "high"=y}
    # -> [x..y], {"low"=x} -> [x..], only {"high"=y} = [..y], {} = []
    features_definitions = [
        {
            # Unstable, experimental features. This should stay in x.dev
            # forever. Features implemented with this flag should be moved to a
            # separate feature flag once the decision to add them permanently
            # has been made.
            "name": "featureUnstable",
            "name_pretty": "Unstable, experimental features",
            "cpp_flag": "DAML_UNSTABLE",
            "version_req": dev_only,
        },
        {
            "name": "featureTextMap",
            "name_pretty": "TextMap type",
            "cpp_flag": "DAML_TEXTMAP",
            "version_req": dev_only,
        },
        {
            "name": "featureBigNumeric",
            "name_pretty": "BigNumeric type",
            "cpp_flag": "DAML_BIGNUMERIC",
            "version_req": dev_only,
        },
        {
            "name": "featureExceptions",
            "name_pretty": "Daml Exceptions",
            "cpp_flag": "DAML_EXCEPTIONS",
            "version_req": {"low": V2_1},
        },
        {
            "name": "featureExtendedInterfaces",
            "name_pretty": "Guards in interfaces",
            "cpp_flag": "DAML_INTERFACE_EXTENDED",
            "version_req": dev_only,
        },
        {
            "name": "featureChoiceFuncs",
            "name_pretty": "choiceController and choiceObserver functions",
            "cpp_flag": "DAML_CHOICE_FUNCS",
            # TODO(#30144): https://github.com/digital-asset/daml/issues/20786: complete implementing this feature
            "version_req": {},
        },
        {
            "name": "featureTemplateTypeRepToText",
            "name_pretty": "templateTypeRepToText function",
            "cpp_flag": "DAML_TEMPLATE_TYPEREP_TO_TEXT",
            "version_req": dev_only,
        },
        {
            "name": "featureContractKeys",
            "name_pretty": "Contract Keys",
            "cpp_flag": "DAML_CONTRACT_KEYS",
            "version_req": dev_only,
        },
        {
            "name": "featureFlatArchive",
            "name_pretty": "Flat Archive",
            "cpp_flag": "DAML_FLATARCHIVE",
            "version_req": {"low": V2_2},
        },
        {
            "name": "featurePackageImports",
            "name_pretty": "Explicit package imports",
            "cpp_flag": "DAML_PackageImports",
            "version_req": {"low": V2_2},
        },
        {
            "name": "featureComplexAnyType",
            "name_pretty": "Complex Any type",
            "cpp_flag": "DAML_COMPLEX_ANY_TYPE",
            "version_req": dev_only,
        },
        {
            "name": "featureCryptoValidateKey",
            "name_pretty": "Crypto primitive key validation",
            "cpp_flag": "DAML_VALIDATE_CRYPTO_KEY",
            "version_req": dev_only,
        },
        {
            "name": "featureExperimental",
            "name_pretty": "Daml Experimental",
            "cpp_flag": "DAML_EXPERIMENTAL",
            "version_req": dev_only,
        },
        # used only in scala
        {
            "name": "featurePackageUpgrades",
            "name_pretty": "Package upgrades",
            "cpp_flag": "DAML_PackageUpgrades",
            "version_req": {"low": V2_1},
        },
        {
            "name": "featureChoiceAuthority",
            "name_pretty": "Choice Authorizers",
            "cpp_flag": "DAML_ChoiceAuthority",
            "version_req": dev_only,
        },
        {
            "name": "featureUnsafeFromInterface",
            "name_pretty": "UnsafeFromInterface builtin",
            "cpp_flag": "DAML_UnsafeFromInterface",
            "version_req": {"high": V2_1},
        },
        {
            "name": "featureNUCK",
            "name_pretty": "Non-unique contract keys",
            "cpp_flag": "DAML_NUCK",
            "version_req": dev_only,
        },
    ]

    return struct(
        all_versions = all_versions,
        stable_versions = stable_versions,
        latest_stable_version = latest_stable_version,
        default_version = default_version,
        dev_version = dev_version,
        staging_version = staging_version,
        features = features_definitions,
    )

# --- Public interface of this .bzl file ---

_data = _init_data()

# semi-public API for the json data to be turned into Haskell/Scala src
RAW_ALL_LF_VERSIONS = _data.all_versions
RAW_STABLE_LF_VERSIONS = _data.stable_versions

RAW_LATEST_STABLE_LF_VERSION = _data.latest_stable_version
RAW_DEFAULT_LF_VERSION = _data.default_version
RAW_DEV_LF_VERSION = _data.dev_version
RAW_STAGING_LF_VERSION = _data.staging_version

# see dotted versions of these below for description
RAW_COMPILER_LF_VERSIONS = RAW_ALL_LF_VERSIONS  #deprecated
RAW_COMPILER_INPUT_LF_VERSIONS = RAW_ALL_LF_VERSIONS
RAW_COMPILER_OUTPUT_LF_VERSIONS = RAW_ALL_LF_VERSIONS

# public API from the internal struct.
ALL_LF_VERSIONS = [v.dotted for v in VERSIONS.ALL_LF_VERSIONS]
STABLE_LF_VERSIONS = [v.dotted for v in VERSIONS.STABLE_LF_VERSIONS]

LATEST_STABLE_LF_VERSION = VERSIONS.LATEST_STABLE_LF_VERSION.dotted
DEFAULT_LF_VERSION = VERSIONS.DEFAULT_LF_VERSION.dotted
DEV_LF_VERSION = VERSIONS.DEV_LF_VERSION.dotted

# Ideally, COMPILER_VERSIONS does not exist. Currently, all of these piont to
# ALL_LF_VERSIONS. Haskell source files point to input/output versions, but
# bazel files point to the nonspecific COMPILER_VERSIONS. As they are equal now,
# there is no difference. As soon as COMPILER_INPUT_VERSIONS <>
# COMPILER_OUTPUT_VERSIONS we must eliminate COMPILER_VERSIONS
COMPILER_LF_VERSIONS = [v.dotted for v in VERSIONS.COMPILER_LF_VERSIONS]  #deprecated

FEATURES = _data.features

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
LF_DOCS_VERSION = LATEST_STABLE_LF_VERSION

# LF Versions supported by the dar reader
READABLE_LF_VERSIONS = (["1.14", "1.15", "1.dev"] if is_intel else []) + ALL_LF_VERSIONS

def lf_version_is_dev(versionStr):
    return versionStr == DEV_LF_VERSION

# The stable versions for which we have an LF proto definition under daml-lf/archive/src/stable
SUPPORTED_PROTO_STABLE_LF_VERSIONS = ["2.1"]

# All LF major versions supported by the compiler
COMPILER_LF_MAJOR_VERSIONS = depset([_major_str(v) for v in ALL_LF_VERSIONS]).to_list()
