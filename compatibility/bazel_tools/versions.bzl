# Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

load("@bazel_skylib//lib:versions.bzl", _bazel_versions = "versions")
load("@os_info//:os_info.bzl", "is_windows")

def _semver_components(version):
    """Separates a semver into its components.

    I.e. separates `MAJOR.MINOR.PATCH-PRERELEASE+BUILD` into

    * the version core `MAJOR.MINOR.PATCH`
    * the optional `PRERELEASE` identifier, `None` if missing
    * the optional `BUILD` identifier, `None` if missing

    Returns:
      The tuple `(version, prerelease, build)`.
    """
    dash_pos = version.find("-")
    plus_pos = version.find("+")

    if dash_pos != -1:
        core_end = dash_pos
    elif plus_pos != -1:
        core_end = plus_pos
    else:
        core_end = len(version)

    if plus_pos != -1:
        prerelease_end = plus_pos
    else:
        prerelease_end = len(version)

    build_end = len(version)

    core = version[:core_end]

    if dash_pos != -1:
        prerelease = version[core_end + 1:prerelease_end]
    else:
        prerelease = None

    if plus_pos != -1:
        build = version[prerelease_end + 1:build_end]
    else:
        build = None

    return (core, prerelease, build)

def _extract_git_revision(prerelease):
    """Extracts the git revision from the prerelease identifier.

    This is assumed to be the last `.` separated component.
    """
    return prerelease.split(".")[-1]

def version_to_name(version):
    """Render a version to a target name or path component.

    On Windows snapshot versions are shortened to `MAJOR.MINOR.PATCH-REV`,
    where `REV` is the git revision. This is to avoid exceeding the `MAX_PATH`
    limit on Windows.
    """
    if is_windows:
        (core, prerelease, _) = _semver_components(version)
        components = [core]
        if prerelease != None:
            components.append(_extract_git_revision(prerelease))
        name = "-".join(components)
    else:
        name = version

    return name

def _cmp(a, b):
    if a == b:
        return 0
    elif a > b:
        return 1
    else:
        return -1

def _cmp_prerelease_part(part1, part2):
    if part1.isdigit() and part2.isdigit():
        return _cmp(int(part1), int(part2))
    elif part1.isdigit():
        return False
    elif part2.isdigit():
        return True
    else:
        return _cmp(part1, part2)

def _cmp_version(version1, version2):
    """Semver comparison of version1 and version2.
    Returns 1 if version1 > version2, 0 if version1 == version2 and -1 if version1 < version2.
    """

    # Handle special-cases for 0.0.0 which is always the latest.
    if version1 == "0.0.0" and version2 == "0.0.0":
        return 0
    elif version1 == "0.0.0":
        return 1
    elif version2 == "0.0.0":
        return -1

    # No version is 0.0.0 so use a proper semver comparison.
    # Note that the comparisons in skylib ignore the prerelease
    # so they aren’t suitable if we have one.

    (core1, prerelease1, _) = _semver_components(version1)
    (core2, prerelease2, _) = _semver_components(version2)
    core1_parsed = _bazel_versions.parse(core1)
    core2_parsed = _bazel_versions.parse(core2)

    # First compare (major, minor, patch) triples
    if core1_parsed > core2_parsed:
        return 1
    elif core1_parsed < core2_parsed:
        return -1
    elif not prerelease1 and not prerelease2:
        return 0
    elif not prerelease1:
        return 1
    elif not prerelease2:
        return -1
    else:
        # Finally compare the prerelease.
        parts1 = prerelease1.split(".")
        parts2 = prerelease2.split(".")
        for a, b in zip(parts1, parts2):
            cmp_result = _cmp_prerelease_part(a, b)
            if cmp_result != 0:
                return cmp_result
        return _cmp(len(a), len(b))

def _is_at_least(threshold, version):
    """Check that a version is higher or equals to a threshold.

    Args:
      threshold: the maximum version string
      version: the version string to be compared to the threshold

    Returns:
      True if version >= threshold.
    """
    return _cmp_version(version, threshold) >= 0

def _is_at_most(threshold, version):
    """Check that a version is lower or equals to a threshold.

    Args:
      threshold: the minimum version string
      version: the version string to be compared to the threshold

    Returns:
      True if version <= threshold.
    """
    return _cmp_version(version, threshold) <= 0

def _is_stable(version):
    """Check that a version is a stable version, i.e., of the form major.minor.patch.
       Note that HEAD has version 0.0.0 so it is considered stable"""
    (core, prerelease, build) = _semver_components(version)
    return not prerelease and not build

versions = struct(
    is_at_most = _is_at_most,
    is_at_least = _is_at_least,
    is_stable = _is_stable,
)
