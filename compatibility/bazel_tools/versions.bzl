# Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
