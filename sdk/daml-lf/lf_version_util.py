# Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared utilities for parsing structured LF version dicts from JSON."""


def parse_version_dict(v):
    """
    Parse a structured version dict into (var_name, base_v, haskell_constructor, revision_or_none).
    Supports:
      {"major": 2, "minor": {"Dev": {}}}
      {"major": 2, "minor": {"Stable": {"version": N}}}
      {"major": 2, "minor": {"Staging": {"version": N, "revision": M}}}
    """
    major = v["major"]
    minor_map = v["minor"]
    if "Dev" in minor_map:
        return f"version{major}_dev", f"{major}.dev", "PointDev", None
    elif "Staging" in minor_map:
        info = minor_map["Staging"]
        minor = info["version"]
        rev = info["revision"]
        return f"version{major}_{minor}", f"{major}.{minor}", f"PointStaging {minor}", rev
    elif "Stable" in minor_map:
        minor = minor_map["Stable"]["version"]
        return f"version{major}_{minor}", f"{major}.{minor}", f"PointStable {minor}", None
    raise ValueError(f"Unknown minor version format: {minor_map}")


def version_to_haskell_var(v):
    """Converts a structured version dict to a Haskell variable reference, e.g., version2_1."""
    var_name, _, _, _ = parse_version_dict(v)
    return var_name


def version_sort_key(v):
    """Sort key for structured version dicts."""
    minor_map = v["minor"]
    if "Dev" in minor_map:
        return (v["major"], 999999)
    elif "Staging" in minor_map:
        return (v["major"], minor_map["Staging"]["version"])
    elif "Stable" in minor_map:
        return (v["major"], minor_map["Stable"]["version"])
    else:
        return (v["major"], 0)


def version_to_key(v):
    """Convert a structured version dict to a hashable lookup key."""
    _, base_v, _, _ = parse_version_dict(v)
    return base_v

